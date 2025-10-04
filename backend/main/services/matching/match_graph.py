import logging
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable, Optional, TypedDict, cast
from uuid import uuid4

import pandas as pd
import rustworkx as rx

from main.services.matching.types import (
    PersonCrosswalkDF,
    PersonCrosswalkRow,
    PersonCrosswalkRowField,
    SplinkResultPartialDF,
    SplinkResultPartialRow,
    SplinkResultPartialRowField,
)


@dataclass
class Node:
    id: int


@dataclass
class PersonNode(Node):
    created: str
    version: int
    record_count: int


@dataclass
class PersonRecordNode(Node):
    person_id: int
    person_version: int


@dataclass
class Edge:
    pass


@dataclass
class ResultEdge(Edge):
    id: int
    match_probability: float


@dataclass
class PersonMembershipEdge(Edge):
    pass


# MatchGroup

type MatchGroupDF = pd.DataFrame

type MatchGroupRow = tuple[str, bool]


class MatchGroupRowField(Enum):
    uuid = 0
    matched = 1


# MatchGroupResult

type MatchGroupResultDF = pd.DataFrame

type MatchGroupResultRow = tuple[int, str]


class MatchGroupResultRowField(Enum):
    result_row_number = 0
    match_group_uuid = 1


# PersonAction

type PersonActionDF = pd.DataFrame

type PersonActionRow = tuple[str, int, int, int, int, int]


class PersonActionRowField(Enum):
    match_group_uuid = 0
    person_record_id = 1
    from_person_id = 2
    from_person_version = 3
    to_person_id = 4
    to_person_version = 5


# MatchAnalysis


class MatchAnalysis(TypedDict):
    match_groups: MatchGroupDF
    results: MatchGroupResultDF
    person_actions: PersonActionDF


class MatchGraph:
    """Graph used for determining MatchGroups and PersonActions."""

    logger: logging.Logger
    # TypeError: type 'rustworkx.PyGraph' is not subscriptable
    # graph: rx.PyGraph[Node, Edge]
    node_idx_by_person_id: dict[int, int]
    node_idx_by_person_record_id: dict[int, int]

    # TODO: Add additional defensive input validation
    def __init__(
        self,
        results: SplinkResultPartialDF,
        persons: PersonCrosswalkDF,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.graph: rx.PyGraph[Node, Edge] = rx.PyGraph()
        self.node_idx_by_person_id = {}
        self.node_idx_by_person_record_id = {}

        self.logger.info("Creating Match Graph")
        start_time = time.perf_counter()

        actual_result_dtypes = [(k, str(v)) for k, v in results.dtypes.items()]
        expected_result_dtypes = [
            ("row_number", "int64"),
            ("match_probability", "float64"),
            ("person_record_l_id", "int64"),
            ("person_record_r_id", "int64"),
        ]
        assert (
            actual_result_dtypes == expected_result_dtypes
        ), f"Expected: {expected_result_dtypes}\nActual: {actual_result_dtypes}"

        actual_person_dtypes = [(k, str(v)) for k, v in persons.dtypes.items()]
        expected_person_dtypes = [
            ("id", "int64"),
            ("created", "string"),
            ("version", "int64"),
            ("record_count", "int64"),
            ("person_record_id", "int64"),
        ]
        assert (
            actual_person_dtypes == expected_person_dtypes
        ), f"Expected: {expected_person_dtypes}\nActual: {actual_person_dtypes}"

        if results.empty:
            raise Exception("results must not be empty")

        if persons.empty:
            raise Exception("persons must not be empty")

        persons_renamed = persons.rename(columns={"id": "person_id"})
        unique_person_ids = persons_renamed.sort_values(by="person_id")[
            "person_id"
        ].drop_duplicates(ignore_index=True)
        unique_person_ids_from_results = (
            pd.concat(
                [
                    pd.merge(
                        results,
                        persons_renamed,
                        how="left",
                        left_on="person_record_l_id",
                        right_on="person_record_id",
                    ),
                    pd.merge(
                        results,
                        persons_renamed,
                        how="left",
                        left_on="person_record_r_id",
                        right_on="person_record_id",
                    ),
                ]
            )
            .sort_values(by="person_id")["person_id"]
            .drop_duplicates(ignore_index=True)
        )

        if not unique_person_ids_from_results.equals(unique_person_ids):
            raise Exception(
                "PersonCrosswalk must contain a Person for each PersonRecord referenced in the Splink results"
                " and must not contain extra Persons"
            )

        person_np_arrays = persons.to_records(index=False)
        person_tuples = cast(list[PersonCrosswalkRow], person_np_arrays.tolist())

        for person in person_tuples:
            node_1 = self.add_person_node(
                id=person[PersonCrosswalkRowField.id.value],
                created=person[PersonCrosswalkRowField.created.value],
                version=person[PersonCrosswalkRowField.version.value],
                record_count=person[PersonCrosswalkRowField.record_count.value],
            )
            node_2 = self.add_person_record_node(
                id=person[PersonCrosswalkRowField.person_record_id.value],
                person_id=person[PersonCrosswalkRowField.id.value],
                person_version=person[PersonCrosswalkRowField.version.value],
            )

            self.graph.add_edge(node_1, node_2, PersonMembershipEdge())

        result_np_arrays = results.to_records(index=False)
        result_tuples = cast(list[SplinkResultPartialRow], result_np_arrays.tolist())

        for result in result_tuples:
            node_3 = self.get_person_record_node(
                result[SplinkResultPartialRowField.person_record_l_id.value]
            )
            node_4 = self.get_person_record_node(
                result[SplinkResultPartialRowField.person_record_r_id.value]
            )

            assert (
                node_3 is not None and node_4 is not None
            ), "PersonCrosswalk must contain a Person for each PersonRecord referenced in the Splink results"

            self.graph.add_edge(
                node_3,
                node_4,
                ResultEdge(
                    id=result[SplinkResultPartialRowField.row_number.value],
                    match_probability=result[
                        SplinkResultPartialRowField.match_probability.value
                    ],
                ),
            )
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        self.logger.info(f"Created Match Graph in {elapsed_time:.5f} seconds")

    def add_person_node(
        self, id: int, created: str, version: int, record_count: int
    ) -> int:
        node_idx = self.node_idx_by_person_id.get(id)

        if node_idx is None:
            node_idx = self.graph.add_node(
                PersonNode(
                    id=id, created=created, version=version, record_count=record_count
                )
            )
            self.node_idx_by_person_id[id] = node_idx

        return node_idx

    def get_person_record_node(self, id: int) -> Optional[int]:
        return self.node_idx_by_person_record_id.get(id)

    def add_person_record_node(
        self, id: int, person_id: int, person_version: int
    ) -> int:
        node_idx = self.get_person_record_node(id)

        if node_idx is None:
            node_idx = self.graph.add_node(
                PersonRecordNode(
                    id=id, person_id=person_id, person_version=person_version
                )
            )
            self.node_idx_by_person_record_id[id] = node_idx

        return node_idx

    @staticmethod
    def is_person_record_node(node: Node) -> bool:
        return isinstance(node, PersonRecordNode)

    @staticmethod
    def is_person_node(node: Node) -> bool:
        return isinstance(node, PersonNode)

    @staticmethod
    def is_auto_match_edge(edge: Edge, auto_match_threshold: float) -> bool:
        return (
            isinstance(edge, ResultEdge)
            and edge.match_probability > auto_match_threshold
        )

    @staticmethod
    def is_person_membership_edge(edge: Edge) -> bool:
        return isinstance(edge, PersonMembershipEdge)

    @staticmethod
    def choose_person(person_nodes: Iterable[PersonNode]) -> PersonNode:
        """Choose representative PersonNode from an Iterable of PersonNodes.

        Choose the Person with the most records. Otherwise, choose the oldest
        Person. Otherwise, choose the Person with the lowest id.
        """
        # Order by record_count (desc), created (asc), id (asc)
        return sorted(
            person_nodes,
            key=lambda node: (
                -node.record_count,
                datetime.fromisoformat(node.created),
                node.id,
            ),
        )[0]

    @staticmethod
    def print_graph(graph: rx.PyGraph) -> None:  # type: ignore[type-arg]
        """Print Nodes and Edges in a graph.

        Useful for testing.
        """
        print("Nodes:")
        for node_index, node_data in enumerate(graph.nodes()):
            print(f"Node {node_index}: {node_data}")

        print("\nEdges:")
        for edge_index, edge_data in graph.edge_index_map().items():
            print(f"Edge {edge_index}: {edge_data}")

        print("\n")

    # TODO: Look for opportunities to break this apart to give confidence in tests
    def analyze_graph(
        self,
        auto_match_threshold: float,
    ) -> MatchAnalysis:
        self.logger.info(
            f"Analyzing Match Graph using auto-match threshold {auto_match_threshold}"
        )
        start_time = time.perf_counter()

        # These are the unique person ids belonging to each match group
        match_group_persons: dict[str, set[int]] = {}
        # A dictionary of match group results indexed by edge index for lookups
        result_dict: dict[int, MatchGroupResultRow] = {}
        person_actions: list[PersonActionRow] = []

        # Each connected component represents Persons and PersonRecords related to a unique MatchGroup
        match_group_components = rx.connected_components(self.graph)

        # Track the node index <-> match group uuid mapping
        match_group_by_node_idx: dict[int, str] = {}

        # Get MatchGroups and Person records for groups containing 1 person id
        for match_group_component in match_group_components:
            match_group_uuid = str(uuid4())
            match_group_persons[match_group_uuid] = set()

            for node_idx in match_group_component:
                node = self.graph.get_node_data(node_idx)
                match_group_by_node_idx[node_idx] = match_group_uuid
                if self.is_person_record_node(node):
                    match_group_persons[match_group_uuid].add(
                        cast(PersonRecordNode, node).person_id
                    )
                for _parent_idx, _node_idx, edge in self.graph.out_edges(node_idx):
                    if isinstance(edge, ResultEdge):
                        # NOTE: We could also update results in place
                        result_dict[edge.id] = (edge.id, match_group_uuid)

        # Create a graph of only auto-matched edges. This is more performant than
        # creating subgraphs for each match group, because whilest it may require a
        # larger search area, it copies signifcantly less data. Creating subgraphs is
        # an expensive operation.
        auto_match_group_edges = self.graph.filter_edges(
            lambda edge: (
                self.is_auto_match_edge(edge, auto_match_threshold)
                or self.is_person_membership_edge(edge)
            )
        )
        auto_match_group_edge_list = [
            self.graph.get_edge_endpoints_by_index(edge_idx)
            for edge_idx in auto_match_group_edges
        ]
        auto_match_graph = self.graph.edge_subgraph(auto_match_group_edge_list)
        auto_match_components = rx.connected_components(auto_match_graph)

        # Iterate over all of the connected auto-match components, updating the
        # records & the match groups as we go.
        for auto_match_component in auto_match_components:
            person_record_nodes: list[PersonRecordNode] = []
            person_nodes: list[PersonNode] = []
            person_match_groups: dict[int, str] = {}

            for node_idx in auto_match_component:
                node = auto_match_graph.get_node_data(node_idx)
                if self.is_person_record_node(node):
                    person_record_nodes.append(cast(PersonRecordNode, node))
                else:
                    assert self.is_person_node(node)
                    person_match_groups[node.id] = match_group_by_node_idx[node_idx]
                    person_nodes.append(cast(PersonNode, node))

            # Choose a representative person for the auto-match
            chosen_person = self.choose_person(person_nodes)

            for person_record_node in person_record_nodes:
                # Add PersonAction to update PersonRecord Person's due to auto-match
                if person_record_node.person_id != chosen_person.id:
                    person_actions.append(
                        (
                            person_match_groups[chosen_person.id],
                            person_record_node.id,
                            person_record_node.person_id,
                            person_record_node.person_version,
                            chosen_person.id,
                            chosen_person.version,
                        )
                    )
                    # Update Person on PersonRecord nodes in order to determine if
                    # a MatchGroup has been fully matched
                    match_group_uid = person_match_groups[chosen_person.id]
                    # Remove this person id from the match group, since we're going
                    # to override it
                    match_group_persons[match_group_uid].discard(
                        person_record_node.person_id
                    )
                    person_record_node.person_id = chosen_person.id

        match_groups = [
            (match_group_uuid, len(person_ids) == 1)
            for match_group_uuid, person_ids in match_group_persons.items()
        ]
        results = list(result_dict.values())
        match_analysis: MatchAnalysis = {
            "match_groups": pd.DataFrame(
                match_groups, columns=[member.name for member in MatchGroupRowField]
            ),
            "results": pd.DataFrame(
                results, columns=[member.name for member in MatchGroupResultRowField]
            ),
            "person_actions": pd.DataFrame(
                person_actions, columns=[member.name for member in PersonActionRowField]
            ),
        }

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        self.logger.info(f"Completed Match Analysis in {elapsed_time:.5f} seconds")
        return match_analysis
