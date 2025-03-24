import time
from typing import Mapping

import pandas as pd
from django.test import TestCase
from pandas.testing import assert_frame_equal

from main.models import Config
from main.services.matching.match_graph import (
    MatchAnalysis,
    MatchGraph,
)
from main.tests.services.matching.util import is_uuidv4


class MatchGraphTestCase(TestCase):
    config: Config
    person_crosswalk_dtypes: Mapping[str, str]

    def setUp(self) -> None:
        self.maxDiff = None
        self.person_crosswalk_dtypes = {
            "id": "int64",
            "created": "string",
            "version": "int64",
            "record_count": "int64",
            "person_record_id": "int64",
        }

    def check_match_analysis(
        self,
        match_analysis: MatchAnalysis,
        results: pd.DataFrame,
        num_match_groups: int,
        expected_match_groups: pd.DataFrame,
        num_person_actions: int,
        expected_actions_by_mg: pd.DataFrame,
    ) -> None:
        # TODO: Verify dtypes
        mg_results = match_analysis["results"]
        match_groups = match_analysis["match_groups"]
        person_actions = match_analysis["person_actions"]

        self.assertEqual(len(mg_results), len(results))
        self.assertEqual(len(match_groups), num_match_groups)
        self.assertEqual(len(person_actions), num_person_actions)

        # Verify that uuid column values are UUIDs

        valid_uuids_count = match_groups["uuid"].apply(is_uuidv4).sum()

        self.assertEqual(valid_uuids_count, len(match_groups))

        # Join Match Groups with Match Group Results to ensure that UUIDs link up
        # and to find which Result IDs are assigned to each Match Group

        match_group_results = pd.merge(
            match_groups,
            mg_results,
            how="inner",
            left_on="uuid",
            right_on="match_group_uuid",
        )

        self.assertEqual(
            len(match_group_results),
            len(results),
            "UUIDs should link results/result groups/match groups",
        )

        # Sort by Result index for test comparisons
        match_group_results.sort_values(
            by="result_row_number", ascending=True, inplace=True
        )

        results_by_mg = (
            match_group_results.groupby(by="uuid", sort=False)
            .agg({"result_row_number": list})
            # uuid is set as the index after groupby - remove it for test comparisons
            .reset_index()[["uuid", "result_row_number"]]
        )

        actual_match_groups = pd.merge(
            results_by_mg,
            match_groups,
            how="inner",
            left_on="uuid",
            right_on="uuid",
        ).rename(columns={"result_row_number": "result_row_numbers"})[
            ["result_row_numbers", "matched"]
        ]

        assert_frame_equal(actual_match_groups, expected_match_groups)

        # Join Match Groups with Match Group Actions to ensure that UUIDs link up and
        # to find which Actions are assigned to each Match Group

        match_group_actions = pd.merge(
            match_groups,
            person_actions,
            how="inner",
            left_on="uuid",
            right_on="match_group_uuid",
        )

        # Sort by Person Record ID for test comparisons
        match_group_actions.sort_values(
            by="person_record_id", ascending=True, inplace=True
        )

        # The groupby and apply below doesn't seem to work the same when match_group_actions
        # is empty, so we check that case here
        if match_group_actions.empty:
            self.assertTrue(expected_actions_by_mg.empty)
        else:
            actual_actions_by_mg = (
                match_group_actions.groupby(by="uuid", sort=False)
                .apply(
                    lambda group: list(
                        zip(
                            group["person_record_id"],
                            group["from_person_id"],
                            group["from_person_version"],
                            group["to_person_id"],
                            group["to_person_version"],
                        )
                    )
                )
                # Convert from Series back to DataFrame
                .to_frame(name="person_actions")
                # uuid is set as the index after groupby - remove it for test comparisons
                .reset_index()[["person_actions"]]
            )
            assert_frame_equal(actual_actions_by_mg, expected_actions_by_mg)

    def test_analyze_graph_groups_simple(self) -> None:
        """Simple Results, no auto-matches."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 1, 3),
                (2, 0.5, 2, 4),
                (3, 0.5, 5, 6),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )

        persons = pd.DataFrame(
            [(id, "2024-12-23T00:19:49.272627+00", 1, 1, id) for id in range(1, 7)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(1)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=2,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0, 1, 2], [3]], "matched": [False, False]}
            ),
            num_person_actions=0,
            expected_actions_by_mg=pd.DataFrame({"person_actions": []}),
        )

    def test_analyze_graph_groups_complex_1(self) -> None:
        """Complex Results, no auto-matches."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 3, 1),
                (2, 0.5, 4, 2),
                (3, 0.5, 5, 3),
                (4, 0.5, 5, 6),
                (5, 0.5, 7, 8),
                (6, 0.5, 8, 9),
                (7, 0.5, 9, 10),
                (8, 0.5, 11, 12),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        persons = pd.DataFrame(
            [(id, "2024-12-23T00:19:49.272627+00", 1, 1, id) for id in range(1, 13)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(1)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [[0, 1, 2, 3, 4], [5, 6, 7], [8]],
                    "matched": [False, False, False],
                }
            ),
            num_person_actions=0,
            expected_actions_by_mg=pd.DataFrame({"person_actions": []}),
        )

    def test_analyze_graph_groups_complex_2(self) -> None:
        """Complex Results, no auto-matches."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 2, 2),
                (2, 0.5, 3, 2),
                (3, 0.5, 3, 1),
                (4, 0.5, 4, 5),
                (5, 0.5, 4, 6),
                (6, 0.5, 4, 7),
                (7, 0.5, 8, 9),
                (8, 0.5, 9, 10),
                (9, 0.5, 10, 11),
                (10, 0.5, 11, 4),
                (11, 0.5, 12, 13),
                (12, 0.5, 14, 15),
                (13, 0.5, 16, 17),
                (14, 0.5, 19, 18),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        persons = pd.DataFrame(
            [(id, "2024-12-23T00:19:49.272627+00", 1, 1, id) for id in range(1, 20)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(1)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=6,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [
                        [0, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9, 10],
                        [11],
                        [12],
                        [13],
                        [14],
                    ],
                    "matched": [False, False, False, False, False, False],
                }
            ),
            num_person_actions=0,
            expected_actions_by_mg=pd.DataFrame({"person_actions": []}),
        )

    def test_analyze_graph_groups_complex_3(self) -> None:
        """Results are linked by a common Person, no auto-matches."""
        # There is a disconnect between PersonRecord 3 and 4
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 2, 3),
                (2, 0.5, 4, 5),
                (3, 0.5, 6, 7),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # However, PersonRecord 3 and 4 belong to the same Person, and are thus connected
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created, 1, 1, 3),
                (3, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (6, created, 1, 1, 6),
                (7, created, 1, 1, 7),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(1)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=2,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0, 1, 2], [3]], "matched": [False, False]}
            ),
            num_person_actions=0,
            expected_actions_by_mg=pd.DataFrame({"person_actions": []}),
        )

    def test_analyze_graph_groups_missing_persons(self) -> None:
        """Single result and missing Persons."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        with self.assertRaisesMessage(
            Exception,
            "PersonCrosswalk must contain a Person for each PersonRecord referenced in the Splink results"
            " and must not contain extra Persons",
        ):
            MatchGraph(results, persons)

    def test_analyze_graph_groups_extra_persons(self) -> None:
        """Single result and extra Persons."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created, 1, 1, 3),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        with self.assertRaisesMessage(
            Exception,
            "PersonCrosswalk must contain a Person for each PersonRecord referenced in the Splink results"
            " and must not contain extra Persons",
        ):
            MatchGraph(results, persons)

    def test_analyze_graph_groups_empty_persons(self) -> None:
        """Empty Persons."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        ).astype(
            {
                "row_number": "int64",
                "match_probability": "float64",
                "person_record_l_id": "int64",
                "person_record_r_id": "int64",
            }
        )
        persons = pd.DataFrame(
            [],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        with self.assertRaisesMessage(
            Exception,
            "persons must not be empty",
        ):
            MatchGraph(results, persons)

    def test_analyze_graph_groups_empty_results(self) -> None:
        """Empty results."""
        results = pd.DataFrame(
            [],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        ).astype(
            {
                "row_number": "int64",
                "match_probability": "float64",
                "person_record_l_id": "int64",
                "person_record_r_id": "int64",
            }
        )
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [(1, created, 1, 1, 1)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        with self.assertRaisesMessage(
            Exception,
            "results must not be empty",
        ):
            MatchGraph(results, persons)

    def test_analyze_graph_actions_simple(self) -> None:
        """Results with a single auto-match."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 1, 1, 3),
                (2, 0.5, 2, 4),
                (3, 0.5, 5, 6),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        persons = pd.DataFrame(
            [(id, "2024-12-23T00:19:49.272627+00", 1, 1, id) for id in range(1, 7)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=2,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0, 1, 2], [3]], "matched": [False, False]}
            ),
            num_person_actions=1,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(3, 3, 1, 1, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_actions_complex_1(self) -> None:
        """Results with multiple disconnected auto-matches."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 1, 3),
                (2, 1, 2, 4),
                (3, 1, 5, 6),
                (4, 0.5, 6, 7),
                (5, 0.5, 8, 9),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        persons = pd.DataFrame(
            [(id, "2024-12-23T00:19:49.272627+00", 1, 1, id) for id in range(1, 10)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [[0, 1, 2], [3, 4], [5]],
                    "matched": [False, False, False],
                }
            ),
            num_person_actions=2,
            expected_actions_by_mg=pd.DataFrame(
                {"person_actions": [[(4, 4, 1, 2, 1)], [(6, 6, 1, 5, 1)]]}
            ),
        )

    def test_analyze_graph_actions_complex_2(self) -> None:
        """Results with multiple connected auto-matches."""
        results = pd.DataFrame(
            [
                (0, 1, 1, 2),
                (1, 1, 1, 3),
                (2, 1, 2, 4),
                (3, 0.5, 4, 5),
                (4, 1, 6, 7),
                (5, 0.5, 7, 8),
                (6, 0.5, 9, 10),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        persons = pd.DataFrame(
            [(id, "2024-12-23T00:19:49.272627+00", 1, 1, id) for id in range(1, 11)],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [[0, 1, 2, 3], [4, 5], [6]],
                    "matched": [False, False, False],
                }
            ),
            num_person_actions=4,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(2, 2, 1, 1, 1), (3, 3, 1, 1, 1), (4, 4, 1, 1, 1)],
                        [(7, 7, 1, 6, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_actions_complex_3(self) -> None:
        """Results are linked by a common Person, single auto-match.

        Additionally, Person has associated records that are not referenced in auto-matches.
        """
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 2, 3),
                (2, 1, 4, 6),
                (3, 0.5, 5, 7),
                (4, 0.5, 8, 9),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # PersonRecord (5 and 6) belong to the same Person, only 6 is referenced in an
        # auto-match
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created, 1, 1, 3),
                (4, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (5, created, 1, 1, 6),
                (7, created, 1, 1, 7),
                (8, created, 1, 1, 8),
                (9, created, 1, 1, 9),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [[0, 1], [2, 3], [4]],
                    "matched": [False, False, False],
                }
            ),
            num_person_actions=2,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(5, 5, 1, 4, 1), (6, 5, 1, 4, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_actions_complex_4(self) -> None:
        """Results are linked by a common Person, single auto-match.

        Additionally, Person has associated records that are not referenced in any Results.
        """
        # PersonRecord 5 and 7 are not referenced by results
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 0.5, 2, 3),
                (2, 1, 4, 6),
                (3, 0.5, 8, 9),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # PersonRecord (3 and 4) and (5, 6 and 7) belong to the same Person
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created, 1, 1, 3),
                (3, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (5, created, 1, 1, 6),
                (5, created, 1, 1, 7),
                (8, created, 1, 1, 8),
                (9, created, 1, 1, 9),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=2,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0, 1, 2], [3]], "matched": [False, False]}
            ),
            num_person_actions=3,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(5, 5, 1, 3, 1), (6, 5, 1, 3, 1), (7, 5, 1, 3, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_actions_complex_5(self) -> None:
        """Results are linked by a common Person, two auto-matches.

        Additionally, three Persons merge.
        """
        # PersonRecord 5 merges with 4 and 6 (Person 4 and 7)
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 1, 4, 5),
                (2, 1, 5, 6),
                (3, 0.5, 5, 9),
                (4, 0.5, 10, 11),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # PersonRecord (3 and 4) and (6, 7 and 8) belong to the same Person
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (4, created, 1, 1, 3),
                (4, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (7, created, 1, 1, 6),
                (7, created, 1, 1, 7),
                (7, created, 1, 1, 8),
                (9, created, 1, 1, 9),
                (10, created, 1, 1, 10),
                (11, created, 1, 1, 11),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [[0], [1, 2, 3], [4]],
                    "matched": [False, False, False],
                }
            ),
            num_person_actions=4,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [
                            (5, 5, 1, 4, 1),
                            (6, 7, 1, 4, 1),
                            (7, 7, 1, 4, 1),
                            (8, 7, 1, 4, 1),
                        ],
                    ]
                }
            ),
        )

    def test_analyze_graph_actions_record_count(self) -> None:
        """Results with a single auto-match, Person chosen by record_count."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 1, 1, 3),
                (2, 0.5, 2, 4),
                (3, 0.5, 5, 6),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # Person 3 has record_count 2
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created, 1, 2, 3),
                (4, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (6, created, 1, 1, 6),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=2,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0, 1, 2], [3]], "matched": [False, False]}
            ),
            num_person_actions=1,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(1, 1, 1, 3, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_actions_created(self) -> None:
        """Results with a single auto-match, Person chosen by created."""
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 1, 1, 3),
                (2, 0.5, 2, 4),
                (3, 0.5, 5, 6),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # Person 3 was created earlier than the rest
        created = "2024-12-23T00:19:49.272627+00"
        created_earlier = "2024-12-23T00:00:00.000000+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created_earlier, 1, 1, 3),
                (4, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (6, created, 1, 1, 6),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=2,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0, 1, 2], [3]], "matched": [False, False]}
            ),
            num_person_actions=1,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(1, 1, 1, 3, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_groups_matched(self) -> None:
        """Results with a single auto-match, MatchGroup is fully matched."""
        # Only 1 and 2 are in the first MatchGroup and they are auto-matched
        results = pd.DataFrame(
            [
                (0, 1, 1, 2),
                (1, 0.5, 3, 4),
                (2, 0.5, 5, 6),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (3, created, 1, 1, 3),
                (4, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (6, created, 1, 1, 6),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {"result_row_numbers": [[0], [1], [2]], "matched": [True, False, False]}
            ),
            num_person_actions=1,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [(2, 2, 1, 1, 1)],
                    ]
                }
            ),
        )

    def test_analyze_graph_groups_matched2(self) -> None:
        """Results are linked by a common Person, two auto-matches, MatchGroup is fully matched.

        Additionally, three Persons merge.
        """
        # PersonRecord 5 merges with 4 and 6 (Person 4 and 7)
        # Even though 5 partially matches with 7, 6 and 7 have the same Person ID
        # and so the MatchGroup is fully matched.
        results = pd.DataFrame(
            [
                (0, 0.5, 1, 2),
                (1, 1, 4, 5),
                (2, 1, 5, 6),
                (3, 0.5, 5, 7),
                (4, 0.5, 10, 11),
            ],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        # PersonRecord (3 and 4) and (6, 7 and 8) belong to the same Person
        created = "2024-12-23T00:19:49.272627+00"
        persons = pd.DataFrame(
            [
                (1, created, 1, 1, 1),
                (2, created, 1, 1, 2),
                (4, created, 1, 1, 3),
                (4, created, 1, 1, 4),
                (5, created, 1, 1, 5),
                (7, created, 1, 1, 6),
                (7, created, 1, 1, 7),
                (7, created, 1, 1, 8),
                (10, created, 1, 1, 10),
                (11, created, 1, 1, 11),
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        graph = MatchGraph(results, persons)
        match_analysis = graph.analyze_graph(0.9)

        self.check_match_analysis(
            match_analysis=match_analysis,
            results=results,
            num_match_groups=3,
            expected_match_groups=pd.DataFrame(
                {
                    "result_row_numbers": [[0], [1, 2, 3], [4]],
                    "matched": [False, True, False],
                }
            ),
            num_person_actions=4,
            expected_actions_by_mg=pd.DataFrame(
                {
                    "person_actions": [
                        [
                            (5, 5, 1, 4, 1),
                            (6, 7, 1, 4, 1),
                            (7, 7, 1, 4, 1),
                            (8, 7, 1, 4, 1),
                        ],
                    ]
                }
            ),
        )

    # FIXME: Add test if PersonRecords already have the same Person to begin with, matched should be true

    def test_analyze_graph_perf(self) -> None:
        num_results = 100_000

        results = pd.DataFrame(
            [(id, 1.0, id, id + 1) for id in range(1, num_results)],
            columns=[
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ],
        )
        persons = pd.DataFrame(
            [
                (id, "2024-12-23T00:19:49.272627+00", 1, 1, id)
                for id in range(1, num_results + 1)
            ],
            columns=["id", "created", "version", "record_count", "person_record_id"],
        ).astype(self.person_crosswalk_dtypes)

        start_time = time.time()

        graph = MatchGraph(results, persons)
        graph.analyze_graph(0)

        end_time = time.time()

        print(
            f"analyze_graph {num_results} results run time: {end_time - start_time:.5f} seconds"
        )
