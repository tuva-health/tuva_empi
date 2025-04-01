import csv
import io
import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Mapping, NotRequired, Optional, TypedDict, cast

from django.db import connection, transaction
from django.db.backends.utils import CursorWrapper
from django.db.models import F, Field
from django.utils import timezone
from psycopg import sql
from psycopg.errors import DataError, IntegrityError

from main.models import (
    MATCH_UPDATE_LOCK_ID,
    TIMESTAMP_FORMAT,
    Config,
    Job,
    JobStatus,
    MatchEvent,
    MatchEventType,
    MatchGroup,
    MatchGroupAction,
    MatchGroupActionType,
    Person,
    PersonAction,
    PersonActionType,
    PersonRecord,
    PersonRecordStaging,
    SplinkResult,
)
from main.s3 import S3Client
from main.util.sql import create_temp_table_like, drop_column


class PartialConfigDict(TypedDict):
    splink_settings: dict[str, Any]
    potential_match_threshold: float
    auto_match_threshold: float


class InvalidPersonRecordFileFormat(Exception):
    """File format is invalid when loading person records via a CSV file."""


class DataSourceDict(TypedDict):
    name: str


class SearchConditionsDict(TypedDict):
    conditions: list[sql.SQL]
    params: Mapping[str, Any]


class PotentialMatchSummaryDict(TypedDict):
    id: int
    first_name: str
    last_name: str
    data_sources: list[str]
    max_match_probability: float


class PersonRecordDict(TypedDict):
    id: int
    created: datetime
    person_uuid: str
    person_updated: datetime
    matched_or_reviewed: bool
    data_source: str
    source_person_id: str
    first_name: str
    last_name: str
    sex: str
    race: str
    birth_date: str
    death_date: str
    social_security_number: str
    address: str
    city: str
    state: str
    zip_code: str
    county: str
    phone: str


class PersonDict(TypedDict):
    uuid: str
    created: datetime
    version: int
    records: list[PersonRecordDict]


class PredictionResultDict(TypedDict):
    id: int
    created: datetime
    match_probability: float
    person_record_l_id: int
    person_record_r_id: int


class PotentialMatchDict(TypedDict):
    id: int
    created: datetime
    version: int
    persons: list[PersonDict]
    results: list[PredictionResultDict]


class PersonUpdateDict(TypedDict):
    uuid: NotRequired[str]
    version: NotRequired[int]
    new_person_record_ids: list[int]


class PersonRecordCommentDict(TypedDict):
    person_record_id: int
    comment: str


class PersonRecordIdsPartialDict(TypedDict):
    person_id: int
    person_record_id: int


# Hopefully this lands at some point: https://peps.python.org/pep-0764/
class PersonRecordIdsWithUUIDPartialDict(TypedDict):
    person_id: int
    person_uuid: str
    person_record_id: int


class ConcurrentMatchUpdates(Exception):
    """A job is currently updating matches. Please wait until the job finishes to perform a match."""


class PersonUpdateActions(TypedDict):
    add_record: list[PersonRecordIdsPartialDict]
    remove_record: list[PersonRecordIdsPartialDict]
    review_record: list[PersonRecordIdsPartialDict]


class InvalidPersonUpdate(Exception):
    """PersonUpdate is invalid."""


class InvalidPotentialMatch(Exception):
    """PotentialMatch is invalid."""


class PersonSummaryDict(TypedDict):
    uuid: str
    first_name: str
    last_name: str
    data_sources: list[str]


class EMPIService:
    logger: logging.Logger
    s3: S3Client

    def __init__(self, s3: Optional[S3Client] = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.s3 = s3 or S3Client()

    def create_config(self, config: PartialConfigDict) -> Config:
        return Config.objects.create(**config)

    def create_job(self, s3_uri: str, config_id: int) -> Job:
        return Job.objects.create(
            s3_uri=s3_uri,
            config_id=config_id,
            status=JobStatus.new,
        )

    def import_person_records(self, s3_uri: str, config_id: int) -> int:
        self.logger.info("Importing person records")

        try:
            csv_col_names = [
                f.column
                for f in PersonRecordStaging._meta.get_fields()
                if isinstance(f, Field)
                and f.column not in {"id", "created", "job_id", "row_number", "sha256"}
            ]
            expected_csv_header = ",".join(csv_col_names)
            csv_header = next(self.s3.get_object_lines(s3_uri)).decode("utf-8")

            if csv_header != expected_csv_header:
                msg = (
                    "Incorrectly formatted person records file due to invalid header."
                    f" Expected header: '{expected_csv_header}'"
                    f" Actual header: '{csv_header}'"
                )
                self.logger.error(msg)
                raise InvalidPersonRecordFileFormat(msg)

            chunks = self.s3.get_object_chunks(s3_uri)

            with transaction.atomic(durable=True):
                with connection.cursor() as cursor:
                    # Create job
                    job = self.create_job(s3_uri, config_id)

                    table = PersonRecordStaging._meta.db_table
                    temp_table = table + "_temp"

                    # Create temporary table like PersonRecord table but without
                    # id, created or job_id columns

                    create_temp_table_like(cursor, table=temp_table, like_table=table)
                    drop_column(cursor, temp_table, "id")
                    drop_column(cursor, temp_table, "created")
                    drop_column(cursor, temp_table, "job_id")
                    drop_column(cursor, temp_table, "row_number")

                    # Load person records from S3 object into temporary table
                    # TODO add as util function in main.util.sql

                    copy_sql = sql.SQL(
                        "copy {table} ({columns}) from stdin with (format csv, delimiter ',', header, force_not_null ({columns}))"
                    ).format(
                        table=sql.Identifier(temp_table),
                        columns=sql.SQL(",").join(
                            [sql.Identifier(col) for col in csv_col_names]
                        ),
                    )

                    with cursor.copy(copy_sql) as copy:
                        for chunk in chunks:
                            copy.write(chunk)

                    # Load data from temporary table into PersonRecordStaging table,
                    # including job_id column

                    insert_sql = sql.SQL(
                        """
                            insert into {table} (job_id, created, {columns})
                            select %(job_id)s, statement_timestamp(), {columns}
                            from {temp_table}
                        """
                    ).format(
                        table=sql.Identifier(table),
                        temp_table=sql.Identifier(temp_table),
                        columns=sql.SQL(",").join(
                            [sql.Identifier(col) for col in csv_col_names]
                        ),
                    )

                    cursor.execute(insert_sql, {"job_id": job.id})

                    return job.id

        except (DataError, IntegrityError) as e:
            msg = f"Incorrectly formatted person records file due to {e}"
            self.logger.error(msg)
            raise InvalidPersonRecordFileFormat(msg) from e

    def get_data_sources(self) -> list[DataSourceDict]:
        data_sources = PersonRecord.objects.values_list(
            "data_source", flat=True
        ).distinct()

        return [DataSourceDict(name=data_source) for data_source in data_sources]

    def _generate_search_conditions(
        self,
        first_name: str = "",
        last_name: str = "",
        birth_date: str = "",
        person_id: str = "",
        source_person_id: str = "",
        data_source: str = "",
    ) -> SearchConditionsDict:
        search_conditions: list[sql.SQL] = []
        search_params: dict[str, Any] = {}

        if first_name:
            search_conditions.append(
                sql.SQL("and pr_all.first_name ilike %(first_name)s")
            )
            search_params["first_name"] = "%" + first_name + "%"
        if last_name:
            search_conditions.append(
                sql.SQL("and pr_all.last_name ilike %(last_name)s")
            )
            search_params["last_name"] = "%" + last_name + "%"
        if birth_date:
            search_conditions.append(
                sql.SQL("and pr_all.birth_date ilike %(birth_date)s")
            )
            search_params["birth_date"] = "%" + birth_date + "%"
        if person_id:
            search_conditions.append(sql.SQL("and p.uuid::text like %(person_id)s"))
            search_params["person_id"] = person_id.lstrip("%") + "%"
        if source_person_id:
            search_conditions.append(
                sql.SQL("and pr_all.source_person_id::text like %(source_person_id)s")
            )
            search_params["source_person_id"] = source_person_id.lstrip("%") + "%"
        if data_source:
            search_conditions.append(
                sql.SQL("and pr_all.data_source = %(data_source)s")
            )
            search_params["data_source"] = data_source

        return {"conditions": search_conditions, "params": search_params}

    def get_potential_matches(
        self,
        first_name: str = "",
        last_name: str = "",
        birth_date: str = "",
        person_id: str = "",
        source_person_id: str = "",
        data_source: str = "",
    ) -> list[PotentialMatchSummaryDict]:
        self.logger.info("Retrieving potential matches")

        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table
        search_conditions = self._generate_search_conditions(
            first_name=first_name,
            last_name=last_name,
            birth_date=birth_date,
            person_id=person_id,
            source_person_id=source_person_id,
            data_source=data_source,
        )

        with connection.cursor() as cursor:
            # TODO: We should consider creating a MatchGroupPerson table to make lookups simpler,
            # especially since we use similar logic in match_person_records. And also,
            # a proper search index would probably be ideal.
            #
            # In the following query, we first retrieve MatchGroup IDs and then retrieve PersonRecords
            # related to those IDs. We do this in two parts, because if we retrieved the PersonRecords
            # in the initial query where search_conditions filters those records, then we wouldn't
            # return all records. We want to return MatchGroups where records match a certain query
            # and each MatchGroup should include all of its associated records, not just those that match
            # the query.
            #
            # Additionally, we join from SplinkResult to PersonRecord to Person and back to PersonRecord.
            # We do this because when joining from SplinkResult to PersonRecord we only get records that
            # are are related to results. But a MatchGroup contains records that are related to connected
            # results and records that are related to other records via Persons. Another way to look at a
            # MatchGroup is a collection of results and a collection of persons (and all records associated
            # with those persons). So if Result1 is in MatchGroup1 and Result1 links Record1 and Record2
            # and Record2 is connected with Person2 and Person2 is also connected with Record3, then Record1,
            # Record2 and Record3 are related to MatchGroup1, even though Record3 is not linked by a result.
            get_potential_matches_sql = sql.SQL(
                """
                    -- Retrieve the MatchGroup IDs that meet search criteria
                    with mgs as (
                        select distinct mg.id
                        from {match_group_table} mg
                        inner join {splink_result_table} sr
                            on mg.matched is null
                            and mg.deleted is null
                            and mg.id = sr.match_group_id
                        inner join {person_record_table} pr
                            on sr.person_record_l_id = pr.id
                            or sr.person_record_r_id = pr.id
                        inner join {person_table} p
                            on pr.person_id = p.id
                        inner join {person_record_table} pr_all
                            on p.id = pr_all.person_id
                            {search_conditions}
                    ),
                    -- Retrieve PersonRecords associated with MatchGroup IDs
                    mg_records as (
                        select distinct on (pr_all.id) mg.id, pr_all.id as record_id, pr_all.first_name, pr_all.last_name, pr_all.data_source, sr.match_probability
                        from {match_group_table} mg
                        inner join mgs
                            on mg.id = mgs.id
                        inner join {splink_result_table} sr
                            on mg.id = sr.match_group_id
                        inner join {person_record_table} pr
                            on sr.person_record_l_id = pr.id
                            or sr.person_record_r_id = pr.id
                        inner join {person_table} p
                            on pr.person_id = p.id
                        inner join {person_record_table} pr_all
                            on p.id = pr_all.person_id
                        order by pr_all.id, sr.match_probability desc
                    )
                    -- Group them to generate a PotentialMatchSummary
                    select
                        id,
                        (array_agg(first_name order by record_id))[1] as first_name,
                        (array_agg(last_name order by record_id))[1] as last_name,
                        array_agg(distinct data_source order by data_source) AS data_sources,
                        (array_agg(match_probability order by match_probability desc))[1] as max_match_probability
                    from mg_records
                    group by id;
                """
            ).format(
                match_group_table=sql.Identifier(match_group_table),
                splink_result_table=sql.Identifier(splink_result_table),
                person_record_table=sql.Identifier(person_record_table),
                person_table=sql.Identifier(person_table),
                search_conditions=sql.SQL(" ").join(search_conditions["conditions"]),
            )
            cursor.execute(get_potential_matches_sql, search_conditions["params"])

            self.logger.info(f"Retrieved {cursor.rowcount} potential matches")

            if cursor.rowcount > 0:
                column_names = [c.name for c in cursor.description]

                return [
                    cast(PotentialMatchSummaryDict, dict(zip(column_names, row)))
                    for row in cursor.fetchall()
                ]
            else:
                return []

    def _get_potential_match_persons(
        self, cursor: CursorWrapper, match_group_id: int
    ) -> list[PersonDict]:
        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        # Read Committed isolation level provides the same snapshot for all joins/subqueries in
        # the statement:
        # https://www.postgresql.org/message-id/flat/CAAc9rOz1TMme7NTb3NkvHiPjX0ckmC5UmFhadPdmXkmxagco7w@mail.gmail.com
        get_persons_sql = sql.SQL(
            """
                with persons as (
                    select distinct on (pr_all.id)
                        p.uuid as person_uuid,
                        p.created as person_created,
                        p.version as person_version,
                        pr_all.*
                    from {match_group_table} mg
                    inner join {splink_result_table} sr
                        on mg.id = %(match_group_id)s
                        and mg.id = sr.match_group_id
                    inner join {person_record_table} pr
                        on sr.person_record_l_id = pr.id
                        or sr.person_record_r_id = pr.id
                    inner join {person_table} p
                        on pr.person_id = p.id
                    inner join {person_record_table} pr_all
                        on p.id = pr_all.person_id
                )
                select
                    person_uuid::text as uuid,
                    person_created as created,
                    person_version as version,
                    array_agg(jsonb_build_object(
                        'id', id,
                        'created', to_char(created, %(timestamp_format)s),
                        'person_uuid', person_uuid,
                        'person_updated', to_char(person_updated, %(timestamp_format)s),
                        'matched_or_reviewed', matched_or_reviewed,
                        'data_source', data_source,
                        'source_person_id', source_person_id,
                        'first_name', first_name,
                        'last_name', last_name,
                        'sex', sex,
                        'race', race,
                        'birth_date', birth_date,
                        'death_date', death_date,
                        'social_security_number', social_security_number,
                        'address', address,
                        'city', city,
                        'state', state,
                        'zip_code', zip_code,
                        'county', county,
                        'phone', phone
                    )) as records
                from persons
                group by person_uuid, person_created, person_version
            """
        ).format(
            match_group_table=sql.Identifier(match_group_table),
            splink_result_table=sql.Identifier(splink_result_table),
            person_record_table=sql.Identifier(person_record_table),
            person_table=sql.Identifier(person_table),
        )
        cursor.execute(
            get_persons_sql,
            {"match_group_id": match_group_id, "timestamp_format": TIMESTAMP_FORMAT},
        )

        self.logger.info(f"Retrieved {cursor.rowcount} potential match persons")

        def row_to_person(row_dict: Mapping[str, Any]) -> PersonDict:
            records = [json.loads(record) for record in row_dict["records"]]

            return PersonDict(
                uuid=row_dict["uuid"],
                created=row_dict["created"],
                version=row_dict["version"],
                records=records,
            )

        persons = []

        if cursor.rowcount > 0:
            column_names = [c.name for c in cursor.description]
            persons = [
                row_to_person(dict(zip(column_names, row))) for row in cursor.fetchall()
            ]

        return persons

    def get_potential_match(self, id: int) -> PotentialMatchDict:
        self.logger.info(f"Retrieving potential match with id {id}")

        with transaction.atomic(durable=True):
            with connection.cursor() as cursor:
                # Use repeatable read isolation mode so that multiple selects/joins don't mix
                # up changing data
                cursor.execute("set transaction isolation level repeatable read")

                match_group = MatchGroup.objects.get(id=id, matched=None, deleted=None)

                self.logger.info("Retrieved MatchGroup")

                splink_results = SplinkResult.objects.filter(
                    match_group_id=match_group.id
                ).all()

                self.logger.info(f"Retrieved {len(splink_results)} SplinkResults")

                persons = self._get_potential_match_persons(cursor, match_group.id)

                return PotentialMatchDict(
                    id=match_group.id,
                    created=match_group.created,
                    version=match_group.version,
                    persons=persons,
                    results=[
                        PredictionResultDict(
                            id=result.id,
                            created=result.created,
                            match_probability=result.match_probability,
                            person_record_l_id=result.person_record_l_id,
                            person_record_r_id=result.person_record_r_id,
                        )
                        for result in splink_results
                    ],
                )

    def _obtain_shared_match_update_lock(self, cursor: CursorWrapper) -> bool:
        """Obtain a lock for match update.

        This lock prevents a manual-match from being run at the same time as an auto-match via a Job.
        This is because we don't want users waiting 10-20 minutes for a Job to finish. Instead, we
        can tell if a Job is holding this lock and return early. Multiple users can share the lock
        because manual-matches are quick and we don't mind if users wait less than a second.
        """
        lock_sql = sql.SQL(
            """
                select pg_try_advisory_xact_lock_shared(%(lock_id)s)
            """
        )
        cursor.execute(lock_sql, {"lock_id": MATCH_UPDATE_LOCK_ID})

        if cursor.rowcount > 0:
            row = cursor.fetchone()
            result = cast(bool, row[0])

            if not result:
                raise ConcurrentMatchUpdates(
                    "A matching job is currently updating matches. Please wait until the job finishes to perform a match."
                )
            return result
        else:
            raise Exception("Failed attempting to obtain MATCH_UPDATE_LOCK")

    def _get_match_group_records_for_update(
        self, cursor: CursorWrapper, match_group: MatchGroup
    ) -> list[PersonRecordIdsWithUUIDPartialDict]:
        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        # Sort persons and records by id to prevent deadlocks
        get_match_group_records_sql = sql.SQL(
            """
                with records as (
                    select
                        p.id as person_id,
                        p.uuid::text as person_uuid,
                        pr_all.id as person_record_id
                    from {match_group_table} mg
                    inner join {splink_result_table} sr
                        on mg.id = %(match_group_id)s
                        and mg.id = sr.match_group_id
                    inner join {person_record_table} pr
                        on sr.person_record_l_id = pr.id
                        or sr.person_record_r_id = pr.id
                    inner join {person_table} p
                        on pr.person_id = p.id
                    inner join {person_record_table} pr_all
                        on p.id = pr_all.person_id
                    order by p.id, pr_all.id
                    for update of p, pr_all
                )
                select distinct on (records.person_record_id) *
                from records
            """
        ).format(
            match_group_table=sql.Identifier(match_group_table),
            splink_result_table=sql.Identifier(splink_result_table),
            person_record_table=sql.Identifier(person_record_table),
            person_table=sql.Identifier(person_table),
        )
        cursor.execute(get_match_group_records_sql, {"match_group_id": match_group.id})

        self.logger.info(f"Retrieved {cursor.rowcount} match group person records")

        if cursor.rowcount > 0:
            column_names = [c.name for c in cursor.description]
            return [
                cast(PersonRecordIdsWithUUIDPartialDict, dict(zip(column_names, row)))
                for row in cursor.fetchall()
            ]
        else:
            raise Exception("Potential match records do not exist")

    def _create_manual_match_event(self) -> MatchEvent:
        self.logger.info(f"Creating '{MatchEventType.manual_match.value}' MatchEvent")

        match_event = MatchEvent.objects.create(
            created=timezone.now(),
            type=MatchEventType.manual_match,
        )

        self.logger.info(
            f"Created '{MatchEventType.manual_match.value}' MatchEvent with ID: {match_event.id}"
        )

        return match_event

    def _update_or_create_person(
        self, match_event: MatchEvent, update: PersonUpdateDict
    ) -> Person:
        person: Person

        if "uuid" in update:
            assert "version" in update, "Invalid Person update"

            self.logger.info(f"Updating Person with UUID: {update["uuid"]}")

            # Update Person
            # FIXME: Don't update person if their records haven't changed
            person_updated_count = Person.objects.filter(
                uuid=update["uuid"], version=update["version"]
            ).update(
                version=F("version") + 1,
                record_count=len(update["new_person_record_ids"]),
                updated=match_event.created,
                deleted=(
                    match_event.created
                    if len(update["new_person_record_ids"]) == 0
                    else None
                ),
            )

            if person_updated_count != 1:
                raise InvalidPersonUpdate("Invalid Person UUID or version outdated")

            person = Person.objects.get(uuid=update["uuid"])

            self.logger.info(
                f"Updated Person with UUID: {update["uuid"]} (ID: {person.id})"
            )
        else:
            assert "version" not in update, "Invalid Person update"

            self.logger.info(
                f"Creating new Person with record IDs: {update["new_person_record_ids"]}"
            )

            # Create Person
            person = Person.objects.create(
                uuid=uuid.uuid4(),
                created=match_event.created,
                updated=match_event.created,
                record_count=len(update["new_person_record_ids"]),
            )

            self.logger.info(
                f"Created new Person with ID: {person.id} (record IDs: {update["new_person_record_ids"]})"
            )

        return person

    def _generate_person_update_actions(
        self, person: Person, current_record_ids: set[int], new_record_ids: set[int]
    ) -> PersonUpdateActions:
        added_ids = new_record_ids.difference(current_record_ids)
        removed_ids = current_record_ids.difference(new_record_ids)
        reviewed_ids = new_record_ids.intersection(current_record_ids)

        assert len(new_record_ids) == len(reviewed_ids) + len(added_ids)
        assert len(current_record_ids) - len(removed_ids) + len(added_ids) == len(
            new_record_ids
        )

        self.logger.info(f"Adding {len(added_ids)} to Person {person.id}")
        self.logger.info(f"Removing {len(removed_ids)} from Person {person.id}")
        self.logger.info(f"Keeping {len(reviewed_ids)} with Person {person.id}")

        return {
            "add_record": [
                {"person_id": person.id, "person_record_id": id} for id in added_ids
            ],
            "remove_record": [
                {"person_id": person.id, "person_record_id": id} for id in removed_ids
            ],
            "review_record": [
                {"person_id": person.id, "person_record_id": id} for id in reviewed_ids
            ],
        }

    def _update_persons_and_generate_actions(
        self,
        match_event: MatchEvent,
        match_group_records: list[PersonRecordIdsWithUUIDPartialDict],
        person_updates: list[PersonUpdateDict],
    ) -> PersonUpdateActions:
        current_record_ids_by_person_id: Mapping[int, set[int]] = defaultdict(set)

        # Group PersonRecord IDs by Person ID
        for pr in match_group_records:
            current_record_ids_by_person_id[pr["person_id"]].add(pr["person_record_id"])

        current_person_ids = set(current_record_ids_by_person_id.keys())
        updated_person_ids: set[int] = set()
        add_action_partials: list[PersonRecordIdsPartialDict] = []
        remove_action_partials: list[PersonRecordIdsPartialDict] = []
        review_action_partials: list[PersonRecordIdsPartialDict] = []

        for update in person_updates:
            #
            # Create or update Person (increment version/update record count and metadata)
            #

            person = self._update_or_create_person(match_event, update)

            #
            # Generate actions based on new record/old record diff
            #

            current_record_ids = current_record_ids_by_person_id.get(person.id, set())
            new_record_ids = set(update["new_person_record_ids"])

            update_action_partials = self._generate_person_update_actions(
                person, current_record_ids, new_record_ids
            )

            add_action_partials.extend(update_action_partials["add_record"])
            remove_action_partials.extend(update_action_partials["remove_record"])
            review_action_partials.extend(update_action_partials["review_record"])
            updated_person_ids.add(person.id)

        #
        # Generate update actions for additional Persons that were part of the match group,
        # but not updated
        #

        reviewed_person_ids = current_person_ids.difference(updated_person_ids)

        self.logger.info(
            f"Updating {updated_person_ids} Persons and marking {reviewed_person_ids} as reviewed"
        )

        for person_id in reviewed_person_ids:
            for person_record_id in current_record_ids_by_person_id[person_id]:
                review_action_partials.append(
                    {
                        "person_id": person_id,
                        "person_record_id": person_record_id,
                    }
                )

        return {
            "add_record": add_action_partials,
            "remove_record": remove_action_partials,
            "review_record": review_action_partials,
        }

    def _update_person_records(
        self,
        match_event: MatchEvent,
        add_action_partials: list[PersonRecordIdsPartialDict],
    ) -> None:
        self.logger.info(f"Updating {len(add_action_partials)} PersonRecords")

        total_record_updated_count = 0

        for action in add_action_partials:
            record_updated_count = PersonRecord.objects.filter(
                id=action["person_record_id"]
            ).update(
                person_id=action["person_id"],
                person_updated=match_event.created,
                matched_or_reviewed=match_event.created,
            )
            total_record_updated_count += record_updated_count

        if total_record_updated_count != len(add_action_partials):
            raise Exception(
                f"Failed to update PersonRecords. Only updated {total_record_updated_count} out of {len(add_action_partials)}"
            )

        self.logger.info(f"Updated {total_record_updated_count} PersonRecords")

    def _bulk_create_person_actions(
        self,
        match_event: MatchEvent,
        match_group: MatchGroup,
        action_partials: list[PersonRecordIdsPartialDict],
        action_type: PersonActionType,
    ) -> None:
        actions = [
            PersonAction(
                match_event_id=match_event.id,
                match_group_id=match_group.id,
                person_id=action["person_id"],
                person_record_id=action["person_record_id"],
                type=action_type,
            )
            for action in action_partials
        ]

        self.logger.info(f"Creating {len(actions)} '{action_type.value}' PersonActions")

        created_actions = PersonAction.objects.bulk_create(actions)

        if len(created_actions) != len(actions):
            raise Exception(
                f"Failed to create '{action_type.value}' PersonActions."
                f" Created {len(created_actions)} out of {len(actions)}"
            )

        self.logger.info(
            f"Created {len(created_actions)} '{action_type.value}' PersonActions"
        )

    def _mark_match_group_matched(
        self, match_group: MatchGroup, match_event: MatchEvent
    ) -> None:
        self.logger.info(f"Marking MatchGroup {match_group.id} as matched")

        match_group.updated = match_event.created
        match_group.matched = match_event.created
        match_group.version = match_group.version + 1
        match_group.save()

        self.logger.info(f"Marked MatchGroup {match_group.id} as matched")

    def validate_person_update(self, person_update: PersonUpdateDict) -> bool:
        assert (
            "new_person_record_ids" in person_update
        ), "new_person_record_ids is required in PersonUpdate"

        if person_update.get("uuid") and not person_update.get("version"):
            raise InvalidPersonUpdate(
                "A PersonUpdate for an existing Person should specify a version"
            )

        if not person_update.get("uuid") and person_update.get("version"):
            raise InvalidPersonUpdate(
                "A PersonUpdate for a new Person should not specify a version"
            )

        if not person_update.get("uuid") and not person_update.get(
            "new_person_record_ids"
        ):
            raise InvalidPersonUpdate(
                "A PersonUpdate for a new Person should have 1 or more new_record_ids"
            )

        return True

    def validate_person_updates(self, person_updates: list[PersonUpdateDict]) -> bool:
        """Check uniqueness of Person UUIDs and that a PersonRecord ID does not exist in more than one PersonUpdate.

        NOTE: These checks depend on UUID being formatted with dashes.
        """
        update_person_uuids = [
            update["uuid"] for update in person_updates if "uuid" in update
        ]

        if len(update_person_uuids) != len(set(update_person_uuids)):
            raise InvalidPersonUpdate(
                "The same Person UUID cannot exist in more than one PersonUpdate"
            )

        person_uuid_by_record_id: dict[int, str] = {}

        for ndx, update in enumerate(person_updates):
            for record_id in update["new_person_record_ids"]:
                if record_id not in person_uuid_by_record_id:
                    person_uuid_by_record_id[record_id] = update.get(
                        "uuid", f"index {ndx}"
                    )
                else:
                    uuid1 = person_uuid_by_record_id[record_id]
                    uuid2 = update.get("uuid", f"index {ndx}")

                    if uuid1 != uuid2:
                        raise InvalidPersonUpdate(
                            "A PersonRecord ID cannot exist in more than PersonUpdate. PersonRecord"
                            f" {record_id} exists in updates for Person {uuid1} and Person {uuid2}."
                        )
                    else:
                        raise InvalidPersonUpdate(
                            "A PersonRecord ID cannot exist twice in the same PersonUpdate."
                            f" PersonRecord {record_id} exists in update for Person {uuid1} twice."
                        )

        return True

    def validate_update_records(
        self,
        person_updates: list[PersonUpdateDict],
        match_group_records: list[PersonRecordIdsWithUUIDPartialDict],
    ) -> bool:
        """NOTE: These checks depend on UUID being formatted with dashes."""
        current_person_uuids: set[str] = set()
        current_record_ids: set[int] = set()
        current_record_ids_by_person_uuid: Mapping[str, set[int]] = defaultdict(set)
        new_record_ids: set[int] = set()
        new_person_uuids: set[str] = set()

        for pr in match_group_records:
            assert "person_uuid" in pr

            current_person_uuids.add(pr["person_uuid"])
            current_record_ids.add(pr["person_record_id"])
            current_record_ids_by_person_uuid[pr["person_uuid"]].add(
                pr["person_record_id"]
            )

        for update in person_updates:
            if "uuid" in update:
                # Check that uuid is related to match group
                if update["uuid"] not in current_person_uuids:
                    raise InvalidPersonUpdate(
                        "Specified Person UUID must be related to PotentialMatch"
                    )

                new_person_uuids.add(update["uuid"])

            for record_id in update["new_person_record_ids"]:
                # Check that record_ids are related to match group
                if record_id not in current_record_ids:
                    raise InvalidPersonUpdate(
                        "PersonRecord IDs specified in new_person_record_ids must be related to PotentialMatch"
                    )

                new_record_ids.add(record_id)

        # Check that if a record_id currently exists in a Person and is not in the corresponding person_update,
        # it exists in another person_update
        for person_uuid, record_ids in current_record_ids_by_person_uuid.items():
            for record_id in record_ids:
                if person_uuid in new_person_uuids and record_id not in new_record_ids:
                    raise InvalidPersonUpdate(
                        "PersonRecord IDs that are removed from a Person, must be added to another Person"
                    )
                elif (
                    person_uuid not in new_person_uuids and record_id in new_record_ids
                ):
                    raise InvalidPersonUpdate(
                        "PersonRecord IDs that are added to a Person, must be removed from another Person"
                    )

        return True

    def match_person_records(
        self,
        potential_match_id: int,
        potential_match_version: int,
        person_updates: list[PersonUpdateDict],
        comments: list[PersonRecordCommentDict] = [],
    ) -> MatchEvent:
        """Match PersonRecords (move PersonRecords between Persons).

        There are two main places where things are updated in the application and where
        we need to be careful about concurrency issues. These are when manual-matches take
        place and when auto-matches take place. Manual-matches update MatchGroups, Persons
        and PersonRecords. While auto-matches update MatchGroups, SplinkResults, Persons and
        PersonRecords.

        As long as we always lock or update (which locks the row for the remainder of the
        transaction) MatchGroups, SplinkResults, Persons and PersonRecords in that order
        (and order rows by ID), we can avoid deadlocks.
        """
        for update in person_updates:
            self.validate_person_update(update)

        self.validate_person_updates(person_updates)

        self.logger.info(
            f"Matching person records for potential match {potential_match_id} v{potential_match_version}"
        )
        self.logger.info(f"Received {len(person_updates)} person updates")

        with transaction.atomic(durable=True):
            with connection.cursor() as cursor:
                # If a Job is running, we could end up blocking for a long time.
                # So for now, we raise an exception when the Job is running and tell
                # the user to check the Job status. But ideally, we can find a way
                # to design the application to minimize locking.
                lock_acquired = self._obtain_shared_match_update_lock(cursor)
                assert lock_acquired

                match_group = (
                    MatchGroup.objects.select_for_update()
                    .filter(id=potential_match_id)
                    .first()
                )

                if not match_group:
                    raise MatchGroup.DoesNotExist("Potential match does not exist")

                if match_group.deleted:
                    raise MatchGroup.DoesNotExist("Potential match has been replaced")

                if match_group.matched:
                    raise InvalidPotentialMatch("Potential has already been matched")

                if match_group.version != potential_match_version:
                    raise InvalidPotentialMatch("Potential match version is outdated")

                match_group_records = self._get_match_group_records_for_update(
                    cursor, match_group
                )

                self.validate_update_records(person_updates, match_group_records)

                #
                # Create manual-match MatchEvent
                #

                match_event = self._create_manual_match_event()

                #
                # Update/create Persons and generate update actions
                #

                update_action_partials = self._update_persons_and_generate_actions(
                    match_event, match_group_records, person_updates
                )

                #
                # Update PersonRecords
                #

                self._update_person_records(
                    match_event, update_action_partials["add_record"]
                )
                self._update_person_records(
                    match_event, update_action_partials["review_record"]
                )

                #
                # Load generated PersonActions
                #

                # FIXME: Integrate with identity provider and add performed_by field
                # FIXME: Add assertions so that if we add a record, we remove it from somewhere else and vice-versa
                # We can do assertion on final update_action_partials - group by record_id

                self._bulk_create_person_actions(
                    match_event,
                    match_group,
                    update_action_partials["review_record"],
                    PersonActionType.review,
                )
                self._bulk_create_person_actions(
                    match_event,
                    match_group,
                    update_action_partials["remove_record"],
                    PersonActionType.remove_record,
                )
                self._bulk_create_person_actions(
                    match_event,
                    match_group,
                    update_action_partials["add_record"],
                    PersonActionType.add_record,
                )

                #
                # Create 'match' MatchGroupAction
                #

                MatchGroupAction.objects.create(
                    match_event_id=match_event.id,
                    match_group_id=match_group.id,
                    type=MatchGroupActionType.match,
                )

                #
                # Mark MatchGroup as matched
                #

                self._mark_match_group_matched(match_group, match_event)

                return match_event

    def get_persons(
        self,
        first_name: str = "",
        last_name: str = "",
        birth_date: str = "",
        person_id: str = "",
        source_person_id: str = "",
        data_source: str = "",
    ) -> list[PersonSummaryDict]:
        self.logger.info("Retrieving persons")

        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table
        search_conditions = self._generate_search_conditions(
            first_name=first_name,
            last_name=last_name,
            birth_date=birth_date,
            person_id=person_id,
            source_person_id=source_person_id,
            data_source=data_source,
        )

        with connection.cursor() as cursor:
            get_persons_sql = sql.SQL(
                """
                    -- Retrieve the Person IDs that meet search criteria
                    with pids as (
                        select p.id
                        from {person_table} p
                        inner join {person_record_table} pr_all
                            on p.id = pr_all.person_id
                            and p.deleted is null
                            {search_conditions}
                    ),
                    -- Retrieve PersonRecords associated with Person IDs
                    p_records as (
                        select p.uuid, pr_all.first_name, pr_all.last_name, pr_all.data_source
                        from {person_table} p
                        inner join pids
                            on p.id = pids.id
                        inner join {person_record_table} pr_all
                            on p.id = pr_all.person_id
                        order by pr_all.id
                    )
                    -- Group them to generate a PersonSummary
                    select
                        uuid::text,
                        (array_agg(first_name))[1] as first_name,
                        (array_agg(last_name))[1] as last_name,
                        array_agg(distinct data_source) AS data_sources
                    from p_records
                    group by uuid
                    order by last_name, first_name;
                """
            ).format(
                match_group_table=sql.Identifier(match_group_table),
                splink_result_table=sql.Identifier(splink_result_table),
                person_record_table=sql.Identifier(person_record_table),
                person_table=sql.Identifier(person_table),
                search_conditions=sql.SQL(" ").join(search_conditions["conditions"]),
            )
            cursor.execute(get_persons_sql, search_conditions["params"])

            self.logger.info(f"Retrieved {cursor.rowcount} persons")

            if cursor.rowcount > 0:
                column_names = [c.name for c in cursor.description]

                return [
                    cast(PersonSummaryDict, dict(zip(column_names, row)))
                    for row in cursor.fetchall()
                ]
            else:
                return []

    def get_person(self, uuid: str) -> PersonDict:
        self.logger.info(f"Retrieving person with id {uuid}")

        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        with connection.cursor() as cursor:
            get_persons_sql = sql.SQL(
                """
                    select
                        p.uuid::text as uuid,
                        p.created as created,
                        p.version as version,
                        array_agg(jsonb_build_object(
                            'id', pr_all.id,
                            'created', to_char(pr_all.created, %(timestamp_format)s),
                            'person_uuid', p.uuid::text,
                            'person_updated', to_char(pr_all.person_updated, %(timestamp_format)s),
                            'matched_or_reviewed', pr_all.matched_or_reviewed,
                            'data_source', pr_all.data_source,
                            'source_person_id', pr_all.source_person_id,
                            'first_name', pr_all.first_name,
                            'last_name', pr_all.last_name,
                            'sex', pr_all.sex,
                            'race', pr_all.race,
                            'birth_date', pr_all.birth_date,
                            'death_date', pr_all.death_date,
                            'social_security_number', pr_all.social_security_number,
                            'address', pr_all.address,
                            'city', pr_all.city,
                            'state', pr_all.state,
                            'zip_code', pr_all.zip_code,
                            'county', pr_all.county,
                            'phone', pr_all.phone
                        )) as records
                    from {person_table} p
                    inner join {person_record_table} pr_all
                        on p.uuid = %(uuid)s
                        and p.deleted is null
                        and p.id = pr_all.person_id
                    group by p.uuid, p.created, p.version
                """
            ).format(
                person_record_table=sql.Identifier(person_record_table),
                person_table=sql.Identifier(person_table),
            )
            cursor.execute(
                get_persons_sql,
                {"uuid": uuid, "timestamp_format": TIMESTAMP_FORMAT},
            )

            if cursor.rowcount == 0:
                raise Person.DoesNotExist()

            if cursor.rowcount > 1:
                raise Person.MultipleObjectsReturned()

            self.logger.info("Retrieved person")

            def row_to_person(row_dict: Mapping[str, Any]) -> PersonDict:
                records = [json.loads(record) for record in row_dict["records"]]

                return PersonDict(
                    uuid=row_dict["uuid"],
                    created=row_dict["created"],
                    version=row_dict["version"],
                    records=records,
                )

            persons = []

            if cursor.rowcount > 0:
                column_names = [c.name for c in cursor.description]
                persons = [
                    row_to_person(dict(zip(column_names, row)))
                    for row in cursor.fetchall()
                ]

            return persons[0]

    def export_person_records(self, s3_uri: str) -> None:
        """Export person records to S3 in CSV format.

        Args:
            s3_uri: The S3 URI to export to.

        Raises:
            UploadError: If the upload fails.
        """
        # Get all person records
        with connection.cursor() as cursor:
            person_records_sql = sql.SQL("""
                select
                    p.uuid as person_id,
                    pr.source_person_id,
                    pr.data_source,
                    pr.first_name,
                    pr.last_name,
                    pr.sex,
                    pr.race,
                    pr.birth_date,
                    pr.death_date,
                    pr.social_security_number,
                    pr.address,
                    pr.city,
                    pr.state,
                    pr.zip_code,
                    pr.county,
                    pr.phone
                from {person_record_table} pr
                inner join {person_table} p on pr.person_id = p.id
            """).format(
                person_record_table=sql.Identifier(PersonRecord._meta.db_table),
                person_table=sql.Identifier(Person._meta.db_table),
            )

            cursor.execute(person_records_sql)

            self.logger.info(f"Retrieved {cursor.rowcount} person records")

            # Create CSV content in memory
            output = io.StringIO(newline="")
            writer = csv.writer(output, lineterminator="\n")

            # Write headers
            column_names = [c.name for c in cursor.description]
            writer.writerow(column_names)

            # Write data
            for row in cursor.fetchall():
                writer.writerow(row)

            # Upload to S3
            self.s3.put_object(s3_uri, output.getvalue().encode("utf-8"))

            self.logger.info(f"Uploaded {cursor.rowcount} person records to {s3_uri}")
