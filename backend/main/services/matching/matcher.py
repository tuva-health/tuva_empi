import gc
import logging
from typing import (
    Optional,
    TypedDict,
    cast,
)

import duckdb
import pandas as pd
from django.db import connection, transaction
from django.db.backends.utils import CursorWrapper
from django.db.models import Field
from django.forms import model_to_dict
from django.utils import timezone
from psycopg import sql
from splink import DuckDBAPI, Linker  # type: ignore[import-untyped]

from main.models import (
    TIMESTAMP_FORMAT,
    DbLockId,
    Job,
    JobStatus,
    JobType,
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
from main.services.matching.match_graph import (
    MatchGraph,
    MatchGroupDF,
    MatchGroupResultDF,
    PersonActionDF,
)
from main.services.matching.types import (
    PersonCrosswalkDF,
)
from main.util.sql import (
    add_column,
    create_index,
    create_temp_table,
    create_temp_table_like,
    drop_column,
    drop_table,
    extract_df,
    load_df,
    obtain_advisory_lock,
)

# id: int,
# created: str,
# job_id: int,
# data_source: str,
# source_person_id: str,
# first_name: str,
# last_name: str,
# sex: str,
# race: str,
# birth_date: str,
# death_date: str,
# social_security_number: str,
# address: str,
# city: str,
# state: str,
# zip_code: str,
# county: str,
# phone: str,
type PersonRecordDF = pd.DataFrame

# row_number: NotRequired[int]
# id: int
# job_id: int
# match_group_uuid: NotRequired[str]
# match_weight: float
# match_probability: float
# person_record_l_id: int
# person_record_r_id: int
# data: str
type SplinkResultDF = pd.DataFrame


class SplinkSettingsOverrides(TypedDict):
    link_type: str
    unique_id_column_name: str
    source_dataset_column_name: str
    retain_matching_columns: bool
    retain_intermediate_calculation_columns: bool
    additional_columns_to_retain: list[str]
    bayes_factor_column_prefix: str
    term_frequency_adjustment_column_prefix: str
    comparison_vector_value_column_prefix: str
    sql_dialect: str


class SplinkBlockingRule(TypedDict):
    blocking_rule: str
    sql_dialect: str


class Matcher:
    logger: logging.Logger
    splink_settings_overrides: SplinkSettingsOverrides

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.splink_settings_overrides = {
            "link_type": "dedupe_only",
            "unique_id_column_name": "id",
            "source_dataset_column_name": "source_dataset",
            # Retaining matching columns would make it easier to analyze results
            # independently, but takes up more space in storage. PersonRecords are
            # immutable, so it's easy to join those up.
            "retain_matching_columns": False,
            # Not sure how important this is to keep, but could help with debugging
            # results
            "retain_intermediate_calculation_columns": True,
            "additional_columns_to_retain": [],
            "bayes_factor_column_prefix": "bf_",
            "term_frequency_adjustment_column_prefix": "tf_",
            "comparison_vector_value_column_prefix": "gamma_",
            "sql_dialect": "duckdb",
        }

    #
    # Loading staging records
    #

    def add_staging_sha256(self, cursor: CursorWrapper, job: Job) -> None:
        self.logger.info(f"Adding sha256 sum to staging records with job ID {job.id}")

        person_record_stg_table = PersonRecordStaging._meta.db_table

        update_sql = sql.SQL(
            """
                update {person_record_stg_table}
                set
                    sha256 = digest(
                        concat_ws(
                            '|',
                            data_source,
                            source_person_id,
                            first_name,
                            last_name,
                            sex,
                            race,
                            birth_date,
                            death_date,
                            social_security_number,
                            address,
                            city,
                            state,
                            zip_code,
                            county,
                            phone
                        ),
                        'sha256'
                    )
                where job_id = %(job_id)s
            """
        ).format(
            person_record_stg_table=sql.Identifier(person_record_stg_table),
        )
        cursor.execute(update_sql, {"job_id": job.id})
        self.logger.info(
            f"Added sha256 sum to {cursor.rowcount} staging records  with job ID {job.id}"
        )

    def dedupe_staging(self, cursor: CursorWrapper, job: Job) -> int:
        self.logger.info(f"Deleting duplicate staging records with job ID {job.id}")

        person_record_table = PersonRecord._meta.db_table
        person_record_stg_table = PersonRecordStaging._meta.db_table

        # Deduplicate staging table against itself and against the PersonRecord table
        deduplicate_staging_sql = sql.SQL(
            """
                delete from {person_record_stg_table}
                where
                    job_id = %(job_id)s
                    and (
                        id not in (
                            select min(id)
                            from {person_record_stg_table}
                            group by sha256
                        )
                        or sha256 in (
                            select sha256
                            from {person_record_table}
                        )
                    )
            """
        ).format(
            person_record_stg_table=sql.Identifier(person_record_stg_table),
            person_record_table=sql.Identifier(person_record_table),
        )
        cursor.execute(deduplicate_staging_sql, {"job_id": job.id})
        self.logger.info(
            f"Deleted {cursor.rowcount} duplicate staging records with job ID {job.id}"
        )

        select_staging_count_sql = sql.SQL(
            """
                select count(*)
                from {person_record_stg_table}
                where
                    job_id = %(job_id)s
            """
        ).format(
            person_record_stg_table=sql.Identifier(person_record_stg_table),
        )
        cursor.execute(select_staging_count_sql, {"job_id": job.id})
        staging_count = cursor.fetchone()[0]

        self.logger.info(
            f"Staging records with job ID {job.id} left after deduplication: {staging_count}"
        )

        # NOTE: Pyright picks up the correct signature here, but mypy doesn't
        return cast(int, staging_count)

    def add_staging_row_number(self, cursor: CursorWrapper, job: Job) -> None:
        self.logger.info(f"Adding row_number to staging records with job ID {job.id}")

        person_record_stg_table = PersonRecordStaging._meta.db_table

        update_sql = sql.SQL(
            """
                update {person_record_stg_table} stg
                set
                    row_number = rn.row_number
                from (
                    select id, row_number() over (order by id) as row_number
                    from {person_record_stg_table}
                    where job_id = %(job_id)s
                ) rn
                where stg.id = rn.id
            """
        ).format(
            person_record_stg_table=sql.Identifier(person_record_stg_table),
        )
        cursor.execute(update_sql, {"job_id": job.id})
        self.logger.info(
            f"Added row_number to {cursor.rowcount} staging records with job ID {job.id}"
        )

    def create_match_event(
        self, cursor: CursorWrapper, job: Job, type: MatchEventType
    ) -> MatchEvent:
        self.logger.info(f"Creating '{type.value}' MatchEvent")

        match_event_table = MatchEvent._meta.db_table

        # TODO: Use ORM?
        create_match_event_sql = sql.SQL(
            """
                insert into {match_event_table} (created, job_id, type)
                values (statement_timestamp(), %(job_id)s, %(type)s)
                returning id, created, job_id, type
            """
        ).format(
            match_event_table=sql.Identifier(match_event_table),
        )
        cursor.execute(
            create_match_event_sql,
            {"job_id": job.id, "type": type.value},
        )

        match_events_created_count = cursor.rowcount

        if match_events_created_count != 1:
            raise Exception(
                f"Failed to create '{type.value}' Match Event."
                f" Created: {match_events_created_count}"
            )

        # FIXME: Figure out how to use psycopg row_factory with Django cursor
        # e.g. cursor.connection.row_factory
        column_names = [c.name for c in cursor.description]
        match_event = MatchEvent(**dict(zip(column_names, cursor.fetchone())))

        self.logger.info(f"Created '{type.value}' Match Event with ID {match_event.id}")

        return match_event

    def create_persons(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        person_id_temp_table: str,
    ) -> None:
        self.logger.info(f"Creating Persons for staging records with job ID {job.id}")

        person_table = Person._meta.db_table
        person_record_stg_table = PersonRecordStaging._meta.db_table

        create_temp_table(
            cursor,
            table=person_id_temp_table,
            columns=[("id", "bigint", "primary key"), ("row_number", "bigint", "")],
        )
        create_index(
            cursor,
            table=person_id_temp_table,
            column="row_number",
            index_name=person_id_temp_table + "_row_number",
        )

        insert_persons_sql = sql.SQL(
            """
                with person_id as (
                    insert into {person_table} (
                        uuid,
                        created,
                        updated,
                        job_id,
                        record_count
                    )
                    select
                        gen_random_uuid(),
                        %(match_event_created)s,
                        %(match_event_created)s,
                        %(job_id)s,
                        %(record_count)s
                    from {person_record_stg_table}
                    where job_id = %(job_id)s
                    returning id
                )
                insert into {person_id_temp_table} (id, row_number)
                select id, row_number() over (order by id)
                from person_id
            """
        ).format(
            person_table=sql.Identifier(person_table),
            person_id_temp_table=sql.Identifier(person_id_temp_table),
            person_record_stg_table=sql.Identifier(person_record_stg_table),
        )
        cursor.execute(
            insert_persons_sql,
            {
                "job_id": job.id,
                "match_event_created": match_event.created,
                "record_count": 1,
            },
        )
        self.logger.info(
            f"Created {cursor.rowcount} Persons for staging records with job ID {job.id}"
        )

    def load_person_records_with_persons(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        person_id_temp_table: str,
    ) -> int:
        self.logger.info(
            f"Loading staging records with job ID {job.id} into PersonRecord table"
        )

        person_record_table = PersonRecord._meta.db_table
        person_record_stg_table = PersonRecordStaging._meta.db_table

        insert_person_records_sql = sql.SQL(
            """
                insert into {person_record_table} (
                    created,
                    job_id,
                    person_id,
                    person_updated,
                    sha256,
                    data_source,
                    source_person_id,
                    first_name,
                    last_name,
                    sex,
                    race,
                    birth_date,
                    death_date,
                    social_security_number,
                    address,
                    city,
                    state,
                    zip_code,
                    county,
                    phone
                )
                select
                    %(match_event_created)s,
                    stg.job_id,
                    pid.id,
                    %(match_event_created)s,
                    stg.sha256,
                    stg.data_source,
                    stg.source_person_id,
                    stg.first_name,
                    stg.last_name,
                    stg.sex,
                    stg.race,
                    stg.birth_date,
                    stg.death_date,
                    stg.social_security_number,
                    stg.address,
                    stg.city,
                    stg.state,
                    stg.zip_code,
                    stg.county,
                    stg.phone
                from {person_record_stg_table} stg
                inner join {person_id_temp_table} pid on
                    stg.job_id = %(job_id)s
                    and stg.row_number = pid.row_number
            """
        ).format(
            person_record_table=sql.Identifier(person_record_table),
            person_record_stg_table=sql.Identifier(person_record_stg_table),
            person_id_temp_table=sql.Identifier(person_id_temp_table),
        )
        cursor.execute(
            insert_person_records_sql,
            {"job_id": job.id, "match_event_created": match_event.created},
        )
        self.logger.info(
            f"Loaded {cursor.rowcount} staging records with job ID {job.id} into PersonRecord table"
        )

        # NOTE: Pyright picks up the correct signature here, but mypy doesn't
        return cast(int, cursor.rowcount)

    def create_new_id_person_actions(
        self, cursor: CursorWrapper, job: Job, match_event: MatchEvent
    ) -> None:
        self.logger.info(
            f"Loading PersonActions for '{match_event.type}' event with ID {match_event.id}"
            f" (related to job {job.id}) into PersonAction table"
        )

        person_record_table = PersonRecord._meta.db_table
        person_action_table = PersonAction._meta.db_table

        insert_person_actions_sql = sql.SQL(
            """
                insert into {person_action_table} (
                    match_event_id,
                    person_id,
                    person_record_id,
                    type
                )
                select
                    %(match_event_id)s,
                    person_id,
                    id,
                    %(match_event_type)s
                from {person_record_table} pr
                where job_id = %(job_id)s
            """
        ).format(
            person_record_table=sql.Identifier(person_record_table),
            person_action_table=sql.Identifier(person_action_table),
        )
        cursor.execute(
            insert_person_actions_sql,
            {
                "job_id": job.id,
                "match_event_id": match_event.id,
                "match_event_type": PersonActionType.add_record.value,
            },
        )
        self.logger.info(
            f"Loaded {cursor.rowcount} PersonActions for '{match_event.type}' event with ID {match_event.id}"
            f" (related to job {job.id}) into PersonAction table"
        )

    def load_person_records(self, cursor: CursorWrapper, job: Job) -> int:
        # TODO: Perhaps add some defensive checks comparing number of persons created with number
        # of staging records and actions created.

        person_table = Person._meta.db_table
        person_id_temp_table = person_table + "_id_temp"

        # Add sha256 for each row in PersonRecordStaging table with job ID
        self.add_staging_sha256(cursor, job)

        # Remove person records from PersonRecordStaging if there is an
        # identical row that already exists in the PersonRecord table
        num_records_left = self.dedupe_staging(cursor, job)

        if num_records_left == 0:
            self.logger.info("No new staging records to load")
            return 0

        # Add row numbers to remaining PersonRecordStaging rows to facilitate
        # bulk loading with newly created Persons in next step
        self.add_staging_row_number(cursor, job)

        # Create MatchEvent to mark the generation of new Persons for the new PersonRecords
        match_event = self.create_match_event(cursor, job, MatchEventType.new_ids)

        # Create Persons and load Person IDs into temporary table
        self.create_persons(cursor, job, match_event, person_id_temp_table)

        # Load PersonRecords from staging table to primary table
        num_records_loaded = self.load_person_records_with_persons(
            cursor, job, match_event, person_id_temp_table
        )

        # TODO: Should we compare num_records_left with num_records_loaded?

        # Load PersonActions that represent the assignment of PersonRecords to Persons
        self.create_new_id_person_actions(cursor, job, match_event)

        return num_records_loaded

    #
    # Running Splink, collecting SplinkResults and Person crosswalk for MatchGraph analysis
    #

    def extract_person_records(self, cursor: CursorWrapper) -> PersonRecordDF:
        self.logger.info("Extracting all PersonRecord rows for matching")

        person_record_table = PersonRecord._meta.db_table
        csv_col_names = [
            f.column
            for f in PersonRecord._meta.get_fields()
            if isinstance(f, Field)
            and f.column
            not in {"person_id", "person_updated", "matched_or_reviewed", "sha256"}
        ]

        extract_sql = sql.SQL("select {columns} from {table}").format(
            table=sql.Identifier(person_record_table),
            columns=sql.SQL(",").join(
                [
                    sql.SQL("to_char({col}, %(timestamp_format)s) as {col}").format(
                        col=sql.Identifier(col)
                    )
                    if col in {"created"}
                    else sql.Identifier(col)
                    for col in csv_col_names
                ]
            ),
        )
        dtype = {
            col: ("int64" if col in {"id", "job_id"} else "string")
            for col in csv_col_names
        }
        df = extract_df(
            cursor,
            extract_sql,
            dtype,
            query_params={"timestamp_format": TIMESTAMP_FORMAT},
            na_filter=False,
        )

        self.logger.info(f"Extracted {len(df)} PersonRecord rows")
        return df

    def update_splink_settings(self, job: Job) -> None:
        """Update Splink settings with overrides.

        - Append a job_id comparison to each blocking rule. Only compare rows if at least
          one of the rows originated from the current job.

        - Override prefixes for output column names to ensure we know what those columns
          are called.
        """
        config = job.config

        self.logger.info(
            f"Updating Splink settings blocking rules for Config {config.id}"
        )

        rules: list[SplinkBlockingRule] = []

        # TODO: See how Splink handles SQL injection. Obviously, the user can supply any SQL
        # string for a blocking rule, but should we attempt to clean strings in some way to
        # prevent escaping out of the larger SQL string that they are a part of?
        for rule in config.splink_settings["blocking_rules_to_generate_predictions"]:
            # Only match records where one of the record's job_id is the current Job ID.
            # We split each rule into two in order to follow the advice given here:
            # https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html
            # Rule queries are combined with 'union all'.
            rules.append(
                {
                    "blocking_rule": f"({rule["blocking_rule"]}) and l.job_id = {job.id}",
                    "sql_dialect": self.splink_settings_overrides["sql_dialect"],
                }
            )
            rules.append(
                {
                    "blocking_rule": f"({rule["blocking_rule"]}) and r.job_id = {job.id}",
                    "sql_dialect": self.splink_settings_overrides["sql_dialect"],
                }
            )

        config.splink_settings["blocking_rules_to_generate_predictions"] = rules

        updated_rules_formatted = "\n".join(
            [
                rule["blocking_rule"]
                for rule in config.splink_settings[
                    "blocking_rules_to_generate_predictions"
                ]
            ]
        )
        self.logger.info(
            f"Updated Splink settings blocking rules for Config {config.id}:\n{updated_rules_formatted}"
        )

        # Overrides
        config.splink_settings.update(self.splink_settings_overrides)

    def run_splink_prediction(
        self, job: Job, person_record_df: PersonRecordDF
    ) -> SplinkResultDF:
        # TODO: Add dry-run flag that runs Splink and analyzes the graph, but doesn't
        # save any data in the DB. Instead, it writes results and analysis to S3.
        # Also a flag to just run matching and get number of results and number of
        # results above potential-match threshold.
        #
        # TODO: Enable Splink logging
        #
        # TODO: Should we also validate Splink config here as an extra check, or just in the API?
        # It doesn't look like Splink performs any runtime validation:
        # https://github.com/moj-analytical-services/splink/blob/f10cf7577ff4421b1cdcd4f070259083fb1669b4/splink/internals/settings.py#L214
        config = job.config

        self.logger.info(f"Running Splink prediction with Config {config.id}")
        self.update_splink_settings(job)

        ddb_conn = duckdb.connect(":memory:")
        num_threads = ddb_conn.execute("select current_setting('threads')").fetchone()

        if num_threads:
            self.logger.info(f"DuckDB is using {num_threads[0]} threads")

        linker = Linker(
            person_record_df,
            config.splink_settings,
            db_api=DuckDBAPI(connection=ddb_conn, output_schema="main"),
        )
        splink_df = linker.inference.predict()

        splink_df.physical_name

        self.logger.info("Splink prediction completed")

        result_rel = splink_df.as_duckdbpyrelation()
        results_count = result_rel.count("*").df().loc[0, "count_star()"]

        self.logger.info(f"Splink returned {results_count} prediction results")

        # FIXME: We are only saving potential-match results since Splink can produce
        # so many results and that's all we need in Postgres. Load all raw results to S3.
        # https://duckdb.org/docs/guides/network_cloud_storage/s3_export.html

        # Filter results to only those above potential-match threshold
        #
        # NOTE: It might be nice to use DuckDB's prepared statement syntax to prevent SQL injection,
        # but not sure Splink even attempts to address that.
        assert isinstance(config.potential_match_threshold, float)

        result_rel = result_rel.filter(
            f"match_probability > {config.potential_match_threshold}"
        )

        results_count = result_rel.count("*").df().loc[0, "count_star()"]

        self.logger.info(
            f"Splink returned {results_count} prediction results above configured potential-match threshold {config.potential_match_threshold}"
        )

        # NOTE: It might be nice to use DuckDB's prepared statement syntax to prevent SQL injection,
        # but not sure Splink even attempts to address that.
        assert isinstance(config.auto_match_threshold, float)

        auto_match_results_count = (
            result_rel.filter(f"match_probability > {config.auto_match_threshold}")
            .count("*")
            .df()
            .loc[0, "count_star()"]
        )

        self.logger.info(
            f"Splink returned {auto_match_results_count} prediction results above configured auto-match threshold {config.auto_match_threshold}"
        )

        # NOTE: Pyright picks up the correct signature here, but mypy doesn't
        result_df = cast(pd.DataFrame, result_rel.df())

        ddb_conn.close()
        gc.collect()

        result_df.rename(
            columns={
                "id_l": "person_record_l_id",
                "id_r": "person_record_r_id",
            },
            inplace=True,
        )

        result_df["id"] = pd.NA
        result_df["job_id"] = job.id

        # NOTE: We may not need to save this data, unless we want to build
        # in additional analytics for evaluating Splink results.
        data_columns = [
            column
            for column in result_df.columns.tolist()
            if (
                column == "match_key"
                or column.startswith(
                    self.splink_settings_overrides[
                        "term_frequency_adjustment_column_prefix"
                    ]
                )
                or column.startswith(
                    self.splink_settings_overrides["bayes_factor_column_prefix"]
                )
                or column.startswith(
                    self.splink_settings_overrides[
                        "comparison_vector_value_column_prefix"
                    ]
                )
            )
        ]
        result_df["data"] = (
            result_df[data_columns]
            .apply(lambda row: row.to_json(), axis=1)
            .astype("object")
        )

        result_df = result_df[
            [
                "id",
                "job_id",
                "match_weight",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
                "data",
            ]
        ].astype(
            {
                "id": "Int64",
                "job_id": "int64",
                "match_weight": "float64",
                "match_probability": "float64",
                "person_record_l_id": "int64",
                "person_record_r_id": "int64",
                "data": "object",
            }
        )

        return result_df

    def extract_current_results_with_lock(
        self,
        cursor: CursorWrapper,
        job: Job,
    ) -> SplinkResultDF:
        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table

        self.logger.info(
            "Locking active, unmatched Match Groups and locking/extracting related Results"
        )

        select_results_sql = sql.SQL(
            """
                select
                    r.id,
                    r.job_id,
                    r.match_weight,
                    r.match_probability,
                    r.person_record_l_id,
                    r.person_record_r_id
                from {splink_result_table} r
                inner join {match_group_table} mg on
                    mg.job_id != %(job_id)s
                    and mg.matched is null
                    and mg.deleted is null
                    and r.match_group_id = mg.id
                for update of mg, r
            """
        ).format(
            splink_result_table=sql.Identifier(splink_result_table),
            match_group_table=sql.Identifier(match_group_table),
        )

        current_result_df = extract_df(
            cursor,
            select_results_sql,
            {
                "id": "int64",
                "job_id": "int64",
                "match_weight": "float64",
                "match_probability": "float64",
                "person_record_l_id": "int64",
                "person_record_r_id": "int64",
            },
            query_params={
                "job_id": job.id,
            },
        )
        current_result_df["data"] = pd.NA

        self.logger.info(
            f"Locked active, unmatched Match Groups and locked/extracted {len(current_result_df)} related Results"
        )
        self.logger.info("Soft-deleting active, unmatched MatchGroups")

        soft_delete_match_groups_sql = sql.SQL(
            """
                update {match_group_table}
                set
                    updated = statement_timestamp(),
                    version = version + 1,
                    deleted = statement_timestamp()
                where
                    job_id != %(job_id)s
                    and matched is null
                    and deleted is null
            """
        ).format(
            match_group_table=sql.Identifier(match_group_table),
        )
        cursor.execute(soft_delete_match_groups_sql, {"job_id": job.id})
        self.logger.info(
            f"Soft-deleted {cursor.rowcount} existing, unmatched Match Groups"
        )

        return current_result_df

    def get_all_results(
        self,
        current_result_df: SplinkResultDF,
        new_result_df: SplinkResultDF,
    ) -> SplinkResultDF:
        splink_result_df = pd.concat([current_result_df, new_result_df])

        # Reset the index and do not add the old index as a column (drop=True)
        splink_result_df.reset_index(drop=True, inplace=True)

        splink_result_df["row_number"] = splink_result_df.index

        assert splink_result_df["row_number"].is_unique

        return splink_result_df

    def extract_person_crosswalk_with_lock(
        self,
        cursor: CursorWrapper,
        job: Job,
        potential_match_result_df: SplinkResultDF,
    ) -> PersonCrosswalkDF:
        """Get PersonRecordPersons and lock PersonRecord/Person rows related to potential-match Results.

        We want to lock:
        - Person rows if they connect two or more ResultGroups (directly connected Results),
          so that we can create MatchGroups correctly.

        - Person rows related to auto-match results, so that we can update the related
          PersonRecords/Persons correctly.

        To simplify things, we just lock Person rows that relate to potential-match results
        as that is a superset of the Person rows described above.
        """
        self.logger.info(
            "Extracting Person crosswalk and locking MatchGroups, Persons and PersonRecords"
        )

        person_table = Person._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_record_id_temp_table = person_record_table + "_id_temp"

        # Create temporary table for storing PersonRecord IDs

        create_temp_table(
            cursor,
            table=person_record_id_temp_table,
            columns=[("person_record_id", "bigint", "primary key")],
        )

        # Create DataFrame with unique PersonRecord IDs

        unique_person_record_ids = pd.unique(
            potential_match_result_df[
                ["person_record_l_id", "person_record_r_id"]
            ].values.ravel()  # flatten
        )
        unique_person_record_id_df = pd.DataFrame(
            unique_person_record_ids, columns=["person_record_id"]
        )

        # Load IDs into temporary table

        load_df(
            cursor,
            person_record_id_temp_table,
            unique_person_record_id_df,
            ["person_record_id"],
        )

        # Retrieve/lock PersonRecord and Person rows that relate to IDs in temporary table

        select_persons_sql = sql.SQL(
            """
                select p.id, to_char(p.created, %(timestamp_format)s) as created, p.version, p.record_count, pare.id as person_record_id
                from {person_record_id_temp_table} pareid
                inner join {person_record_table} pare on
                    pareid.person_record_id = pare.id
                inner join {person_table} p on
                    pare.person_id = p.id
                for update of pare, p
            """
        ).format(
            person_table=sql.Identifier(person_table),
            person_record_table=sql.Identifier(person_record_table),
            person_record_id_temp_table=sql.Identifier(person_record_id_temp_table),
        )

        df = extract_df(
            cursor,
            select_persons_sql,
            {
                "id": "int64",
                "created": "string",
                "version": "int64",
                "record_count": "int64",
                "person_record_id": "int64",
            },
            query_params={"timestamp_format": TIMESTAMP_FORMAT},
        )

        # Drop temporary table

        drop_table(cursor, person_record_id_temp_table)

        self.logger.info(f"Extracted {len(df)} Person crosswalk rows")
        return df

    #
    # Loading new SplinkResults and MatchGroups, updating existing SplinkResults
    #

    def load_match_groups(
        self,
        cursor: CursorWrapper,
        match_event: MatchEvent,
        match_group_df: MatchGroupDF,
    ) -> None:
        self.logger.info(
            f"Loading {len(match_group_df)} Match Groups to temporary table"
        )

        match_group_table = MatchGroup._meta.db_table
        match_group_temp_table = match_group_table + "_temp"

        # Create temporary table for new MatchGroups

        create_temp_table_like(
            cursor, table=match_group_temp_table, like_table=match_group_table
        )
        drop_column(cursor, table=match_group_temp_table, column="id")
        drop_column(cursor, table=match_group_temp_table, column="created")
        drop_column(cursor, table=match_group_temp_table, column="updated")
        drop_column(cursor, table=match_group_temp_table, column="version")
        drop_column(cursor, table=match_group_temp_table, column="matched")
        add_column(
            cursor,
            table=match_group_temp_table,
            column="matched",
            column_type="boolean",
            constraints=["not null"],
        )

        # Load new MatchGroups to temporary table

        col_names = ["uuid", "job_id", "matched"]
        loaded_count = load_df(
            cursor, match_group_temp_table, match_group_df, col_names
        )

        self.logger.info(f"Loaded {loaded_count} Match Groups to temporary table")

        # Load new MatchGroups to MatchGroup table

        self.logger.info("Loading Match Groups")

        insert_results_sql = sql.SQL(
            """
                insert into {match_group_table} (uuid, created, updated, job_id, matched)
                select
                    uuid,
                    %(match_event_created)s,
                    %(match_event_created)s,
                    job_id,
                    case
                        when matched
                        then %(match_event_created)s
                        else null
                    end
                from {match_group_temp_table}
            """
        ).format(
            match_group_table=sql.Identifier(match_group_table),
            match_group_temp_table=sql.Identifier(match_group_temp_table),
        )
        cursor.execute(insert_results_sql, {"match_event_created": match_event.created})

        if cursor.rowcount != len(match_group_df):
            raise Exception("Failed to load Match Groups")

        self.logger.info(f"Loaded {cursor.rowcount} Match Groups")

        # Drop temporary table

        drop_table(cursor, match_group_temp_table)

    def load_temp_new_splink_results(
        self,
        cursor: CursorWrapper,
        new_result_temp_table: str,
        new_result_df: SplinkResultDF,
    ) -> None:
        self.logger.info(
            f"Loading {len(new_result_df)} new Splink Results to temporary table"
        )

        splink_result_table = SplinkResult._meta.db_table

        create_temp_table_like(
            cursor, table=new_result_temp_table, like_table=splink_result_table
        )
        drop_column(cursor, table=new_result_temp_table, column="id")
        drop_column(cursor, table=new_result_temp_table, column="created")
        drop_column(cursor, table=new_result_temp_table, column="match_group_id")
        drop_column(cursor, table=new_result_temp_table, column="match_group_updated")
        add_column(
            cursor,
            table=new_result_temp_table,
            column="match_group_uuid",
            column_type="uuid",
        )
        create_index(
            cursor,
            table=new_result_temp_table,
            column="match_group_uuid",
            index_name=new_result_temp_table + "_uuid",
        )

        col_names = [
            "job_id",
            "match_weight",
            "match_probability",
            "person_record_l_id",
            "person_record_r_id",
            "data",
            "match_group_uuid",
        ]
        loaded_count = load_df(cursor, new_result_temp_table, new_result_df, col_names)

        self.logger.info(f"Loaded {loaded_count} new Splink Results to temporary table")

    def create_new_result_add_actions(
        self, cursor: CursorWrapper, job: Job, match_event: MatchEvent
    ) -> None:
        splink_result_table = SplinkResult._meta.db_table
        match_group_action_table = MatchGroupAction._meta.db_table
        add_action = MatchGroupActionType.add_result.value

        self.logger.info(
            f"Creating '{add_action}' MatchGroupActions for new results related to"
            f"'{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

        load_add_result_actions_sql = sql.SQL(
            """
                insert into {match_group_action_table} (match_event_id, match_group_id, splink_result_id, type)
                select  %(match_event_id)s, sr.match_group_id, sr.id, {add_action}
                from {splink_result_table} sr
                where
                    sr.job_id = %(job_id)s
            """
        ).format(
            splink_result_table=sql.Identifier(splink_result_table),
            match_group_action_table=sql.Identifier(match_group_action_table),
            add_action=sql.Literal(add_action),
        )
        cursor.execute(
            load_add_result_actions_sql,
            {"job_id": job.id, "match_event_id": match_event.id},
        )

        self.logger.info(
            f"Created {cursor.rowcount} '{add_action}' MatchGroupActions for new results"
            f" related to '{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

    def load_new_results(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        new_result_df: SplinkResultDF,
    ) -> None:
        splink_result_table = SplinkResult._meta.db_table
        new_result_temp_table = splink_result_table + "_new_temp"
        match_group_table = MatchGroup._meta.db_table

        # Load new results into temporary table

        self.load_temp_new_splink_results(cursor, new_result_temp_table, new_result_df)

        # Load new results into SplinkResult table with MatchGroup ID

        self.logger.info("Loading new Splink Results")

        insert_new_results_sql = sql.SQL(
            """
                insert into {splink_result_table} (created, job_id, match_group_id, match_group_updated, match_weight, match_probability, person_record_l_id, person_record_r_id, data)
                select mg.created, nr.job_id, mg.id, mg.created, nr.match_weight, nr.match_probability, nr.person_record_l_id, nr.person_record_r_id, nr.data
                from {new_result_temp_table} nr
                inner join {match_group_table} mg
                    on mg.job_id = %(job_id)s
                    and nr.match_group_uuid = mg.uuid
            """
        ).format(
            splink_result_table=sql.Identifier(splink_result_table),
            new_result_temp_table=sql.Identifier(new_result_temp_table),
            match_group_table=sql.Identifier(match_group_table),
        )
        cursor.execute(insert_new_results_sql, {"job_id": job.id})

        if cursor.rowcount != len(new_result_df):
            raise Exception(
                "Failed to load new Splink Results."
                f" Attempted to update {len(new_result_df)}, but only updated {cursor.rowcount}"
            )

        self.logger.info(f"Loaded {cursor.rowcount} new Splink Results")

        # Create MatchGroupActions

        self.create_new_result_add_actions(cursor, job, match_event)

        # Drop temporary table

        drop_table(cursor, new_result_temp_table)

    def load_temp_current_splink_result_updates(
        self,
        cursor: CursorWrapper,
        current_result_temp_table: str,
        current_result_df: SplinkResultDF,
    ) -> None:
        self.logger.info(
            f"Loading {len(current_result_df)} current Splink Result updates to temporary table"
        )

        create_temp_table(
            cursor,
            table=current_result_temp_table,
            columns=[("id", "bigint", "unique"), ("match_group_uuid", "uuid", "")],
        )
        create_index(
            cursor,
            table=current_result_temp_table,
            column="match_group_uuid",
            index_name=current_result_temp_table + "_uuid",
        )

        col_names = [
            "id",
            "match_group_uuid",
        ]
        loaded_count = load_df(
            cursor, current_result_temp_table, current_result_df, col_names
        )

        self.logger.info(
            f"Loaded {loaded_count} current Splink Result updates to temporary table"
        )

    def create_current_result_remove_actions(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        current_result_temp_table: str,
    ) -> None:
        splink_result_table = SplinkResult._meta.db_table
        match_group_table = MatchGroup._meta.db_table
        match_group_action_table = MatchGroupAction._meta.db_table
        remove_action = MatchGroupActionType.remove_result.value

        self.logger.info(
            f"Creating '{remove_action}' MatchGroupActions for current results related to"
            f"'{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

        load_add_result_actions_sql = sql.SQL(
            """
                insert into {match_group_action_table} (match_event_id, match_group_id, splink_result_id, type)
                select  %(match_event_id)s, mg.id, cr.id, {remove_action}
                from {current_result_temp_table} cr
                inner join {splink_result_table} sr
                    on cr.id = sr.id
                inner join {match_group_table} mg
                    on sr.match_group_id = mg.id
            """
        ).format(
            splink_result_table=sql.Identifier(splink_result_table),
            current_result_temp_table=sql.Identifier(current_result_temp_table),
            match_group_table=sql.Identifier(match_group_table),
            match_group_action_table=sql.Identifier(match_group_action_table),
            remove_action=sql.Literal(remove_action),
        )
        cursor.execute(
            load_add_result_actions_sql,
            {"job_id": job.id, "match_event_id": match_event.id},
        )

        self.logger.info(
            f"Created {cursor.rowcount} '{remove_action}' MatchGroupActions for current results"
            f" related to '{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

    def create_current_result_add_actions(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        current_result_temp_table: str,
    ) -> None:
        match_group_table = MatchGroup._meta.db_table
        match_group_action_table = MatchGroupAction._meta.db_table
        add_action = MatchGroupActionType.add_result.value

        self.logger.info(
            f"Creating '{add_action}' MatchGroupActions for current results related to"
            f"'{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

        load_add_result_actions_sql = sql.SQL(
            """
                insert into {match_group_action_table} (match_event_id, match_group_id, splink_result_id, type)
                select  %(match_event_id)s, mg.id, cr.id, {add_action}
                from {current_result_temp_table} cr
                inner join {match_group_table} mg
                    on mg.job_id = %(job_id)s
                    and cr.match_group_uuid = mg.uuid
            """
        ).format(
            current_result_temp_table=sql.Identifier(current_result_temp_table),
            match_group_table=sql.Identifier(match_group_table),
            match_group_action_table=sql.Identifier(match_group_action_table),
            add_action=sql.Literal(add_action),
        )
        cursor.execute(
            load_add_result_actions_sql,
            {"job_id": job.id, "match_event_id": match_event.id},
        )

        self.logger.info(
            f"Created {cursor.rowcount} '{add_action}' MatchGroupActions for current results"
            f" related to '{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

    def update_current_results(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        current_result_df: SplinkResultDF,
    ) -> None:
        splink_result_table = SplinkResult._meta.db_table
        current_result_temp_table = splink_result_table + "_current_temp"
        match_group_table = MatchGroup._meta.db_table

        # Load current result updates into temporary table

        self.load_temp_current_splink_result_updates(
            cursor, current_result_temp_table, current_result_df
        )

        # Add remove_result MatchGroupAction for each current result

        self.create_current_result_remove_actions(
            cursor, job, match_event, current_result_temp_table
        )

        # Update current results in SplinkResult table with new MatchGroup ID

        self.logger.info("Updating current Splink Results")

        update_current_results_sql = sql.SQL(
            """
                update {splink_result_table} sr
                set
                    match_group_id = mg.id,
                    match_group_updated = mg.created
                from {current_result_temp_table} cr
                inner join {match_group_table} mg
                    on mg.job_id = %(job_id)s
                    and cr.match_group_uuid = mg.uuid
                where
                    sr.id = cr.id
            """
        ).format(
            splink_result_table=sql.Identifier(splink_result_table),
            current_result_temp_table=sql.Identifier(current_result_temp_table),
            match_group_table=sql.Identifier(match_group_table),
        )
        cursor.execute(update_current_results_sql, {"job_id": job.id})

        if cursor.rowcount != len(current_result_df):
            raise Exception(
                "Failed to update current Splink Results."
                f" Attempted to update {len(current_result_df)}, but only updated {cursor.rowcount}"
            )

        self.logger.info(f"Updated {cursor.rowcount} current Splink Results")

        # Add add_result MatchGroupAction for each current result

        self.create_current_result_add_actions(
            cursor, job, match_event, current_result_temp_table
        )

        # Drop temporary table

        drop_table(cursor, current_result_temp_table)

    def create_match_group_match_actions(
        self, cursor: CursorWrapper, job: Job, match_event: MatchEvent
    ) -> None:
        match_group_table = MatchGroup._meta.db_table
        match_group_action_table = MatchGroupAction._meta.db_table

        self.logger.info(
            f"Creating '{MatchGroupActionType.match.value}' MatchGroupActions for '{match_event.type}' event"
            f" with ID {match_event.id} (related to job {job.id})"
        )

        load_add_result_actions_sql = sql.SQL(
            """
                insert into {match_group_action_table} (match_event_id, match_group_id, type)
                select  %(match_event_id)s, mg.id, {match_action}
                from {match_group_table} mg
                where
                    mg.job_id = %(job_id)s
                    and mg.matched is not null
            """
        ).format(
            match_group_table=sql.Identifier(match_group_table),
            match_group_action_table=sql.Identifier(match_group_action_table),
            match_action=sql.Literal(MatchGroupActionType.match.value),
        )
        cursor.execute(
            load_add_result_actions_sql,
            {"job_id": job.id, "match_event_id": match_event.id},
        )

        self.logger.info(
            f"Created {cursor.rowcount} '{MatchGroupActionType.match.value}' MatchGroupActions"
            f" for '{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

    def load_results_groups_and_actions(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        splink_result_df: SplinkResultDF,
        match_group_result_df: MatchGroupResultDF,
        match_group_df: MatchGroupDF,
    ) -> None:
        self.logger.info(
            "Loading new Splink Results and Match Groups, updating current Splink Results, and adding MatchGroupActions"
        )

        match_group_df["job_id"] = job.id

        result_df = pd.merge(
            splink_result_df,
            match_group_result_df,
            left_on="row_number",
            right_on="result_row_number",
            how="inner",
        )

        assert len(result_df) == len(splink_result_df)

        # NOTE: We could save the count of originally extracted current results
        # and verify that here.
        current_result_df = result_df[result_df["job_id"] != job.id]
        new_result_df = result_df[result_df["job_id"] == job.id]

        assert len(current_result_df) + len(new_result_df) == len(result_df)

        # Load match groups into MatchGroup table

        self.load_match_groups(cursor, match_event, match_group_df)

        # Load new results into SplinkResult table and create MatchGroupActions

        self.load_new_results(cursor, job, match_event, new_result_df)

        # Load current results in SplinkResult table and create MatchGroupActions

        self.update_current_results(cursor, job, match_event, current_result_df)

        # Create match MatchGroupActions

        self.create_match_group_match_actions(cursor, job, match_event)

    #
    # Updating PersonRecords and Persons
    #

    def load_temp_person_actions(
        self,
        cursor: CursorWrapper,
        person_action_df: PersonActionDF,
        person_action_temp_table: str,
    ) -> None:
        self.logger.info(
            f"Loading {len(person_action_df)} (compact) PersonActions to temporary table"
        )

        create_temp_table(
            cursor,
            table=person_action_temp_table,
            columns=[
                ("from_person_id", "bigint", ""),
                ("from_person_version", "bigint", ""),
                ("to_person_id", "bigint", ""),
                ("to_person_version", "bigint", ""),
                ("person_record_id", "bigint", ""),
                ("match_group_uuid", "uuid", ""),
            ],
        )
        col_names = [
            "from_person_id",
            "from_person_version",
            "to_person_id",
            "to_person_version",
            "person_record_id",
            "match_group_uuid",
        ]
        loaded_count = load_df(
            cursor, person_action_temp_table, person_action_df, col_names
        )
        self.logger.info(
            f"Loaded {loaded_count} (compact) PersonActions to temporary table"
        )

    def update_persons(
        self, cursor: CursorWrapper, person_action_temp_table: str
    ) -> None:
        self.logger.info("Updating versions and record counts for Persons")

        person_table = Person._meta.db_table

        # Add up the record_count diffs for each Person-update and update Persons.
        update_persons_sql = sql.SQL(
            """
                with person_updates as (
                    select person_id, max(person_version) as person_version, sum(count_diff) as count_diff
                    from (
                        select
                            from_person_id as person_id,
                            from_person_version as person_version,
                            -1 as count_diff
                        from {person_action_temp_table}
                        union all
                        select
                            to_person_id as person_id,
                            to_person_version as person_version,
                            1 as count_diff
                        from {person_action_temp_table}
                    )
                    group by person_id
                ),
                updated_persons as (
                    update {person_table} p
                    set
                        updated = statement_timestamp(),
                        version = version + 1,
                        record_count = record_count + person_updates.count_diff,
                        deleted = (
                            case
                                when record_count + person_updates.count_diff = 0
                                then statement_timestamp()
                                else null
                            end
                        )
                    from person_updates
                    where
                        p.id = person_updates.person_id
                        and p.version = person_updates.person_version
                    returning 1
                )
                select
                    (select count(*) from person_updates) as expected_person_updates_count,
                    (select count(*) from updated_persons) as actual_person_updates_count
            """
        ).format(
            person_action_temp_table=sql.Identifier(person_action_temp_table),
            person_table=sql.Identifier(person_table),
        )
        cursor.execute(update_persons_sql)

        result = cursor.fetchone()
        if not result:
            raise Exception("Failed to update Persons")

        expected_person_updates_count, actual_person_updates_count = result

        if expected_person_updates_count != actual_person_updates_count:
            raise Exception(
                "Failed to update Persons due to missing Person or version mismatch."
                f" Expected: {expected_person_updates_count} Actual: {actual_person_updates_count}"
            )

        self.logger.info(
            f"Updated versions and record counts for {actual_person_updates_count} Persons"
        )

    def update_person_record_persons(
        self,
        cursor: CursorWrapper,
        match_event: MatchEvent,
        person_action_temp_table: str,
        person_action_df: PersonActionDF,
    ) -> None:
        self.logger.info("Updating Person IDs for Person Records")

        person_record_table = PersonRecord._meta.db_table

        # Update PersonRecord Person IDs

        update_person_records_sql = sql.SQL(
            """
                update {person_record_table} pare
                set
                    person_id = ma.to_person_id,
                    person_updated = %(match_event_created)s
                from {person_action_temp_table} ma
                where
                    pare.id = ma.person_record_id
                    and pare.person_id = ma.from_person_id
            """
        ).format(
            person_record_table=sql.Identifier(person_record_table),
            person_action_temp_table=sql.Identifier(person_action_temp_table),
        )
        cursor.execute(
            update_person_records_sql, {"match_event_created": match_event.created}
        )

        expected_person_actions_count = len(person_action_df)
        actual_person_actions_count = cursor.rowcount

        if expected_person_actions_count != actual_person_actions_count:
            raise Exception(
                "Failed to update Person Records due to missing Person Record or Person ID mismatch."
                f" Expected: {expected_person_actions_count} Actual: {actual_person_actions_count}"
            )
        self.logger.info(
            f"Updated Person IDs for {actual_person_actions_count} Person Records"
        )

        # Set matched_or_reviewed on all records related to Person IDs in MatchActions

        set_person_records_matched_sql = sql.SQL(
            """
                update {person_record_table} pare
                set
                    matched_or_reviewed = %(match_event_created)s
                from {person_action_temp_table} ma
                where
                    pare.person_id = ma.to_person_id
            """
        ).format(
            person_record_table=sql.Identifier(person_record_table),
            person_action_temp_table=sql.Identifier(person_action_temp_table),
        )
        cursor.execute(
            set_person_records_matched_sql,
            {"match_event_created": match_event.created},
        )

        self.logger.info(
            f"Set matched_or_reviewed for {cursor.rowcount} Person Records"
        )

    def create_auto_match_person_actions(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        person_action_temp_table: str,
        person_action_df: PersonActionDF,
    ) -> None:
        self.logger.info(
            f"Creating PersonActions for '{match_event.type}' event with ID {match_event.id} (related to job {job.id})"
        )

        person_action_table = PersonAction._meta.db_table
        match_group_table = MatchGroup._meta.db_table
        add_action = PersonActionType.add_record.value
        remove_action = PersonActionType.remove_record.value

        # Split up the Person Actions into add/remove actions and insert them into the PersonAction
        # table. Remove actions should be ordered before add actions.
        load_person_actions_sql = sql.SQL(
            """
                with actions as (
                    select mg.id as match_group_id, from_person_id, to_person_id, person_record_id
                    from {person_action_temp_table} ma
                    inner join {match_group_table} mg on
                        ma.match_group_uuid = mg.uuid
                )
                insert into {person_action_table} (match_event_id, match_group_id, person_id, person_record_id, type)
                select  %(match_event_id)s, match_group_id, person_id, person_record_id, type
                from (
                    select
                        match_group_id,
                        from_person_id as person_id,
                        person_record_id,
                        {remove_action} as type,
                        0 as type_order
                    from actions
                    union
                    select
                        match_group_id,
                        to_person_id as person_id,
                        person_record_id,
                        {add_action} as type,
                        1 as type_order
                    from actions
                )
                order by
                    person_record_id,
                    type_order
            """
        ).format(
            person_action_table=sql.Identifier(person_action_table),
            person_action_temp_table=sql.Identifier(person_action_temp_table),
            match_group_table=sql.Identifier(match_group_table),
            remove_action=sql.Literal(remove_action),
            add_action=sql.Literal(add_action),
        )
        cursor.execute(load_person_actions_sql, {"match_event_id": match_event.id})

        expected_person_actions_count = len(person_action_df) * 2
        actual_person_actions_count = cursor.rowcount

        if expected_person_actions_count != actual_person_actions_count:
            raise Exception(
                "Failed to create Match Actions."
                f" Expected: {expected_person_actions_count} Actual: {actual_person_actions_count}"
            )
        self.logger.info(
            f"Created {actual_person_actions_count} PersonActions for '{match_event.type}' event"
            f" with ID {match_event.id} (related to job {job.id})"
        )

    def update_persons_and_load_actions(
        self,
        cursor: CursorWrapper,
        job: Job,
        match_event: MatchEvent,
        person_action_df: PersonActionDF,
    ) -> None:
        if person_action_df.empty:
            self.logger.info("Loaded 0 PersonActions - no PersonActions to load")
            return

        self.logger.info("Loading MatchEvents, PersonActions and updating Persons")

        person_action_table = PersonAction._meta.db_table
        person_action_temp_table = person_action_table + "_temp"

        # Load PersonAction DF to temporary table
        self.load_temp_person_actions(
            cursor, person_action_df, person_action_temp_table
        )

        # Update Persons with new record counts and versions
        self.update_persons(cursor, person_action_temp_table)

        # Update PersonRecord with new Person IDs
        self.update_person_record_persons(
            cursor, match_event, person_action_temp_table, person_action_df
        )

        # Load Match Actions
        self.create_auto_match_person_actions(
            cursor, job, match_event, person_action_temp_table, person_action_df
        )

    #
    # Processing job
    #

    def process_job(self, cursor: CursorWrapper, job: Job) -> None:
        # Create new Persons for PersonRecordStaging rows with job ID.
        # Then join those new Persons with the PersonRecordStaging rows to link
        # the FK and insert into the PersonRecord table.
        num_records_loaded = self.load_person_records(cursor, job)

        if num_records_loaded == 0:
            self.logger.info("Job finished")
            return

        # Extract PersonRecord table to Pandas DF
        person_record_df = self.extract_person_records(cursor)

        # Run Splink prediction
        new_result_df = self.run_splink_prediction(job, person_record_df)

        if new_result_df.empty:
            self.logger.info("No new Splink prediction results")
            self.logger.info("Job finished")
            return

        lock_acquired = obtain_advisory_lock(cursor, DbLockId.match_update)
        assert lock_acquired

        # Lock and retrieve current SplinkResults
        current_result_df = self.extract_current_results_with_lock(cursor, job)

        # Concat current SplinkResults with new SplinkResults
        splink_result_df = self.get_all_results(current_result_df, new_result_df)

        # Lock and retrieve PersonRecord/Person rows related to current SplinkResults
        person_crosswalk_df = self.extract_person_crosswalk_with_lock(
            cursor, job, splink_result_df
        )

        # Select a subset of columns for use in matching below
        result_partial_df = splink_result_df[
            [
                "row_number",
                "match_probability",
                "person_record_l_id",
                "person_record_r_id",
            ]
        ]

        # Generate ResultGroupPartialDict objects, MatchGroupPartialDict objects and auto-match
        # actions
        match_graph = MatchGraph(result_partial_df, person_crosswalk_df)
        match_analysis = match_graph.analyze_graph(job.config.auto_match_threshold)

        # Create Match Event
        match_event = self.create_match_event(cursor, job, MatchEventType.auto_matches)

        # Load SplinkResult, MatchGroup an MatchGroupAction rows
        self.load_results_groups_and_actions(
            cursor,
            job,
            match_event,
            splink_result_df,
            match_analysis["results"],
            match_analysis["match_groups"],
        )

        # Update Person membership based on auto-matches and load PersonActions related to
        # 'auto-matches'
        self.update_persons_and_load_actions(
            cursor, job, match_event, match_analysis["person_actions"]
        )

        self.logger.info("Job finished")

    def update_job_success(self, job: Job) -> None:
        self.logger.info(f"Job {job.id} succeeded")
        updated_count = Job.objects.filter(id=job.id).update(
            status=JobStatus.succeeded, updated=timezone.now(), reason=None
        )
        if updated_count != 1:
            raise Exception(
                f"Failed to update job status for job {job.id}."
                f" Expected to update 1 row, but updated {updated_count}"
            )

    def update_job_failure(self, job: Job, reason: str) -> None:
        self.logger.error(f"Job {job.id} failed: {reason}")
        updated_count = Job.objects.filter(id=job.id).update(
            status=JobStatus.failed,
            updated=timezone.now(),
            reason=f"Error: {reason}",
        )
        if updated_count != 1:
            raise Exception(
                f"Failed to update job status for job {job.id}."
                f" Expected to update 1 row, but updated {updated_count}"
            )

    def delete_staging_records(self, job: Job) -> None:
        self.logger.info(f"Deleting staging records with job ID {job.id}")
        deleted_count, _ = PersonRecordStaging.objects.filter(job_id=job.id).delete()
        self.logger.info(
            f"Deleted {deleted_count} staging records with job ID {job.id}"
        )

    def cleanup_failed_job(self, job: Optional[Job], e: Exception) -> None:
        if job:
            with transaction.atomic(durable=True):
                refreshed_job = (
                    Job.objects.select_for_update().filter(id=job.id).first()
                )

                if refreshed_job:
                    # The main transaction ended, so there is a potential that the Job was
                    # marked as succeeded or failed after the main transaction ended and before
                    # this error handling transaction started. We don't want to overwrite the
                    # Job status in that case.
                    if refreshed_job.status == JobStatus.new:
                        self.update_job_failure(refreshed_job, str(e))
                        self.delete_staging_records(refreshed_job)
                    else:
                        self.logger.error(
                            "Failed to update Job failure status and cleanup staging records."
                            f" Job {refreshed_job.id} status is {refreshed_job.status}, expected {JobStatus.new.value}."
                        )
                        raise Exception(
                            "Failed to update Job failure status and cleanup staging records."
                            f" Job {refreshed_job.id} status is {refreshed_job.status}, expected {JobStatus.new.value}."
                        ) from e
                else:
                    self.logger.error(
                        "Failed to update Job failure status and cleanup staging records."
                        f" Job {job.id} does not exist."
                    )
                    raise Exception(
                        "Failed to update Job failure status and cleanup staging records."
                        f" Job {job.id} does not exist."
                    ) from e
        else:
            self.logger.error(
                "No current Job, skipping Job failure status update and staging records cleanup"
            )
            raise Exception(
                "No current Job, skipping Job failure status update and staging records cleanup"
            ) from e

    def process_next_job(self) -> None:
        job: Optional[Job] = None

        try:
            with transaction.atomic(durable=True):
                with connection.cursor() as cursor:
                    # Obtain (wait for) lock to prevent multiple Matchers from processing jobs at the
                    # same time. Jobs should be processed sequentially.
                    lock_acquired = obtain_advisory_lock(cursor, DbLockId.matching_job)
                    assert lock_acquired

                    self.logger.info("Checking for new jobs")

                    job = (
                        Job.objects.select_for_update()
                        .filter(
                            status=JobStatus.new, job_type=JobType.import_person_records
                        )
                        .order_by("id")
                        .first()
                    )

                    if not job:
                        self.logger.info("No new jobs found")
                        return

                    self.logger.info("Found job: %s", model_to_dict(job))

                    self.process_job(cursor, job)
                    self.update_job_success(job)
                    self.delete_staging_records(job)
        except Exception as e:
            self.logger.exception(
                f"Unexpected error while processing next Job {job.id if job else None}: {e}"
            )
            self.cleanup_failed_job(job, e)
