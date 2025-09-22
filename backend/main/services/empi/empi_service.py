import csv
import io
import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime
from typing import IO, Any, Mapping, NotRequired, Optional, TypedDict, cast

import psycopg
from django.core.files.uploadedfile import UploadedFile
from django.db import connection, transaction
from django.db.backends.utils import CursorWrapper
from django.db.models import F, Field
from django.utils import timezone
from psycopg import sql
from psycopg.errors import DataError, IntegrityError

from main.models import (
    TIMESTAMP_FORMAT,
    Config,
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
    User,
)
from main.util.io import DEFAULT_BUFFER_SIZE, get_uri, open_sink, open_source
from main.util.sql import create_temp_table_like, drop_column, try_advisory_lock
from main.util.record_preprocessor import create_transformation_functions, transform_all_columns, remove_invalid_and_dedupe, TableResult


class PartialConfigDict(TypedDict):
    splink_settings: dict[str, Any]
    potential_match_threshold: float
    auto_match_threshold: float


class InvalidPersonRecordFileFormat(Exception):
    """File format is invalid when loading person records via a CSV file."""

class PersonRecordImportError(Exception):
    """Wrapper for Exceptions in PersonRecord import process."""

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

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def create_config(self, config: PartialConfigDict) -> Config:
        return Config.objects.create(**config)

    def create_job(self, source: str | UploadedFile, config_id: int) -> Job:
        return Job.objects.create(
            source_uri=get_uri(source),
            config_id=config_id,
            status=JobStatus.new,
        )

    def create_export_job(
        self,
        config_id: int,
        sink_uri: str,
    ) -> Job:
        """Create a background job for exporting potential matches.

        Args:
            config_id: Configuration ID for the job
            sink_uri: S3 URI where the export will be saved
        Returns:
            Job object representing the background export task
        """
        return Job.objects.create(
            source_uri=sink_uri,
            config_id=config_id,
            job_type=JobType.export_potential_matches,
            status=JobStatus.new,
        )

    def process_export_job(self, job: Job) -> None:
        """Process an export job in the background.

        This method should be called by a background worker to process
        the export job asynchronously.

        Args:
            job: The Job object to process

        Raises:
            Exception: If the export fails
        """
        start_time = time.perf_counter()

        try:
            self.logger.info(f"Starting export job {job.id} (sink: {job.source_uri})")

            # Parse job metadata
            job_metadata = json.loads(job.reason) if job.reason else {}

            # Log job parameters for debugging
            if job_metadata:
                self.logger.info(f"Export job {job.id} parameters: no filters applied")

            # Update job status to processing
            job.status = JobStatus.new
            job.save()

            # Estimate total records for progress tracking
            estimation_start = time.perf_counter()
            self.logger.info(f"Export job {job.id}: estimating record count...")
            estimated_count = self.estimate_export_count()
            estimation_time = time.perf_counter() - estimation_start

            self.logger.info(
                f"Export job {job.id}: estimated {estimated_count:,} records to export "
                f"in {estimation_time:.3f}s"
            )

            # Update job reason with progress info
            job.reason = json.dumps(
                {
                    **job_metadata,
                    "estimated_count": estimated_count,
                    "progress": 0,
                    "status": "processing",
                    "started_at": timezone.now().isoformat(),
                }
            )
            job.save()

            # Perform the export
            export_start = time.perf_counter()
            self.logger.info(
                f"Export job {job.id}: starting export to {job.source_uri}"
            )
            if job.source_uri is None:
                raise ValueError("Job source_uri cannot be None for export jobs")

            self.export_potential_matches(
                sink=job.source_uri,
                estimated_count=estimated_count,  # Pass the already calculated count
            )
            export_time = time.perf_counter() - export_start

            # Mark job as succeeded
            job.status = JobStatus.succeeded
            job.reason = json.dumps(
                {
                    **job_metadata,
                    "estimated_count": estimated_count,
                    "progress": 100,
                    "status": "completed",
                    "completed_at": timezone.now().isoformat(),
                }
            )
            job.save()

            end_time = time.perf_counter()
            elapsed_time = end_time - start_time

            self.logger.info(
                f"Export job {job.id} completed successfully in {elapsed_time:.5f} seconds "
                f"({estimated_count:,} records exported to {job.source_uri}) "
                f"[estimation: {estimation_time:.3f}s, export: {export_time:.3f}s]"
            )

        except Exception as e:
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time

            self.logger.exception(
                f"Export job {job.id} failed after {elapsed_time:.5f} seconds: {str(e)}"
            )

            job.status = JobStatus.failed
            job.reason = json.dumps(
                {
                    **(job_metadata if job_metadata else {}),
                    "error": str(e),
                    "failed_at": timezone.now().isoformat(),
                    "elapsed_seconds": elapsed_time,
                }
            )
            job.save()
            raise

    def import_person_records(self, source: str | UploadedFile, config_id: int) -> int:
        self.logger.info("Importing person records")

        try:
            csv_col_names = [
                f.column
                for f in PersonRecordStaging._meta.get_fields()
                if isinstance(f, Field)
                and f.column not in {"id", "created", "job_id", "row_number", "sha256"}
            ]
            expected_csv_header = ",".join(csv_col_names)

            with open_source(source) as f:
                csv_header = f.readline().decode("utf-8").strip()

            if csv_header != expected_csv_header:
                msg = (
                    "Incorrectly formatted person records file due to invalid header."
                    f" Expected header: '{expected_csv_header}'"
                    f" Actual header: '{csv_header}'"
                )
                self.logger.error(msg)
                raise InvalidPersonRecordFileFormat(msg)

            with transaction.atomic(durable=True):
                with connection.cursor() as cursor:
                    # Create job
                    job = self.create_job(source, config_id)
                    table = PersonRecordStaging._meta.db_table
                    temp_table = table + "_temp"

                    # 1. Create temporary table
                    self.logger.info("Creating temporary table")
                    result = self._create_raw_temp_table(cursor, temp_table, csv_col_names, self.logger)
                    if not result["success"]:
                        raise PersonRecordImportError(result["error"])

                    # 2. Load data into temp table
                    self.logger.info("Loading data into temporary table")
                    result = self._load_csv_into_temp_table(cursor, temp_table, source, csv_col_names, self.logger, config_id)
                    if not result["success"]:
                        raise InvalidPersonRecordFileFormat(result["error"])

                    # 3. Remove invalid rows and dedupe
                    self.logger.info("Cleaning up invalid records, and deduping")
                    dedupe_result = remove_invalid_and_dedupe(cursor, temp_table)
                    if not dedupe_result["success"]:
                        raise PersonRecordImportError(result["error"])

                    # 4. Create all transformation functions
                    self.logger.info("Creating preprocessing transformations")
                    create_result = create_transformation_functions(cursor)
                    if not create_result["success"]:
                        raise PersonRecordImportError(result["error"])

                    # 5. Apply transformations
                    self.logger.info("Applying preprocessing transformations")
                    transformation_result = transform_all_columns(cursor, temp_table)
                    if not transformation_result["success"]:
                        raise PersonRecordImportError(result["error"])

                    # TODO add as util function in main.util.sql
                    # Load data from temporary table into PersonRecordStaging table,
                    # including job_id column

                    self.logger.info("Inserting into staging table")
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
                    final_count = cursor.rowcount
                    self.logger.info(f"successfully inserted {final_count} records")
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
        self,
        cursor: CursorWrapper,
        match_group_id: int,
        fields: str = "id,first_name,last_name,data_source",
    ) -> list[PersonDict]:
        """Get persons for a potential match with selective field loading.

        Args:
            cursor: Database cursor
            match_group_id: ID of the match group
            fields: Comma-separated list of fields to include (default: essential fields only)
        """
        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        # Define available fields and their SQL mappings
        available_fields = {
            "id": "pr.id",
            "created": "to_char(pr.created, %(timestamp_format)s)",
            "person_uuid": "mp.uuid",
            "person_updated": "to_char(pr.person_updated, %(timestamp_format)s)",
            "matched_or_reviewed": "pr.matched_or_reviewed",
            "data_source": "pr.data_source",
            "source_person_id": "pr.source_person_id",
            "first_name": "pr.first_name",
            "last_name": "pr.last_name",
            "sex": "pr.sex",
            "race": "pr.race",
            "birth_date": "pr.birth_date",
            "death_date": "pr.death_date",
            "social_security_number": "pr.social_security_number",
            "address": "pr.address",
            "city": "pr.city",
            "state": "pr.state",
            "zip_code": "pr.zip_code",
            "county": "pr.county",
            "phone": "pr.phone",
        }

        # Parse requested fields
        requested_fields = [f.strip() for f in fields.split(",")]
        # Validate fields
        invalid_fields = [f for f in requested_fields if f not in available_fields]
        if invalid_fields:
            raise ValueError(f"Invalid fields: {invalid_fields}")

        # SECURE approach - Use predefined query templates to prevent SQL injection
        # We'll use a mapping of field combinations to pre-built, safe SQL queries

        # Validate all requested fields are in our whitelist
        invalid_fields = [f for f in requested_fields if f not in available_fields]
        if invalid_fields:
            raise ValueError(f"Invalid fields: {invalid_fields}")

        # Sort fields for consistent query selection
        requested_fields_sorted = sorted(requested_fields)
        fields_key = ",".join(requested_fields_sorted)

        # Pre-defined safe query templates for common field combinations
        # This is 100% safe from SQL injection as all queries are hardcoded
        query_templates = {
            "data_source,first_name,id,last_name": """
                array_agg(
                    jsonb_build_object(
                        'id', pr.id,
                        'first_name', pr.first_name,
                        'last_name', pr.last_name,
                        'data_source', pr.data_source
                    )
                ) as records
            """,
            "id,first_name,last_name": """
                array_agg(
                    jsonb_build_object(
                        'id', pr.id,
                        'first_name', pr.first_name,
                        'last_name', pr.last_name
                    )
                ) as records
            """,
            "id,data_source": """
                array_agg(
                    jsonb_build_object(
                        'id', pr.id,
                        'data_source', pr.data_source
                    )
                ) as records
            """,
            "id": """
                array_agg(
                    jsonb_build_object(
                        'id', pr.id
                    )
                ) as records
            """,
            "county,death_date,phone,social_security_number": """
                array_agg(
                    jsonb_build_object(
                        'id', pr.id,
                        'county', pr.county,
                        'death_date', pr.death_date,
                        'phone', pr.phone,
                        'social_security_number', pr.social_security_number
                    )
                ) as records
            """,
        }

        # Use the most specific template available, or build dynamic template for requested fields
        if fields_key in query_templates:
            records_clause = query_templates[fields_key]
        else:
            # Build dynamic template for requested fields (still secure as we validate fields above)
            field_mappings = []
            for field in requested_fields_sorted:
                if field == "id":
                    field_mappings.append("'id', pr.id")
                elif field == "created":
                    field_mappings.append(
                        "'created', to_char(pr.created, %(timestamp_format)s)"
                    )
                elif field == "person_uuid":
                    field_mappings.append("'person_uuid', mp.uuid")
                elif field == "person_updated":
                    field_mappings.append(
                        "'person_updated', to_char(pr.person_updated, %(timestamp_format)s)"
                    )
                elif field == "matched_or_reviewed":
                    field_mappings.append(
                        "'matched_or_reviewed', pr.matched_or_reviewed"
                    )
                elif field == "data_source":
                    field_mappings.append("'data_source', pr.data_source")
                elif field == "source_person_id":
                    field_mappings.append("'source_person_id', pr.source_person_id")
                elif field == "first_name":
                    field_mappings.append("'first_name', pr.first_name")
                elif field == "last_name":
                    field_mappings.append("'last_name', pr.last_name")
                elif field == "sex":
                    field_mappings.append("'sex', pr.sex")
                elif field == "race":
                    field_mappings.append("'race', pr.race")
                elif field == "birth_date":
                    field_mappings.append("'birth_date', pr.birth_date")
                elif field == "death_date":
                    field_mappings.append("'death_date', pr.death_date")
                elif field == "social_security_number":
                    field_mappings.append(
                        "'social_security_number', pr.social_security_number"
                    )
                elif field == "address":
                    field_mappings.append("'address', pr.address")
                elif field == "city":
                    field_mappings.append("'city', pr.city")
                elif field == "state":
                    field_mappings.append("'state', pr.state")
                elif field == "zip_code":
                    field_mappings.append("'zip_code', pr.zip_code")
                elif field == "county":
                    field_mappings.append("'county', pr.county")
                elif field == "phone":
                    field_mappings.append("'phone', pr.phone")

            # Always include id field for consistency
            if "'id', pr.id" not in field_mappings:
                field_mappings.insert(0, "'id', pr.id")

            records_clause = f"""
                array_agg(
                    jsonb_build_object(
                        {', '.join(field_mappings)}
                    )
                ) as records
            """

        # Use a completely static query with pre-built safe templates
        get_persons_sql = sql.SQL(
            """
                with match_persons as (
                    -- First, get distinct person IDs involved in this match group
                    select distinct p.id as person_id, p.uuid, p.created, p.version
                    from {match_group_table} mg
                    inner join {splink_result_table} sr
                        on mg.id = %(match_group_id)s
                        and mg.id = sr.match_group_id
                    inner join {person_record_table} pr
                        on sr.person_record_l_id = pr.id
                            or sr.person_record_r_id = pr.id
                    inner join {person_table} p
                        on pr.person_id = p.id
                    order by p.id
                ),
                person_records as (
                    -- Then, get selected fields for these persons using safe template
                    select
                        mp.person_id,
                        mp.uuid,
                        mp.created,
                        mp.version,
                        {records_clause}
                    from match_persons mp
                    inner join {person_record_table} pr on mp.person_id = pr.person_id
                    group by mp.person_id, mp.uuid, mp.created, mp.version
                )
                select
                    uuid::text as uuid,
                    created,
                    version,
                    records
                from person_records
                order by uuid
            """
        ).format(
            match_group_table=sql.Identifier(match_group_table),
            splink_result_table=sql.Identifier(splink_result_table),
            person_record_table=sql.Identifier(person_record_table),
            person_table=sql.Identifier(person_table),
            records_clause=sql.SQL(records_clause),
        )

        # Use server-side cursor to avoid memory issues with large datasets
        query_start_time = time.perf_counter()
        cursor.execute(
            get_persons_sql,
            {"match_group_id": match_group_id, "timestamp_format": TIMESTAMP_FORMAT},
        )
        query_time = time.perf_counter() - query_start_time
        self.logger.info(f"Query executed in {query_time:.3f}s")

        self.logger.info(
            f"Starting server-side cursor processing for potential match persons with fields: {fields}"
        )
        processing_start_time = time.perf_counter()

        def row_to_person(row_dict: Mapping[str, Any]) -> PersonDict:
            # Optimize JSON parsing for large datasets
            records = [json.loads(record) for record in row_dict["records"]]

            return PersonDict(
                uuid=row_dict["uuid"],
                created=row_dict["created"],
                version=row_dict["version"],
                records=records,
            )

        persons = []
        processed_count = 0
        batch_size = 1000

        # Process results in batches using server-side cursor
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break

            column_names = [c.name for c in cursor.description]
            batch_persons = [
                row_to_person(dict(zip(column_names, row))) for row in batch
            ]
            persons.extend(batch_persons)

            processed_count += len(batch)
            # Log progress every 5000 persons to reduce log noise
            if processed_count % 5000 == 0:
                self.logger.info(f"Processed {processed_count:,} persons so far...")

        processing_time = time.perf_counter() - processing_start_time
        self.logger.info(
            f"Completed processing {len(persons)} potential match persons in {processing_time:.3f}s (fields: {fields})"
        )
        return persons

    def get_potential_match(
        self, id: int, fields: str = "id,first_name,last_name,data_source"
    ) -> PotentialMatchDict:
        """Get PotentialMatch by ID using export-style efficient processing.

        Args:
            id: Match group ID
            fields: Comma-separated list of fields to include (default: essential fields only)
        """
        self.logger.info(
            f"Retrieving potential match with id {id} using export-style processing (fields: {fields})"
        )

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

                persons = self._get_potential_match_persons(
                    cursor, match_group.id, fields
                )

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

    def get_potential_match_person_count(self, id: int) -> int:
        """Get the total number of persons in a potential match.

        Args:
            id: Match group ID

        Returns:
            Total number of persons in the match group
        """
        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        with connection.cursor() as cursor:
            count_sql = sql.SQL(
                """
                    select count(distinct p.id) as person_count
                    from {match_group_table} mg
                    inner join {splink_result_table} sr
                        on mg.id = %(match_group_id)s
                        and mg.id = sr.match_group_id
                    inner join {person_record_table} pr
                        on sr.person_record_l_id = pr.id
                            or sr.person_record_r_id = pr.id
                    inner join {person_table} p
                        on pr.person_id = p.id
                """
            ).format(
                match_group_table=sql.Identifier(match_group_table),
                splink_result_table=sql.Identifier(splink_result_table),
                person_record_table=sql.Identifier(person_record_table),
                person_table=sql.Identifier(person_table),
            )

            cursor.execute(count_sql, {"match_group_id": id})
            result = cursor.fetchone()
            return result[0] if result else 0

    def _obtain_shared_match_update_lock(self, cursor: CursorWrapper) -> bool:
        """Obtain a lock for match update.

        This lock prevents a manual-match from being run at the same time as an auto-match via a Job.
        This is because we don't want users waiting 10-20 minutes for a Job to finish. Instead, we
        can tell if a Job is holding this lock and return early. Multiple users can share the lock
        because manual-matches are quick and we don't mind if users wait less than a second.
        """
        result = try_advisory_lock(cursor, DbLockId.match_update, shared=True)

        if not result:
            raise ConcurrentMatchUpdates(
                "A matching job is currently updating matches. Please wait until the job finishes to perform a match."
            )
        return result

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

            self.logger.info(f"Updating Person with UUID: {update['uuid']}")

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
                f"Updated Person with UUID: {update['uuid']} (ID: {person.id})"
            )
        else:
            assert "version" not in update, "Invalid Person update"

            self.logger.info(
                f"Creating new Person with record IDs: {update['new_person_record_ids']}"
            )

            # Create Person
            person = Person.objects.create(
                uuid=uuid.uuid4(),
                created=match_event.created,
                updated=match_event.created,
                record_count=len(update["new_person_record_ids"]),
            )

            self.logger.info(
                f"Created new Person with ID: {person.id} (record IDs: {update['new_person_record_ids']})"
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
        review_action_partials: Optional[list[PersonRecordIdsPartialDict]] = None,
    ) -> None:
        self.logger.info(f"Updating {len(add_action_partials)} PersonRecords as added")

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

        if review_action_partials:
            self.logger.info(
                f"Updating {len(review_action_partials)} PersonRecords as reviewed"
            )
            for action in review_action_partials:
                record_updated_count = PersonRecord.objects.filter(
                    id=action["person_record_id"]
                ).update(
                    matched_or_reviewed=match_event.created,
                )
                total_record_updated_count += record_updated_count

        add_action_partials_count = len(add_action_partials)
        review_action_partials_count = (
            len(review_action_partials) if review_action_partials else 0
        )
        if (
            total_record_updated_count
            != add_action_partials_count + review_action_partials_count
        ):
            raise Exception(
                f"Failed to update PersonRecords. Only updated {total_record_updated_count} out of {add_action_partials_count + review_action_partials_count}"
            )

        self.logger.info(f"Updated {total_record_updated_count} PersonRecords")

    def _bulk_create_person_actions(
        self,
        match_event: MatchEvent,
        match_group: MatchGroup,
        action_partials: list[PersonRecordIdsPartialDict],
        action_type: PersonActionType,
        performed_by: User,
    ) -> None:
        actions = [
            PersonAction(
                match_event_id=match_event.id,
                match_group_id=match_group.id,
                person_id=action["person_id"],
                person_record_id=action["person_record_id"],
                type=action_type,
                performed_by_id=performed_by.id,
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
        performed_by: User,
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

                self.logger.info(
                    f"Selecting MatchGroup {potential_match_id} for update"
                )
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
                    raise InvalidPotentialMatch(
                        "Potential match has already been matched"
                    )

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
                    match_event,
                    update_action_partials["add_record"],
                    update_action_partials["review_record"],
                )

                #
                # Load generated PersonActions
                #

                # FIXME: Add assertions so that if we add a record, we remove it from somewhere else and vice-versa
                # We can do assertion on final update_action_partials - group by record_id

                self._bulk_create_person_actions(
                    match_event,
                    match_group,
                    update_action_partials["review_record"],
                    PersonActionType.review,
                    performed_by,
                )
                self._bulk_create_person_actions(
                    match_event,
                    match_group,
                    update_action_partials["remove_record"],
                    PersonActionType.remove_record,
                    performed_by,
                )
                self._bulk_create_person_actions(
                    match_event,
                    match_group,
                    update_action_partials["add_record"],
                    PersonActionType.add_record,
                    performed_by,
                )

                #
                # Create 'match' MatchGroupAction
                #

                MatchGroupAction.objects.create(
                    match_event_id=match_event.id,
                    match_group_id=match_group.id,
                    type=MatchGroupActionType.match,
                    performed_by_id=performed_by.id,
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

    def export_person_records(self, sink: str | IO[bytes]) -> None:
        """Export person records to S3 in CSV format.

        Args:
            sink: The data sink URI or file.

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

            # Write CSV content to FileProvider stream
            with open_sink(sink) as f:
                text_stream = io.TextIOWrapper(f, encoding="utf-8", newline="")
                writer = csv.writer(text_stream, lineterminator="\n")

                # Write headers
                column_names = [c.name for c in cursor.description]
                writer.writerow(column_names)

                # Write data
                for row in cursor.fetchall():
                    writer.writerow(row)

                text_stream.flush()
                text_stream.detach()

            self.logger.info(
                f"Wrote {cursor.rowcount} person records to {get_uri(sink) if isinstance(sink, str) else 'buffer'}"
            )

    def export_potential_matches(
        self,
        sink: str | IO[bytes],
        estimated_count: Optional[int] = None,
    ) -> None:
        """Export potential matches to CSV format.

        This method efficiently exports potential matches in a pairwise format,
        where each row represents a potential match between two person records.
        The export is designed to handle large datasets without memory issues.

        The export includes source_person_id and birth_date fields to help users
        trace back to their original input data and better identify individuals.

        Args:
            sink: The data sink URI or file.
            estimated_count: Pre-calculated count (optional, for job processing)

        Raises:
            UploadError: If the upload fails.
        """
        self.logger.info("Exporting potential matches to CSV")

        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        # Get estimated count early for performance monitoring and chunk sizing
        if estimated_count is None:
            # Only calculate if not provided (for direct API calls)
            estimated_count = self.estimate_export_count()
            self.logger.info(f"Export estimated count: {estimated_count:,} records")
        else:
            # Use provided count (from job processing)
            self.logger.info(
                f"Using provided estimated count: {estimated_count:,} records"
            )

        # Use server-side cursor for memory-efficient processing of large datasets
        # Wrap in transaction to support DECLARE CURSOR
        with transaction.atomic():
            with connection.cursor() as cursor:
                # Log database connection info
                pid = cursor.connection.info.backend_pid
                self.logger.info(
                    f"[pg_pid={pid}] Starting export with server-side cursor"
                )

                # Set cursor name for server-side cursor
                cursor_name = f"export_cursor_{uuid.uuid4().hex[:8]}"
                self.logger.info(f"[pg_pid={pid}] Using cursor: {cursor_name}")

                # Create the optimized query with server-side cursor
                # Simplified query for better performance - direct joins without complex CTE
                export_sql = sql.SQL("""
                    declare {cursor_name} cursor for
                    select
                        'pm_' || mg.id as group_id,
                        'Person_' || p1.id as person1_id,
                        pr1.first_name as person1_first_name,
                        pr1.last_name as person1_last_name,
                        pr1.birth_date as person1_birth_date,
                        pr1.data_source as person1_data_source,
                        pr1.source_person_id as person1_source_id,
                        'Person_' || p2.id as person2_id,
                        pr2.first_name as person2_first_name,
                        pr2.last_name as person2_last_name,
                        pr2.birth_date as person2_birth_date,
                        pr2.data_source as person2_data_source,
                        pr2.source_person_id as person2_source_id,
                        sr.match_probability
                    from {match_group_table} mg
                    inner join {splink_result_table} sr on mg.id = sr.match_group_id
                    inner join {person_record_table} pr1 on sr.person_record_l_id = pr1.id
                    inner join {person_table} p1 on pr1.person_id = p1.id
                    inner join {person_record_table} pr2 on sr.person_record_r_id = pr2.id
                    inner join {person_table} p2 on pr2.person_id = p2.id
                    where mg.matched is null
                        and mg.deleted is null
                    order by mg.id, sr.match_probability desc
                """).format(
                    cursor_name=sql.Identifier(cursor_name),
                    match_group_table=sql.Identifier(match_group_table),
                    splink_result_table=sql.Identifier(splink_result_table),
                    person_record_table=sql.Identifier(person_record_table),
                    person_table=sql.Identifier(person_table),
                )

                # Execute the cursor declaration with performance monitoring for large exports
                cursor_declare_start = time.perf_counter()
                cursor.execute(export_sql)
                cursor_declare_time = time.perf_counter() - cursor_declare_start

                self.logger.info(
                    f"[pg_pid={pid}] Cursor declared in {cursor_declare_time:.3f}s"
                )

                # Log query performance details for large exports
                if estimated_count > 100000:
                    self.logger.info(
                        f"[pg_pid={pid}] Large export detected ({estimated_count:,} records) - monitoring performance"
                    )

                    # Note: Query plan generation removed to avoid SQL complexity issues
                    # Performance monitoring is handled through timing and progress logging

                # Write CSV content to sink with streaming
                with open_sink(sink) as f:
                    text_stream = io.TextIOWrapper(f, encoding="utf-8", newline="")
                    writer = csv.writer(text_stream, lineterminator="\n")

                    # Write headers
                    headers = [
                        "group_id",
                        "person1_id",
                        "person1_first_name",
                        "person1_last_name",
                        "person1_birth_date",
                        "person1_data_source",
                        "person1_source_id",
                        "person2_id",
                        "person2_first_name",
                        "person2_last_name",
                        "person2_birth_date",
                        "person2_data_source",
                        "person2_source_id",
                        "match_probability",
                    ]
                    writer.writerow(headers)

                    # Dynamic chunk sizing based on dataset size
                    if estimated_count > 1000000:
                        chunk_size = 10000  # Larger chunks for very large datasets
                        self.logger.info(
                            f"[pg_pid={pid}] Using large chunk size ({chunk_size}) for {estimated_count:,} estimated records"
                        )
                    elif estimated_count > 100000:
                        chunk_size = 5000  # Medium chunks for large datasets
                        self.logger.info(
                            f"[pg_pid={pid}] Using medium chunk size ({chunk_size}) for {estimated_count:,} estimated records"
                        )
                    else:
                        chunk_size = 2000  # Default chunk size for smaller datasets
                        self.logger.info(
                            f"[pg_pid={pid}] Using default chunk size ({chunk_size}) for {estimated_count:,} estimated records"
                        )
                    total_rows = 0
                    start_time = time.perf_counter()
                    chunk_count = 0
                    last_progress_time = start_time
                    progress_interval = 5.0  # Update progress every 5 seconds

                    while True:
                        # Fetch chunk from cursor
                        fetch_start = time.perf_counter()
                        fetch_sql = sql.SQL(
                            "fetch {chunk_size} from {cursor_name}"
                        ).format(
                            chunk_size=sql.Literal(chunk_size),
                            cursor_name=sql.Identifier(cursor_name),
                        )
                        cursor.execute(fetch_sql)
                        fetch_time = time.perf_counter() - fetch_start

                        rows = cursor.fetchall()
                        if not rows:
                            break

                        # Write chunk to CSV
                        write_start = time.perf_counter()
                        writer.writerows(rows)  # Use writerows for better performance
                        write_time = time.perf_counter() - write_start

                        total_rows += len(rows)
                        chunk_count += 1

                        # Log performance metrics for debugging
                        if chunk_count % 10 == 0:  # Log every 10 chunks
                            self.logger.debug(
                                f"[pg_pid={pid}] Chunk {chunk_count}: fetch={fetch_time:.3f}s, write={write_time:.3f}s, rows={len(rows)}"
                            )

                        # Update progress bar every 2 seconds instead of every 10k rows
                        current_time = time.perf_counter()
                        if current_time - last_progress_time >= progress_interval:
                            elapsed = current_time - start_time
                            rate = total_rows / elapsed if elapsed > 0 else 0

                            # Handle cases where estimated_count is None or 0
                            if estimated_count and estimated_count > 0:
                                progress_percent = total_rows / estimated_count * 100
                                # Create progress bar
                                bar_length = 30
                                filled_length = int(
                                    bar_length * total_rows // estimated_count
                                )
                                bar = "█" * filled_length + "░" * (
                                    bar_length - filled_length
                                )

                                # Single line progress update with carriage return
                                progress_msg = (
                                    f"[pg_pid={pid}] Export progress: {total_rows:,}/{estimated_count:,} "
                                    f"({progress_percent:.1f}%) [{bar}] "
                                    f"{rate:.0f} rows/sec | {elapsed:.1f}s elapsed"
                                )
                            else:
                                # Fallback when no estimated count available
                                progress_msg = (
                                    f"[pg_pid={pid}] Export progress: {total_rows:,} rows processed "
                                    f"| {rate:.0f} rows/sec | {elapsed:.1f}s elapsed"
                                )

                            # Use logger with a unique identifier for progress updates
                            self.logger.info(f"PROGRESS_UPDATE: {progress_msg.strip()}")

                            last_progress_time = current_time
                            text_stream.flush()

                    # Close the cursor
                    close_start = time.perf_counter()
                    close_sql = sql.SQL("close {cursor_name}").format(
                        cursor_name=sql.Identifier(cursor_name)
                    )
                    cursor.execute(close_sql)
                    close_time = time.perf_counter() - close_start

                    text_stream.flush()
                    text_stream.detach()

                    # Show final 100% progress before moving to next line
                    if estimated_count and estimated_count > 0:
                        final_elapsed = time.perf_counter() - start_time
                        final_rate = (
                            total_rows / final_elapsed if final_elapsed > 0 else 0
                        )
                        bar = "█" * 30  # Full progress bar
                        final_progress_msg = (
                            f"[pg_pid={pid}] Export progress: {total_rows:,}/{estimated_count:,} "
                            f"(100.0%) [{bar}] "
                            f"{final_rate:.0f} rows/sec | {final_elapsed:.1f}s elapsed"
                        )
                        self.logger.info(
                            f"PROGRESS_UPDATE: {final_progress_msg.strip()}"
                        )
                    else:
                        # Handle case where no estimated count was available
                        final_elapsed = time.perf_counter() - start_time
                        final_rate = (
                            total_rows / final_elapsed if final_elapsed > 0 else 0
                        )
                        final_progress_msg = (
                            f"[pg_pid={pid}] Export completed: {total_rows:,} rows processed "
                            f"| {final_rate:.0f} rows/sec | {final_elapsed:.1f}s elapsed"
                        )
                        self.logger.info(
                            f"PROGRESS_UPDATE: {final_progress_msg.strip()}"
                        )

                    # Log completion
                    self.logger.info(f"[pg_pid={pid}] Export progress completed")

                    self.logger.info(
                        f"[pg_pid={pid}] Cursor closed in {close_time:.3f}s"
                    )

                end_time = time.perf_counter()
                total_elapsed = end_time - start_time
                final_rate = total_rows / total_elapsed if total_elapsed > 0 else 0
                avg_chunk_time = total_elapsed / chunk_count if chunk_count > 0 else 0

                self.logger.info(
                    f"Export completed: {total_rows:,} potential match pairs written to "
                    f"{get_uri(sink) if isinstance(sink, str) else 'buffer'} "
                    f"in {total_elapsed:.2f}s ({final_rate:.0f} rows/sec) "
                    f"[chunks: {chunk_count}, avg chunk time: {avg_chunk_time:.3f}s]"
                )

    def estimate_export_count(self) -> int:
        """Estimate the number of records that will be exported.

        This method runs a count query to estimate the total number of records
        that will be exported, useful for progress tracking.

        Returns:
            Estimated number of records to be exported
        """
        match_group_table = MatchGroup._meta.db_table
        splink_result_table = SplinkResult._meta.db_table
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table

        with connection.cursor() as cursor:
            # Log database connection info
            pid = cursor.connection.info.backend_pid
            self.logger.info(f"[pg_pid={pid}] Starting export count estimation")

            # Count estimation that matches the actual export logic - count potential match pairs
            # This counts the actual SplinkResult rows that will be exported
            count_sql = sql.SQL("""
                select count(*)
                from {match_group_table} mg
                inner join {splink_result_table} sr on mg.id = sr.match_group_id
                inner join {person_record_table} pr1 on sr.person_record_l_id = pr1.id
                inner join {person_table} p1 on pr1.person_id = p1.id
                inner join {person_record_table} pr2 on sr.person_record_r_id = pr2.id
                inner join {person_table} p2 on pr2.person_id = p2.id
                where mg.matched is null
                    and mg.deleted is null
            """).format(
                match_group_table=sql.Identifier(match_group_table),
                splink_result_table=sql.Identifier(splink_result_table),
                person_record_table=sql.Identifier(person_record_table),
                person_table=sql.Identifier(person_table),
            )

            count_start = time.perf_counter()
            cursor.execute(count_sql)
            count_time = time.perf_counter() - count_start

            result = cursor.fetchone()
            estimated_count = result[0] if result else 0

            self.logger.info(
                f"[pg_pid={pid}] Count estimation completed in {count_time:.3f}s: {estimated_count:,} records"
            )

            # Log performance comparison
            if count_time > 1.0:
                self.logger.info(
                    f"[pg_pid={pid}] ⚠️  Count estimation took {count_time:.3f}s - consider adding indexes for better performance"
                )
            else:
                self.logger.info(
                    f"[pg_pid={pid}] ✅ Count estimation completed in {count_time:.3f}s"
                )

            # Log accuracy note
            self.logger.info(
                f"[pg_pid={pid}] 📊 Estimated {estimated_count:,} potential match pairs (SplinkResult rows) to be exported"
            )

            return estimated_count

    @staticmethod
    def _create_raw_temp_table(cursor: CursorWrapper, temp_table: str, columns: list[str], logger: logging.Logger) -> TableResult:
        """Create temporary table with all TEXT columns for raw data loading."""

        if not columns:
            error_msg = "Cannot create table with no columns"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

        try:
            column_defs = [f'"{col}" TEXT' for col in columns]
            create_sql = f"""
                CREATE TEMP TABLE {temp_table} (
                    {', '.join(column_defs)}
                )
            """
            cursor.execute(create_sql)
            return TableResult(success=True, message=f"Table '{temp_table}' created successfully.")

        except psycopg.ProgrammingError as e:
            if "permission denied" in str(e).lower():
                error_msg = f"Insufficient permissions to create table '{temp_table}': {e}"
            else:
                error_msg = f"SQL syntax or schema error creating table '{temp_table}': {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except psycopg.OperationalError as e:
            if "disk" in str(e).lower() or "space" in str(e).lower():
                error_msg = f"Insufficient disk space to create table '{temp_table}': {e}"
            elif "connection" in str(e).lower():
                error_msg = f"Database connection lost while creating table '{temp_table}': {e}"
            else:
                error_msg = f"Database operational error creating table '{temp_table}': {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except psycopg.DatabaseError as e:
            error_msg = f"Database error creating table '{temp_table}': {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except Exception as e:
            error_msg = f"Unexpected error creating table '{temp_table}': {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

    @staticmethod
    def _load_csv_into_temp_table(cursor: CursorWrapper, temp_table: str, source: str | UploadedFile,
                              columns: list[str], logger: logging.Logger, job_id: int) -> TableResult:
        """Load CSV data into temporary table."""
        try:
            # Use PostgreSQL COPY for efficient loading
            copy_sql = sql.SQL("COPY {table} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER)").format(
                table=sql.Identifier(temp_table),
                columns=sql.SQL(', ').join(sql.Identifier(col) for col in columns)
            )

            # Load data using chunked streaming approach like import_person_records
            with open_source(source) as f, cursor.copy(copy_sql) as copy:
                while chunk := f.read(DEFAULT_BUFFER_SIZE):
                    copy.write(chunk)

            # Get count of loaded records
            count_sql = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                table=sql.Identifier(temp_table)
            )
            cursor.execute(count_sql)
            loaded_count = cursor.fetchone()[0]

            logger.info(f"Loaded {loaded_count:,} records from CSV into {temp_table}")
            return TableResult(success=True, message=loaded_count)  # Return count as integer


        except FileNotFoundError as e:
            error_msg = f"CSV file not found: {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except PermissionError as e:
            error_msg = f"Permission denied reading CSV file: {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except UnicodeDecodeError as e:
            error_msg = f"CSV file encoding error - file may not be UTF-8: {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except psycopg.Error as e:
            error_msg = f"Database error during CSV load: {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)

        except Exception as e:
            error_msg = f"Unexpected error loading CSV into temp table '{temp_table}': {e}"
            logger.error(error_msg)
            return TableResult(success=False, error=error_msg)
