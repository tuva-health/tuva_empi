import copy
import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta
from datetime import timezone as tz
from typing import Any, Mapping, TypedDict, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pandas.testing as pdt
import psycopg
from django.conf import settings
from django.db import DatabaseError, OperationalError, connection, transaction
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from psycopg import sql
from splink.dsl import col

from main.models import (
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
from main.services.empi.empi_service import EMPIService
from main.services.matching.matcher import Matcher
from main.tests.testing.concurrency import run_with_lock_contention
from main.util.dict import select_keys
from main.util.sql import load_df


class HashableRecordPartialDict(TypedDict):
    data_source: str
    source_person_id: str
    first_name: str | None
    last_name: str | None
    sex: str | None
    race: str | None
    birth_date: str | None
    death_date: str | None
    social_security_number: str | None
    address: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    county: str | None
    phone: str | None


test_splink_settings = {
    "probability_two_random_records_match": 0.00298012298012298,
    "em_convergence": 0.0001,
    "max_iterations": 25,
    "blocking_rules_to_generate_predictions": [
        {"blocking_rule": '(l."last_name" = r."last_name")'},
    ],
    "comparisons": [
        {
            "output_column_name": "first_name",
            "comparison_levels": [
                {
                    "sql_condition": col("first_name").is_null(),
                    "label_for_charts": "first_name is NULL",
                    "is_null_level": True,
                },
                {
                    "sql_condition": col("first_name_l") == col("first_name_r"),
                    "label_for_charts": "Exact match on first_name",
                    "m_probability": 0.49142094931763786,
                    "u_probability": 0.0057935713975033705,
                    "tf_adjustment_column": "first_name",
                    "tf_adjustment_weight": 1.0,
                },
                {
                    "sql_condition": 'jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.92',
                    "label_for_charts": "Jaro-Winkler distance of first_name >= 0.92",
                    "m_probability": 0.15176057384758357,
                    "u_probability": 0.0023429457903817435,
                },
                {
                    "sql_condition": 'jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.88',
                    "label_for_charts": "Jaro-Winkler distance of first_name >= 0.88",
                    "m_probability": 0.07406496776118936,
                    "u_probability": 0.0015484319951285285,
                },
                {
                    "sql_condition": 'jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.7',
                    "label_for_charts": "Jaro-Winkler distance of first_name >= 0.7",
                    "m_probability": 0.07908610771504865,
                    "u_probability": 0.018934945558406913,
                },
                {
                    "sql_condition": "ELSE",
                    "label_for_charts": "All other comparisons",
                    "m_probability": 0.20366740135854072,
                    "u_probability": 0.9713801052585794,
                },
            ],
            "comparison_description": "NameComparison",
        }
    ],
}


class MatcherTestCase(TestCase):
    logger: logging.Logger
    splink_settings: dict[str, Any]

    def setUp(self) -> None:
        self.maxDiff = None
        self.logger = logging.getLogger(__name__)
        self.splink_settings = copy.deepcopy(test_splink_settings)

    @staticmethod
    def sha256(value: str) -> bytes:
        return hashlib.sha256(value.encode()).digest()

    @staticmethod
    def get_record_sha256(record: HashableRecordPartialDict) -> bytes:
        return MatcherTestCase.sha256(
            "|".join(
                [
                    record["data_source"],
                    record["source_person_id"],
                    record["first_name"] or "",
                    record["last_name"] or "",
                    record["sex"] or "",
                    record["race"] or "",
                    record["birth_date"] or "",
                    record["death_date"] or "",
                    record["social_security_number"] or "",
                    record["address"] or "",
                    record["city"] or "",
                    record["state"] or "",
                    record["zip_code"] or "",
                    record["county"] or "",
                    record["phone"] or "",
                ]
            )
        )

    def test_load_person_records(self) -> None:
        person_record_table = PersonRecord._meta.db_table
        person_table = Person._meta.db_table
        person_action_table = PersonAction._meta.db_table

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.5,
                "auto_match_threshold": 1.0,
            }
        )
        job1 = empi.create_job("s3://tuva-health-example/test", config.id)
        job2 = empi.create_job("s3://tuva-health-example/test", config.id)

        person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job1,
            version=1,
            record_count=1,
        )

        common_partial_stg_record = {
            "created": timezone.now(),
            "data_source": "example-ds-1",
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }

        new_person_records = [
            {
                **common_partial_stg_record,
                "job_id": job1.id,
                "person_id": person1.id,
                "person_updated": timezone.now(),
                "matched_or_reviewed": timezone.now(),
            }
        ]
        new_person_records = [
            {
                "sha256": self.get_record_sha256(
                    cast(HashableRecordPartialDict, record)
                ),
                **record,
            }
            for record in new_person_records
        ]

        new_stg_person_records = [
            {
                **common_partial_stg_record,
                "job_id": job2.id,
                "data_source": "example-ds-2",
            },
            {
                **common_partial_stg_record,
                "job_id": job2.id,
                "data_source": "example-ds-2",
            },
            {**common_partial_stg_record, "job_id": job2.id},
        ]

        #
        # Given that person records already exist in the PersonRecords table
        #

        for record in new_person_records:
            PersonRecord.objects.create(**record)

        self.assertEqual(PersonRecord.objects.count(), 1)

        #
        # Given that person records exist in a staging table
        #

        for record in new_stg_person_records:
            PersonRecordStaging.objects.create(**record)

        with connection.cursor() as cursor:
            num_records_loaded = Matcher().load_person_records(cursor, job2)

        #
        # Staging records should get a sha256
        #

        stg_person_records = list(PersonRecordStaging.objects.values())

        self.assertEqual(
            [self.get_record_sha256(record) for record in stg_person_records],
            [record["sha256"] for record in stg_person_records],
        )

        #
        # A new-ids MatchEvent should be created
        #

        match_events = list(MatchEvent.objects.all())

        self.assertEqual(len(match_events), 1)

        match_event = match_events[0]

        self.assertTrue(isinstance(match_event.id, int))
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < match_event.created
            < timezone.now()
        )
        self.assertEqual(match_event.job_id, job2.id)
        self.assertEqual(match_event.type, MatchEventType.new_ids.value)

        #
        # Staging records should get deduped against the person record table and against themselves
        # and loaded into the PersonRecord table
        #

        # We should have only loaded 1 record
        self.assertEqual(num_records_loaded, 1)

        loaded_person_record_dict = (
            PersonRecord.objects.filter(data_source="example-ds-2").values().first()
        )

        if loaded_person_record_dict is None:
            self.fail()

        # And there should now exist 2 records in the PersonRecord table
        self.assertEqual(PersonRecord.objects.count(), 2)

        common_keys = [
            "job_id",
            "data_source",
            "source_person_id",
            "first_name",
            "last_name",
            "sex",
            "race",
            "birth_date",
            "death_date",
            "social_security_number",
            "address",
            "city",
            "state",
            "zip_code",
            "county",
            "phone",
        ]
        self.assertDictEqual(
            select_keys(new_stg_person_records[0], common_keys),
            select_keys(loaded_person_record_dict, common_keys),
        )
        self.assertTrue(isinstance(loaded_person_record_dict["id"], int))
        self.assertEqual(loaded_person_record_dict["created"], match_event.created)
        self.assertEqual(loaded_person_record_dict["matched_or_reviewed"], None)
        self.assertTrue(isinstance(loaded_person_record_dict["person_id"], int))
        self.assertEqual(
            loaded_person_record_dict["person_updated"], match_event.created
        )
        self.assertEqual(
            loaded_person_record_dict["sha256"],
            self.get_record_sha256(loaded_person_record_dict),
        )

        #
        # Once loaded into the PersonRecord table, each new record should get a new unique Person
        #

        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    select count(*)
                    from {person_record_table} pr
                    inner join {person_table} p on
                        pr.person_id = p.id
                """
                ).format(
                    person_record_table=sql.Identifier(person_record_table),
                    person_table=sql.Identifier(person_table),
                )
            )
            count = cursor.fetchone()[0]

            self.assertTrue(
                PersonRecord.objects.count() == count == Person.objects.count()
            )

        loaded_person_record = PersonRecord.objects.filter(
            data_source="example-ds-2"
        ).first()

        if loaded_person_record is None:
            self.fail()

        loaded_person = loaded_person_record.person

        self.assertTrue(isinstance(loaded_person.id, int))
        self.assertEqual(loaded_person.uuid.version, 4)
        self.assertEqual(loaded_person.created, match_event.created)
        self.assertEqual(loaded_person.updated, match_event.created)
        self.assertEqual(loaded_person.job_id, job2.id)
        self.assertEqual(loaded_person.version, 1)
        self.assertEqual(loaded_person.deleted, None)
        self.assertEqual(loaded_person.record_count, 1)

        #
        # Unique new ID match actions should exist for each record
        #

        # Use raw SQL with cursor to avoid row indexing issues
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    select pa.*
                    from {person_action_table} pa
                    inner join {person_table} p on
                        pa.person_id = p.id
                    inner join {person_record_table} pr on
                        p.id = pr.person_id
                    """
                ).format(
                    person_action_table=sql.Identifier(person_action_table),
                    person_record_table=sql.Identifier(person_record_table),
                    person_table=sql.Identifier(person_table),
                )
            )
            raw_results = cursor.fetchall()
            # Get column names to understand the order
            column_names = [desc[0] for desc in cursor.description]

        self.assertEqual(
            len(raw_results),
            1,
        )

        # Get the first row and create a mock object for testing
        raw_row = raw_results[0]
        # Create a dictionary mapping column names to values
        row_dict = dict(zip(column_names, raw_row))
        loaded_person_action = type("MockPersonAction", (), row_dict)()

        self.assertTrue(isinstance(loaded_person_action.id, int))
        self.assertEqual(loaded_person_action.match_event_id, match_event.id)
        self.assertEqual(loaded_person_action.match_group_id, None)
        self.assertEqual(
            loaded_person_action.person_record_id, loaded_person_record_dict["id"]
        )
        self.assertEqual(
            loaded_person_action.person_id, loaded_person_record_dict["person_id"]
        )
        self.assertEqual(loaded_person_action.type, PersonActionType.add_record.value)
        self.assertEqual(loaded_person_action.performed_by_id, None)

    def test_extract_person_records(self) -> None:
        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.5,
                "auto_match_threshold": 1.0,
            }
        )
        job = empi.create_job("s3://tuva-health-example/test", config.id)

        person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )

        person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )

        person_record_table = PersonRecord._meta.db_table
        now_str = timezone.now().isoformat()

        common_person_record = {
            "created": now_str,
            "job_id": job.id,
            "person_updated": now_str,
            "matched_or_reviewed": None,
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }
        new_person_records = [
            {
                **common_person_record,
                "person_id": person1.id,
                "sha256": b"test-sha256-1",
                "data_source": "example-ds-1",
            },
            {
                **common_person_record,
                "person_id": person2.id,
                "sha256": b"test-sha256-2",
                "data_source": "example-ds-2",
            },
        ]

        new_person_record_df = pd.DataFrame(new_person_records).astype(
            {
                col: (
                    "int64"
                    if col in {"job_id", "person_id"}
                    else ("object" if col == "sha256" else "string")
                )
                for col in new_person_records[0].keys()
            }
        )

        with connection.cursor() as cursor:
            # Load PersonRecords

            load_df(
                cursor,
                person_record_table,
                new_person_record_df,
                list(new_person_records[0].keys()),
            )

            # Extract person records

            person_record_df = Matcher().extract_person_records(cursor)

            # Verify PersonRecords are similar

            pdt.assert_frame_equal(
                new_person_record_df.drop(
                    columns=[
                        "person_id",
                        "person_updated",
                        "matched_or_reviewed",
                        "sha256",
                    ]
                ).sort_index(axis=1),  # sort columns
                person_record_df.drop(columns=["id"]).sort_index(
                    axis=1
                ),  # sort columns
                check_dtype=False,
            )

    def test_run_splink_prediction(self) -> None:
        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": self.splink_settings,
                "potential_match_threshold": 0,
                "auto_match_threshold": 0.002,
            }
        )
        job = empi.create_job("s3://tuva-health-example/test", config.id)

        person = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )

        common_partial_stg_record = {
            "created": timezone.now(),
            "data_source": "example-ds-1",
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }

        new_person_records = [
            {
                **common_partial_stg_record,
                "id": 0,
                "job_id": job.id,
                "person_id": person.id,
                "person_updated": timezone.now(),
                "matched_or_reviewed": timezone.now(),
                "sha256": b"test-sha256-1",
                "first_name": "test-first-name",
                "last_name": "test-last-name",
            },
            {
                **common_partial_stg_record,
                "id": 1,
                "job_id": job.id,
                "person_id": person.id,
                "person_updated": timezone.now(),
                "matched_or_reviewed": timezone.now(),
                "sha256": b"test-sha256-2",
                "first_name": "test-first-name",
                "last_name": "test-last-name",
            },
            {
                **common_partial_stg_record,
                "id": 2,
                "job_id": job.id,
                "person_id": person.id,
                "person_updated": timezone.now(),
                "matched_or_reviewed": timezone.now(),
                "sha256": b"test-sha256-3",
                "first_name": "test-first-nam",  # Missing last letter
                "last_name": "test-last-name",
            },
            {
                **common_partial_stg_record,
                "id": 3,
                "job_id": job.id,
                "person_id": person.id,
                "person_updated": timezone.now(),
                "matched_or_reviewed": timezone.now(),
                "sha256": b"test-sha256-4",
                "first_name": "test-first-name-1",
                "last_name": "test-last-name-1",
            },
            {
                **common_partial_stg_record,
                "id": 4,
                "job_id": job.id,
                "person_id": person.id,
                "person_updated": timezone.now(),
                "matched_or_reviewed": timezone.now(),
                "sha256": b"test-sha256-5",
                "first_name": "test-first-name-2",
                "last_name": "test-last-name-2",
            },
        ]

        for record in new_person_records:
            PersonRecord.objects.create(**record)

        with connection.cursor() as cursor:
            person_record_df = Matcher().extract_person_records(cursor)

        splink_result_df = Matcher().run_splink_prediction(job, person_record_df)

        # Only the first 3 records are compared because of the blocking rules
        self.assertEqual(len(splink_result_df), 3)
        self.assertEqual(splink_result_df["person_record_l_id"].dtype, "int64")
        self.assertEqual(splink_result_df["person_record_r_id"].dtype, "int64")
        self.assertEqual(
            len(
                splink_result_df.query(
                    "person_record_l_id == 0 and person_record_r_id == 1"
                )
            ),
            1,
        )
        self.assertEqual(
            len(
                splink_result_df.query(
                    "person_record_l_id == 0 and person_record_r_id == 2"
                )
            ),
            1,
        )
        self.assertEqual(
            len(
                splink_result_df.query(
                    "person_record_l_id == 1 and person_record_r_id == 2"
                )
            ),
            1,
        )

        # match_weight/probability should be filled in with floats
        self.assertEqual(splink_result_df["match_weight"].dtype, "float64")
        self.assertEqual(splink_result_df["match_probability"].dtype, "float64")
        self.assertTrue(not splink_result_df["match_weight"].isnull().any())
        self.assertTrue(not splink_result_df["match_probability"].isnull().any())

        # All results have probability above 0
        self.assertEqual((splink_result_df["match_probability"] > 0).sum(), 3)
        # Only 2 have probability above 0.004
        self.assertEqual((splink_result_df["match_probability"] > 0.004).sum(), 2)

        # Note that in this case Splink gives a higher match_probabilty for the inexact first_name
        # matches. It seems like it's pretty sensitive to the training and can give pretty unexpected
        # results if the model doesn't match the data as in this case (model parameters are from a
        # Splink example).

        # job_id should be static
        self.assertTrue((splink_result_df["job_id"] == job.id).all())

        # id should be Int64 type and be None
        self.assertEqual(splink_result_df["id"].dtype, "Int64")
        self.assertTrue(splink_result_df["id"].isnull().all())

        # Check data column dtype, parse data JSON and check keys and values
        self.assertEqual(splink_result_df["data"].dtype, "object")

        expected_data_keys = {
            "tf_first_name_l",
            "tf_first_name_r",
            "bf_first_name",
            "bf_tf_adj_first_name",
            "match_key",
        }
        splink_result_df["parsed_data"] = splink_result_df["data"].apply(json.loads)

        def is_valid_data(data: Mapping[str, Any]) -> bool:
            return (
                data.keys() == expected_data_keys
                and isinstance(data["tf_first_name_l"], float)
                and isinstance(data["tf_first_name_r"], float)
                and isinstance(data["bf_first_name"], float)
                and isinstance(data["bf_tf_adj_first_name"], float)
                and isinstance(data["match_key"], str)
            )

        self.assertTrue(splink_result_df["parsed_data"].apply(is_valid_data).all())

        # Increase potential match threshold and ensure there are less results

        job.config.potential_match_threshold = 0.004

        splink_result_df = Matcher().run_splink_prediction(job, person_record_df)

        # There should only be two results now

        self.assertEqual(len(splink_result_df), 2)
        self.assertEqual(
            len(
                splink_result_df.query(
                    "person_record_l_id == 0 and person_record_r_id == 2"
                )
            ),
            1,
        )
        self.assertEqual(
            len(
                splink_result_df.query(
                    "person_record_l_id == 1 and person_record_r_id == 2"
                )
            ),
            1,
        )

        # Increase potential match threshold and ensure there are no results

        job.config.potential_match_threshold = 0.5

        splink_result_df = Matcher().run_splink_prediction(job, person_record_df)

        self.assertTrue(splink_result_df.empty)

    def test_get_all_results(self) -> None:
        df_current = pd.DataFrame({"id": [0]})

        df_new = pd.DataFrame({"id": [1]})

        result_df = Matcher().get_all_results(df_current, df_new)

        result_ids = sorted(result_df["id"].tolist())

        self.assertEqual([0, 1], result_ids)

        self.assertTrue(
            result_df.index.is_unique,
            "The index in the result DataFrame is not unique.",
        )

        self.assertIn(
            "row_number",
            result_df.columns,
            "The result DataFrame does not have a 'row_number' column.",
        )

        self.assertTrue(
            result_df["row_number"].is_unique,
            "The 'row_number' column in the result DataFrame is not unique.",
        )

    def test_create_match_event(self) -> None:
        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.5,
                "auto_match_threshold": 1.0,
            }
        )
        job = empi.create_job("s3://tuva-health-example/test", config.id)

        self.assertEqual(MatchEvent.objects.count(), 0)

        with connection.cursor() as cursor:
            match_event = Matcher().create_match_event(
                cursor, job, MatchEventType.auto_matches
            )

        self.assertEqual(match_event.job_id, job.id)
        self.assertEqual(match_event.type, MatchEventType.auto_matches.value)
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < match_event.created
            < timezone.now()
        )
        self.assertTrue(isinstance(match_event.id, int))

        self.assertTrue(MatchEvent.objects.filter(id=match_event.id).exists())

    def test_load_results_groups_and_actions(self) -> None:
        #
        # Load PersonRecords, Persons and Results
        #

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.5,
                "auto_match_threshold": 1.0,
            }
        )
        job1 = empi.create_job("s3://tuva-health-example/test", config.id)
        job2 = empi.create_job("s3://tuva-health-example/test", config.id)

        # Load Persons

        person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job1,
            version=1,
            record_count=1,
        )
        person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job1,
            version=1,
            record_count=1,
        )

        # Load PersonRecords

        common_person_record = {
            "created": timezone.now(),
            "job_id": job1.id,
            "person_updated": timezone.now(),
            "matched_or_reviewed": None,
            "data_source": "example-ds-1",
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }
        pr1 = PersonRecord.objects.create(
            **common_person_record,
            id=0,
            person_id=person1.id,
            sha256=b"test-sha256-1",
        )
        pr2 = PersonRecord.objects.create(
            **common_person_record,
            id=1,
            person_id=person2.id,
            sha256=b"test-sha256-2",
        )

        # Load MatchGroup

        mg1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            deleted=None,
            job_id=job1.id,
            matched=None,
        )

        # Load current SplinkResults

        result_df_columns = [
            "id",
            "job_id",
            "match_weight",
            "match_probability",
            "person_record_l_id",
            "person_record_r_id",
            "data",
        ]

        common_current_result = {
            "created": timezone.now(),
            "job_id": job1.id,
            "match_group_id": mg1.id,
            "match_group_updated": timezone.now(),
            "match_weight": 0.8,
            "match_probability": 0.9,
            "person_record_l_id": pr1.id,
            "person_record_r_id": pr2.id,
            "data": "{}",
        }
        current_result1 = SplinkResult.objects.create(
            **{**common_current_result, "match_probability": 0.85}
        )
        current_result2 = SplinkResult.objects.create(
            **{**common_current_result, "match_probability": 0.89}
        )
        current_results = [
            {**common_current_result, "id": current_result1.id, "data": None},
            {**common_current_result, "id": current_result2.id, "data": None},
        ]
        current_result_df = pd.DataFrame(current_results).astype(
            {
                "id": "int64",
                "job_id": "int64",
                "match_weight": "float64",
                "match_probability": "float64",
                "person_record_l_id": "int64",
                "person_record_r_id": "int64",
                "data": "object",
            }
        )[result_df_columns]

        self.assertEqual(current_result1.match_group_id, mg1.id)
        self.assertEqual(current_result2.match_group_id, mg1.id)

        # Create auto-matches MatchEvent

        with connection.cursor() as cursor:
            match_event = Matcher().create_match_event(
                cursor, job1, MatchEventType.auto_matches
            )

        # Create MatchGroup DF

        match_group_df = pd.DataFrame(
            [
                {"uuid": uuid.uuid4(), "matched": False},
                {"uuid": uuid.uuid4(), "matched": True},
            ]
        )

        # Create new SplinkResults DF

        common_new_result = {
            "id": None,
            "job_id": job2.id,
            "match_weight": 0.8,
            "match_probability": 0.9,
            "person_record_l_id": pr1.id,
            "person_record_r_id": pr2.id,
            "data": "{}",
        }
        new_results = [
            {**common_new_result, "match_probability": 0.98},
            {**common_new_result, "match_probability": 0.99},
        ]
        new_result_df = pd.DataFrame(new_results).astype(
            {
                "id": "Int64",
                "job_id": "int64",
                "match_weight": "float64",
                "match_probability": "float64",
                "person_record_l_id": "int64",
                "person_record_r_id": "int64",
                "data": "object",
            }
        )[result_df_columns]

        # Create combined SplinkResult DF

        splink_result_df = pd.concat([current_result_df, new_result_df]).reset_index(
            drop=True
        )
        splink_result_df["row_number"] = splink_result_df.index
        assert splink_result_df["row_number"].is_unique

        # These DataFrames have row_number added
        _current_result_df = splink_result_df.query(f"job_id == {job1.id}").reset_index(
            drop=True
        )
        _new_result_df = splink_result_df.query(f"job_id == {job2.id}").reset_index(
            drop=True
        )

        # Create MatchGroupResult DF

        match_group_result_df = pd.DataFrame(
            [
                # Current results
                {
                    "result_row_number": _current_result_df.at[0, "row_number"],
                    "match_group_uuid": match_group_df.at[0, "uuid"],
                },
                {
                    "result_row_number": _current_result_df.at[1, "row_number"],
                    "match_group_uuid": match_group_df.at[1, "uuid"],
                },
                # New results
                {
                    "result_row_number": _new_result_df.at[0, "row_number"],
                    "match_group_uuid": match_group_df.at[1, "uuid"],
                },
                {
                    "result_row_number": _new_result_df.at[1, "row_number"],
                    "match_group_uuid": match_group_df.at[1, "uuid"],
                },
            ]
        )

        #
        # Run load_results_groups_and_actions
        #

        with connection.cursor() as cursor:
            Matcher().load_results_groups_and_actions(
                cursor,
                job2,
                match_event,
                splink_result_df,
                match_group_result_df,
                match_group_df,
            )

        #
        # Check that MatchGroups are loaded
        #

        loaded_match_group1 = MatchGroup.objects.get(
            uuid=cast(str, match_group_df.loc[0, "uuid"])
        )

        self.assertTrue(isinstance(loaded_match_group1.id, int))
        self.assertEqual(loaded_match_group1.created, match_event.created)
        self.assertEqual(loaded_match_group1.updated, match_event.created)
        self.assertEqual(loaded_match_group1.deleted, None)
        self.assertEqual(loaded_match_group1.job.id, job2.id)
        self.assertEqual(loaded_match_group1.version, 1)
        self.assertEqual(loaded_match_group1.matched, None)

        loaded_match_group2 = MatchGroup.objects.get(
            uuid=cast(str, match_group_df.loc[1, "uuid"])
        )

        self.assertTrue(isinstance(loaded_match_group2.id, int))
        self.assertEqual(loaded_match_group2.created, match_event.created)
        self.assertEqual(loaded_match_group2.updated, match_event.created)
        self.assertEqual(loaded_match_group2.deleted, None)
        self.assertEqual(loaded_match_group2.job.id, job2.id)
        self.assertEqual(loaded_match_group2.version, 1)
        self.assertEqual(loaded_match_group2.matched, match_event.created)

        #
        # Check that new SplinkResults are loaded
        #

        loaded_new_results = (
            SplinkResult.objects.filter(job_id=job2.id)
            .order_by("match_probability")
            .values()
        )

        self.assertEqual(len(loaded_new_results), 2)

        self.assertEqual(
            [
                {
                    **select_keys(
                        result,
                        [
                            "job_id",
                            "match_weight",
                            "match_probability",
                            "person_record_l_id",
                            "person_record_r_id",
                        ],
                    ),
                    "data": json.loads(cast(str, result["data"])),
                    "created": match_event.created,
                    "match_group_id": loaded_match_group2.id,
                    "match_group_updated": match_event.created,
                }
                for result in new_results
            ],
            [
                select_keys(
                    result,
                    [
                        "job_id",
                        "match_weight",
                        "match_probability",
                        "person_record_l_id",
                        "person_record_r_id",
                        "data",
                        "created",
                        "match_group_id",
                        "match_group_updated",
                    ],
                )
                for result in loaded_new_results
            ],
        )
        for result in loaded_new_results:
            self.assertTrue(isinstance(result["id"], int))

        #
        # Check that MatchGroupActions for new results are loaded
        #

        # There should be 2 add_result actions

        new_result_add_actions = MatchGroupAction.objects.filter(
            match_event=match_event,
            type=MatchGroupActionType.add_result.value,
            match_group_id=loaded_match_group2.id,
            splink_result_id__in=[result["id"] for result in loaded_new_results],
            performed_by=None,
        ).all()

        self.assertEqual(len(new_result_add_actions), 2)

        for action in new_result_add_actions:
            self.assertTrue(isinstance(action.id, int))

        # And 0 remove_result actions, since the Results are new

        self.assertEqual(
            MatchGroupAction.objects.filter(
                match_event=match_event,
                type=MatchGroupActionType.remove_result.value,
                match_group_id=loaded_match_group2.id,
                splink_result_id__in=[result["id"] for result in loaded_new_results],
                performed_by=None,
            ).count(),
            0,
        )

        #
        # Check that current results are updated
        #

        self.assertEqual(
            SplinkResult.objects.filter(job_id=job1.id)
            .order_by("match_probability")
            .count(),
            2,
        )

        current_result1.refresh_from_db()
        current_result2.refresh_from_db()

        self.assertEqual(current_result1.job, job1)
        self.assertEqual(current_result1.match_group, loaded_match_group1)
        self.assertEqual(current_result1.match_group_updated, match_event.created)

        self.assertEqual(current_result2.job, job1)
        self.assertEqual(current_result2.match_group, loaded_match_group2)
        self.assertEqual(current_result2.match_group_updated, match_event.created)

        #
        # Check that MatchGroupActions for current results are loaded (remove and add)
        #

        # There should be an add action moving current_result1 to loaded_match_group1

        current_result_add_action1 = MatchGroupAction.objects.filter(
            match_event=match_event,
            type=MatchGroupActionType.add_result.value,
            match_group_id=loaded_match_group1.id,
            splink_result_id=current_result1.id,
            performed_by=None,
        ).all()

        self.assertEqual(len(current_result_add_action1), 1)
        self.assertTrue(
            isinstance(
                cast(MatchGroupAction, current_result_add_action1.first()).id, int
            )
        )

        # There should be an add action moving current_result2 to loaded_match_group2

        current_result_add_action2 = MatchGroupAction.objects.filter(
            match_event=match_event,
            type=MatchGroupActionType.add_result.value,
            match_group_id=loaded_match_group2.id,
            splink_result_id=current_result2.id,
            performed_by=None,
        ).all()

        self.assertEqual(len(current_result_add_action2), 1)
        self.assertTrue(
            isinstance(
                cast(MatchGroupAction, current_result_add_action2.first()).id, int
            )
        )

        # There should be 2 remove actions removing current_results from their original MatchGroup

        current_result_remove_actions = MatchGroupAction.objects.filter(
            match_event=match_event,
            type=MatchGroupActionType.remove_result.value,
            match_group_id=mg1.id,
            splink_result_id__in=[current_result1.id, current_result2.id],
        ).all()

        self.assertEqual(len(current_result_remove_actions), 2)

        for action in current_result_remove_actions:
            self.assertTrue(isinstance(action.id, int))

        #
        # Verify that match MatchGroupActions are loaded
        #

        # There should be a single match action (see match_group_df "matched" field)

        match_group_match_action = MatchGroupAction.objects.filter(
            match_event=match_event,
            type=MatchGroupActionType.match.value,
            match_group_id=loaded_match_group2.id,
            splink_result_id=None,
        ).all()

        self.assertEqual(len(match_group_match_action), 1)
        self.assertTrue(
            isinstance(cast(MatchGroupAction, match_group_match_action.first()).id, int)
        )

        #
        # Check that there are no extra rows loaded
        #

        all_results = SplinkResult.objects.count()
        all_match_groups = MatchGroup.objects.count()
        all_match_events = MatchEvent.objects.count()
        all_match_group_actions = MatchGroupAction.objects.count()

        self.assertEqual(all_results, len(splink_result_df))
        self.assertEqual(all_match_groups, len(match_group_df) + 1)
        self.assertEqual(all_match_events, 1)
        self.assertEqual(
            all_match_group_actions, (len(current_results) * 2) + len(new_results) + 1
        )

    def test_update_persons_and_load_actions(self) -> None:
        #
        # Load PersonRecords, Persons
        #

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.5,
                "auto_match_threshold": 1.0,
            }
        )
        job1 = empi.create_job("s3://tuva-health-example/test", config.id)
        job2 = empi.create_job("s3://tuva-health-example/test", config.id)

        # Load Persons

        person_created = timezone.now()
        person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=person_created,
            updated=person_created,
            job=job1,
            version=1,
            record_count=2,
        )
        person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=person_created,
            updated=person_created,
            job=job1,
            version=1,
            record_count=1,
        )
        person3 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=person_created,
            updated=person_created,
            job=job1,
            version=1,
            record_count=1,
        )

        # Load PersonRecords

        common_person_record = {
            "created": timezone.now(),
            "job_id": job1.id,
            "person_updated": timezone.now(),
            "matched_or_reviewed": None,
            "data_source": "example-ds-1",
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }
        pr1 = PersonRecord.objects.create(
            **common_person_record,
            person_id=person1.id,
            sha256=b"test-sha256-1",
        )
        pr2 = PersonRecord.objects.create(
            **common_person_record,
            person_id=person1.id,
            sha256=b"test-sha256-2",
        )
        pr3 = PersonRecord.objects.create(
            **common_person_record,
            person_id=person2.id,
            sha256=b"test-sha256-3",
        )
        pr4 = PersonRecord.objects.create(
            **common_person_record,
            person_id=person3.id,
            sha256=b"test-sha256-4",
        )

        self.assertEqual(pr1.person_id, person1.id)
        self.assertEqual(pr1.person_updated, common_person_record["person_updated"])

        self.assertEqual(pr2.person_id, person1.id)
        self.assertEqual(pr2.person_updated, common_person_record["person_updated"])

        self.assertEqual(pr3.person_id, person2.id)
        self.assertEqual(pr3.person_updated, common_person_record["person_updated"])

        self.assertEqual(pr4.person_id, person3.id)
        self.assertEqual(pr4.person_updated, common_person_record["person_updated"])

        # Load MatchGroups

        mg1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            deleted=None,
            job_id=job1.id,
            matched=None,
        )

        # Create auto-matches MatchEvent

        with connection.cursor() as cursor:
            match_event = Matcher().create_match_event(
                cursor, job1, MatchEventType.auto_matches
            )

        person_action_df = pd.DataFrame(
            [
                {
                    "match_group_uuid": mg1.uuid,
                    "person_record_id": pr1.id,
                    "from_person_id": person1.id,
                    "from_person_version": person1.version,
                    "to_person_id": person2.id,
                    "to_person_version": person2.version,
                },
                {
                    "match_group_uuid": mg1.uuid,
                    "person_record_id": pr2.id,
                    "from_person_id": person1.id,
                    "from_person_version": person1.version,
                    "to_person_id": person2.id,
                    "to_person_version": person2.version,
                },
            ]
        )

        #
        # Run update_persons_and_load_actions
        #

        with connection.cursor() as cursor:
            Matcher().update_persons_and_load_actions(
                cursor, job2, match_event, person_action_df
            )

        #
        # Check that PersonRecords were updated
        #

        pr1.refresh_from_db()
        pr2.refresh_from_db()
        pr3.refresh_from_db()

        self.assertEqual(pr1.person_id, person2.id)
        self.assertEqual(pr1.person_updated, match_event.created)

        self.assertEqual(pr2.person_id, person2.id)
        self.assertEqual(pr2.person_updated, match_event.created)

        self.assertEqual(pr3.person_id, person2.id)
        self.assertEqual(pr3.person_updated, common_person_record["person_updated"])

        self.assertEqual(pr4.person_id, person3.id)
        self.assertEqual(pr4.person_updated, common_person_record["person_updated"])

        #
        # Check that Persons were updated
        #

        person1.refresh_from_db()
        person2.refresh_from_db()

        self.assertTrue(person1.created < person1.updated < timezone.now())
        self.assertEqual(person1.version, 2)
        self.assertEqual(person1.record_count, 0)
        self.assertTrue(person1.deleted is not None)
        self.assertTrue(
            person1.created < cast(datetime, person1.deleted) < timezone.now()
        )

        self.assertTrue(person2.created < person2.updated < timezone.now())
        self.assertEqual(person2.version, 2)
        self.assertEqual(person2.record_count, 3)
        self.assertEqual(person2.deleted, None)

        self.assertEqual(person3.created, person3.updated)
        self.assertEqual(person3.version, 1)
        self.assertEqual(person3.record_count, 1)
        self.assertEqual(person3.deleted, None)

        #
        # Check that PersonActions were loaded
        #

        # Remove actions

        self.assertEqual(
            PersonAction.objects.filter(
                match_event_id=match_event.id,
                match_group_id=mg1.id,
                person_id=person1.id,
                person_record_id=pr1.id,
                type=PersonActionType.remove_record,
                performed_by=None,
            ).count(),
            1,
        )
        self.assertEqual(
            PersonAction.objects.filter(
                match_event_id=match_event.id,
                match_group_id=mg1.id,
                person_id=person1.id,
                person_record_id=pr2.id,
                type=PersonActionType.remove_record,
                performed_by=None,
            ).count(),
            1,
        )

        # Add actions

        self.assertEqual(
            PersonAction.objects.filter(
                match_event_id=match_event.id,
                match_group_id=mg1.id,
                person_id=person2.id,
                person_record_id=pr1.id,
                type=PersonActionType.add_record,
                performed_by=None,
            ).count(),
            1,
        )
        self.assertEqual(
            PersonAction.objects.filter(
                match_event_id=match_event.id,
                match_group_id=mg1.id,
                person_id=person2.id,
                person_record_id=pr2.id,
                type=PersonActionType.add_record,
                performed_by=None,
            ).count(),
            1,
        )

        # No additional actions

        self.assertEqual(PersonAction.objects.count(), 4)

    def test_process_job(self) -> None:
        #
        # Load staging records
        #

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": self.splink_settings,
                "potential_match_threshold": 0.001,
                "auto_match_threshold": 0.0013,
            }
        )
        job = empi.create_job("s3://tuva-health-example/test", config.id)

        common_person_record = {
            "created": timezone.now(),
            "job_id": job.id,
            "data_source": "example-ds-1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }
        PersonRecordStaging.objects.create(
            **common_person_record, source_person_id="a1"
        )
        PersonRecordStaging.objects.create(
            **common_person_record, source_person_id="a2"
        )
        PersonRecordStaging.objects.create(
            **common_person_record, source_person_id="a2"
        )

        self.assertEqual(PersonRecordStaging.objects.count(), 3)
        self.assertEqual(PersonRecord.objects.count(), 0)
        self.assertEqual(Person.objects.count(), 0)
        self.assertEqual(SplinkResult.objects.count(), 0)
        self.assertEqual(MatchGroup.objects.count(), 0)
        self.assertEqual(MatchGroupAction.objects.count(), 0)
        self.assertEqual(PersonAction.objects.count(), 0)

        #
        # Run process_job
        #

        with connection.cursor() as cursor:
            Matcher().process_job(cursor, job)

        #
        # Check that PersonRecords were loaded (and deduplicated)
        #

        self.assertEqual(PersonRecord.objects.count(), 2)

        loaded_records = PersonRecord.objects.order_by("source_person_id")

        self.assertEqual(loaded_records[0].source_person_id, "a1")
        self.assertEqual(loaded_records[1].source_person_id, "a2")
        self.assertEqual(loaded_records[0].first_name, "test-first-name")
        self.assertEqual(loaded_records[1].last_name, "test-last-name")
        self.assertTrue(loaded_records[0].sha256 is not None)
        self.assertTrue(loaded_records[1].sha256 is not None)

        #
        # Check that distinct Persons were created for each record
        #

        self.assertEqual(Person.objects.count(), 2)

        self.assertEqual(loaded_records[0].person, loaded_records[1].person)
        self.assertEqual(loaded_records[0].person.record_count, 2)

        #
        # Check that MatchEvents were created
        #

        self.assertEqual(MatchEvent.objects.count(), 2)
        match_events = list(MatchEvent.objects.order_by("id").all())
        self.assertEqual(match_events[0].job_id, job.id)
        self.assertEqual(match_events[0].type, MatchEventType.new_ids.value)
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < match_events[0].created
            < timezone.now()
        )
        self.assertEqual(match_events[1].job_id, job.id)
        self.assertEqual(match_events[1].type, MatchEventType.auto_matches.value)
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < match_events[0].created
            < timezone.now()
        )

        #
        # Check that Splink Results were created
        #

        self.assertEqual(SplinkResult.objects.count(), 1)
        result = cast(SplinkResult, SplinkResult.objects.first())
        self.assertEqual(result.job_id, job.id)
        self.assertTrue(result.match_probability > 0)
        self.assertTrue(result.data is not None)

        #
        # Check that MatchGroups were created
        #

        self.assertEqual(MatchGroup.objects.count(), 1)
        match_group = cast(MatchGroup, MatchGroup.objects.first())
        self.assertEqual(match_group.job_id, job.id)
        self.assertTrue(match_group.deleted is None)
        self.assertEqual(match_group.version, 1)

        #
        # Check that MatchGroupActions were created
        #

        self.assertEqual(MatchGroupAction.objects.count(), 2)
        match_group_actions = list(MatchGroupAction.objects.order_by("id").all())
        self.assertEqual(match_group_actions[0].match_event.job_id, job.id)
        self.assertEqual(
            match_group_actions[0].type, MatchGroupActionType.add_result.value
        )
        self.assertEqual(match_group_actions[1].match_event.job_id, job.id)
        self.assertEqual(match_group_actions[1].type, MatchGroupActionType.match.value)

        #
        # Check that PersonActions were created
        #

        self.assertEqual(PersonAction.objects.count(), 4)
        person_action = cast(PersonAction, PersonAction.objects.first())
        self.assertEqual(person_action.match_event.job_id, job.id)
        self.assertEqual(person_action.type, PersonActionType.add_record.value)

        #
        # Check that running it again does not cause duplicate records
        #

        with self.assertLogs(
            "main.services.matching.matcher", level=logging.INFO
        ) as log_capture:
            with connection.cursor() as cursor:
                Matcher().process_job(cursor, job)

        logs = "\n".join(log_capture.output)

        self.logger.info(logs)
        self.assertIn("No new staging records to load", logs)

        self.assertEqual(PersonRecord.objects.count(), 2)
        self.assertEqual(Person.objects.count(), 2)
        self.assertEqual(SplinkResult.objects.count(), 1)
        self.assertEqual(MatchGroup.objects.count(), 1)
        self.assertEqual(MatchGroupAction.objects.count(), 2)
        self.assertEqual(PersonAction.objects.count(), 4)

    def test_process_job_no_new_results(self) -> None:
        #
        # Load staging records
        #

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": self.splink_settings,
                # Increase the potential match threshold so that run_splink_prediction returns zero results
                "potential_match_threshold": 0.002,
                "auto_match_threshold": 0.0023,
            }
        )
        job = empi.create_job("s3://tuva-health-example/test", config.id)

        common_person_record = {
            "created": timezone.now(),
            "job_id": job.id,
            "data_source": "example-ds-1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }
        PersonRecordStaging.objects.create(
            **common_person_record, source_person_id="a1"
        )
        PersonRecordStaging.objects.create(
            **common_person_record, source_person_id="a2"
        )
        PersonRecordStaging.objects.create(
            **common_person_record, source_person_id="a2"
        )

        self.assertEqual(PersonRecordStaging.objects.count(), 3)
        self.assertEqual(PersonRecord.objects.count(), 0)
        self.assertEqual(Person.objects.count(), 0)
        self.assertEqual(SplinkResult.objects.count(), 0)
        self.assertEqual(MatchGroup.objects.count(), 0)
        self.assertEqual(MatchGroupAction.objects.count(), 0)
        self.assertEqual(PersonAction.objects.count(), 0)

        #
        # Run process_job
        #

        with self.assertLogs(
            "main.services.matching.matcher", level=logging.INFO
        ) as log_capture:
            with connection.cursor() as cursor:
                Matcher().process_job(cursor, job)

        logs = "\n".join(log_capture.output)

        self.logger.info(logs)
        self.assertIn("No new Splink prediction results", logs)

        #
        # Check that PersonRecords were loaded (and deduplicated)
        #

        self.assertEqual(PersonRecord.objects.count(), 2)

        loaded_records = PersonRecord.objects.order_by("source_person_id")

        self.assertEqual(loaded_records[0].source_person_id, "a1")
        self.assertEqual(loaded_records[1].source_person_id, "a2")
        self.assertEqual(loaded_records[0].first_name, "test-first-name")
        self.assertEqual(loaded_records[1].last_name, "test-last-name")
        self.assertTrue(loaded_records[0].sha256 is not None)
        self.assertTrue(loaded_records[1].sha256 is not None)

        #
        # Check that distinct Persons were created for each record
        #

        self.assertEqual(Person.objects.count(), 2)

        self.assertNotEqual(loaded_records[0].person, loaded_records[1].person)
        self.assertEqual(loaded_records[0].person.record_count, 1)

        #
        # Check that a MatchEvent was created
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        match_event = cast(MatchEvent, MatchEvent.objects.order_by("id").first())
        self.assertEqual(match_event.job_id, job.id)
        self.assertEqual(match_event.type, MatchEventType.new_ids.value)
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < match_event.created
            < timezone.now()
        )

        #
        # Check that SplinkResults, MatchGroups and MatchGroupActions were not created
        #

        self.assertEqual(SplinkResult.objects.count(), 0)
        self.assertEqual(MatchGroup.objects.count(), 0)
        self.assertEqual(MatchGroupAction.objects.count(), 0)

        #
        # Check that PersonActions were created
        #

        self.assertEqual(PersonAction.objects.count(), 2)
        person_action = cast(PersonAction, PersonAction.objects.first())
        self.assertEqual(person_action.match_event.job_id, job.id)
        self.assertEqual(person_action.type, PersonActionType.add_record.value)


class ProcessNextJobTestCase(TransactionTestCase):
    """Tests for Matcher.process_next_job method (and supporting methods)."""

    empi: EMPIService
    config: Config
    job1: Job
    job2: Job

    def setUp(self) -> None:
        self.maxDiff = None

        self.empi = EMPIService()
        self.config = self.empi.create_config(
            {
                "splink_settings": copy.deepcopy(test_splink_settings),
                # Increase the potential match threshold so that run_splink_prediction returns zero results
                "potential_match_threshold": 0.002,
                "auto_match_threshold": 0.0023,
            }
        )
        self.job1 = self.empi.create_job(
            "s3://tuva-health-example/test", self.config.id
        )
        self.job2 = self.empi.create_job(
            "s3://tuva-health-example/test", self.config.id
        )

        common_person_record = {
            "created": timezone.now(),
            "data_source": "example-ds-1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }
        PersonRecordStaging.objects.create(
            **common_person_record, job_id=self.job1.id, source_person_id="a1"
        )
        PersonRecordStaging.objects.create(
            **common_person_record, job_id=self.job1.id, source_person_id="a2"
        )
        PersonRecordStaging.objects.create(
            **common_person_record, job_id=self.job2.id, source_person_id="a3"
        )

        self.assertEqual(self.job1.status, JobStatus.new)
        self.assertEqual(self.job2.status, JobStatus.new)
        self.assertEqual(PersonRecordStaging.objects.count(), 3)

    def test_deleted_staging_records(self) -> None:
        """Method delete_staging_records should only delete PatientRecordStaging rows for the provided Job."""
        Matcher().delete_staging_records(self.job1)

        self.assertEqual(PersonRecordStaging.objects.count(), 1)
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job1.id).count(), 0
        )
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job2.id).count(), 1
        )

    @patch("main.services.matching.matcher.Matcher.process_job")
    def test_process_next_job_no_job(self, mock_process_job: MagicMock) -> None:
        """Method process_next_job should return early if there are no new jobs."""
        now = timezone.now()

        self.job1.status = JobStatus.succeeded
        self.job1.updated = now
        self.job1.save()

        self.job2.status = JobStatus.failed
        self.job2.updated = now
        self.job2.save()

        Matcher().process_next_job()

        mock_process_job.assert_not_called()

        self.job1.refresh_from_db()
        self.job2.refresh_from_db()

        # job1 should remain the same
        self.assertEqual(self.job1.status, JobStatus.succeeded)
        self.assertEqual(self.job1.updated, now)

        # job2 should remain the same
        self.assertEqual(self.job2.status, JobStatus.failed)
        self.assertEqual(self.job2.updated, now)

        # Staging records should remain untouched
        self.assertEqual(PersonRecordStaging.objects.count(), 3)

    @patch("main.services.matching.matcher.Matcher.process_job")
    def test_process_next_job_failure_exc(self, mock_process_job: MagicMock) -> None:
        """Method process_next_job should mark Job as failed if process_job throws an exception."""
        mock_process_job.side_effect = ValueError("Something unexpected happened")

        Matcher().process_next_job()

        self.job1.refresh_from_db()
        self.job2.refresh_from_db()

        # job1 status should be failed
        self.assertEqual(self.job1.status, JobStatus.failed)
        self.assertEqual(self.job1.reason, "Error: Something unexpected happened")
        # process_next_job should clear out staged records for the job1
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job1.id).count(), 0
        )

        # job2 status should remain new
        self.assertEqual(self.job2.status, JobStatus.new)
        self.assertIsNone(self.job2.reason)
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job2.id).count(), 1
        )

    @patch("main.services.matching.matcher.Matcher.process_job")
    def test_process_next_job_failure_database_exc(
        self, mock_process_job: MagicMock
    ) -> None:
        """Method process_next_job should mark Job as failed if process_job throws a DatabaseError exception."""
        mock_process_job.side_effect = DatabaseError("Database error occurred")

        Matcher().process_next_job()

        self.job1.refresh_from_db()
        self.job2.refresh_from_db()

        # job1 status should be failed
        self.assertEqual(self.job1.status, JobStatus.failed)
        self.assertEqual(self.job1.reason, "Error: Database error occurred")
        # process_next_job should clear out staged records for the job
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job1.id).count(), 0
        )

        # job2 status should remain new
        self.assertEqual(self.job2.status, JobStatus.new)
        self.assertIsNone(self.job2.reason)
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job2.id).count(), 1
        )

    def test_process_next_job_success(self) -> None:
        """Method process_next_job should mark Job as succeeded if process_job returns."""
        Matcher().process_next_job()

        self.job1.refresh_from_db()
        self.job2.refresh_from_db()

        # job1 status should be succeeded
        self.assertEqual(self.job1.status, JobStatus.succeeded)
        self.assertIsNone(self.job1.reason)
        # process_next_job should clear out staged records for the job
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job1.id).count(), 0
        )

        # job2 status should remain new
        self.assertEqual(self.job2.status, JobStatus.new)
        self.assertIsNone(self.job2.reason)
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job2.id).count(), 1
        )

        # PersonRecords should have been loaded
        self.assertEqual(PersonRecord.objects.count(), 2)

    @patch("main.services.matching.matcher.Matcher.run_splink_prediction")
    def test_process_next_job_failure_rollback(
        self, mock_run_splink_prediction: MagicMock
    ) -> None:
        """Method process_next_job should not commit some changes if an exception occurs in process_job."""
        # run_splink_prediction is called in process_job after PersonRecords are loaded
        mock_run_splink_prediction.side_effect = ValueError("Test error")

        Matcher().process_next_job()

        self.job1.refresh_from_db()

        # job1 status should be failed
        self.assertEqual(self.job1.status, JobStatus.failed)
        self.assertEqual(self.job1.reason, "Error: Test error")
        # process_next_job should clear out staged records for the job
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job1.id).count(), 0
        )

        # No PersonRecords should have been loaded
        self.assertEqual(PersonRecord.objects.count(), 0)

    def test_cleanup_failed_job_ok(self) -> None:
        """Method cleanup_failed_fob should mark Job as failed if Job exists and status is still new."""
        Matcher().cleanup_failed_job(self.job1, ValueError("Test error"))

        self.job1.refresh_from_db()
        self.job2.refresh_from_db()

        # job1 status should be failed
        self.assertEqual(self.job1.status, JobStatus.failed)
        self.assertEqual(self.job1.reason, "Error: Test error")
        # cleanup_failed_job should clear out staged records for the job
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job1.id).count(), 0
        )

        # job2 status should remain new
        self.assertEqual(self.job2.status, JobStatus.new)
        self.assertIsNone(self.job2.reason)
        self.assertEqual(
            PersonRecordStaging.objects.filter(job_id=self.job2.id).count(), 1
        )

    def test_cleanup_failed_job_no_overwrite(self) -> None:
        """Method cleanup_failed_job should not update Job status if status isn't new."""
        self.job1.status = JobStatus.succeeded
        self.job1.save()

        with self.assertRaisesMessage(
            Exception,
            "Failed to update Job failure status and cleanup staging records."
            f" Job {self.job1.id} status is {self.job1.status}, expected {JobStatus.new.value}.",
        ) as cm:
            Matcher().cleanup_failed_job(self.job1, ValueError("Test error"))

        self.assertIsInstance(cm.exception.__cause__, ValueError)

        self.job1.refresh_from_db()
        self.assertEqual(self.job1.status, JobStatus.succeeded)
        self.assertEqual(PersonRecordStaging.objects.count(), 3)

    def test_cleanup_failed_job_no_job(self) -> None:
        """Method cleanup_failed_job should not update Job status if the job doesn't exist."""
        with self.assertRaisesMessage(
            Exception,
            "No current Job, skipping Job failure status update and staging records cleanup",
        ) as cm:
            Matcher().cleanup_failed_job(None, ValueError("Test error"))

        self.assertIsInstance(cm.exception.__cause__, ValueError)

    def test_cleanup_failed_job_no_job_in_db(self) -> None:
        """Method cleanup_failed_job should not update Job status if the job doesn't exist in DB."""
        job3 = self.empi.create_job("s3://tuva-health-example/test", self.config.id)

        Job.objects.filter(id=job3.id).delete()

        with self.assertRaisesMessage(
            Exception,
            "Failed to update Job failure status and cleanup staging records."
            f" Job {job3.id} does not exist.",
        ) as cm:
            Matcher().cleanup_failed_job(job3, ValueError("Test error"))

        self.assertIsInstance(cm.exception.__cause__, ValueError)


class MatcherWithLockingTestCase(TransactionTestCase):
    def __init__(self, method_name: str) -> None:
        self.db_alias = "default_copy"

        if self.db_alias not in settings.DATABASES:
            settings.DATABASES[self.db_alias] = copy.deepcopy(
                settings.DATABASES["default"]
            )

        MatcherWithLockingTestCase.databases = {"default", "default_copy"}

        super().__init__(method_name)

    def setUp(self) -> None:
        self.maxDiff = None

    def test_extract_current_results_with_lock(self) -> None:
        #
        # Load PersonRecords, Persons, Results and MatchGroups
        #

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.0,
                "auto_match_threshold": 1.0,
            }
        )
        job1 = empi.create_job("s3://tuva-health-example/test", config.id)
        job2 = empi.create_job("s3://tuva-health-example/test", config.id)

        person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job1,
            version=1,
            record_count=1,
        )

        common_person_record = {
            "created": timezone.now(),
            "person_id": person1.id,
            "person_updated": timezone.now(),
            "matched_or_reviewed": None,
            "data_source": "example-ds-1",
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }

        pr1 = PersonRecord.objects.create(
            **common_person_record,
            id=0,
            job_id=job1.id,
            sha256=b"test-sha256-1",
        )
        pr2 = PersonRecord.objects.create(
            **common_person_record,
            id=1,
            job_id=job1.id,
            sha256=b"test-sha256-2",
        )

        # Active and unmatched from job1
        mg1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            deleted=None,
            job_id=job1.id,
            matched=None,
        )
        # Deleted and unmatched from job1
        mg2 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            deleted=timezone.now(),
            job_id=job1.id,
            matched=None,
        )
        # Active and matched from job1
        mg3 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            deleted=None,
            job_id=job1.id,
            matched=timezone.now(),
        )
        # Active and unmatched from job2
        mg4 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            deleted=None,
            job_id=job2.id,
            matched=None,
        )

        splink_result_table = SplinkResult._meta.db_table
        now_str = timezone.now().isoformat()
        common_result = {
            "id": 0,
            "created": now_str,
            "job_id": job1.id,
            "match_group_id": mg1.id,
            "match_group_updated": now_str,
            "match_weight": 0.01,
            "match_probability": 0.02,
            "person_record_l_id": pr1.id,
            "person_record_r_id": pr2.id,
            "data": "{}",
        }

        new_result_df = pd.DataFrame(
            [
                # Results for mg1
                {**common_result, "id": 0, "job_id": job1.id, "match_group_id": mg1.id},
                {**common_result, "id": 1, "job_id": job1.id, "match_group_id": mg1.id},
                # Results for mg2
                {**common_result, "id": 2, "job_id": job1.id, "match_group_id": mg2.id},
                # Results for mg3
                {**common_result, "id": 3, "job_id": job1.id, "match_group_id": mg3.id},
                # Results for job2 and mg4
                {**common_result, "id": 4, "job_id": job2.id, "match_group_id": mg4.id},
            ]
        )

        with connection.cursor() as cursor:
            load_df(
                cursor,
                splink_result_table,
                new_result_df,
                list(common_result.keys()),
            )

        with transaction.atomic(durable=True):
            #
            # Extract results
            #

            with connection.cursor() as cursor:
                current_result_df = Matcher().extract_current_results_with_lock(
                    cursor, job2
                )

            #
            # Check returned results are as expected
            #

            expected_current_result_df = new_result_df.query("id == 0 or id == 1").drop(
                columns=["created", "match_group_id", "match_group_updated"]
            )
            expected_current_result_df["data"] = pd.NA

            self.assertEqual(len(current_result_df), 2)
            pdt.assert_frame_equal(expected_current_result_df, current_result_df)

            #
            # Check that 'for update' was used in extraction query to lock SplinkResults and MatchGroups
            #

            # TODO: We can also check that locks are not held for other rows

            result_ids = current_result_df["id"].tolist()

            self.assertEqual(len(result_ids), 2)

            with transaction.atomic(using=self.db_alias, durable=True):
                with self.assertRaises(OperationalError) as err_ctx:
                    list(
                        SplinkResult.objects.using(self.db_alias)
                        .select_for_update(nowait=True)
                        .filter(id__in=result_ids)
                        .all()
                    )

                self.assertTrue(
                    isinstance(
                        err_ctx.exception.__cause__, psycopg.errors.LockNotAvailable
                    )
                )

            with transaction.atomic(using=self.db_alias, durable=True):
                with self.assertRaises(OperationalError) as err_ctx:
                    list(
                        MatchGroup.objects.using(self.db_alias)
                        .select_for_update(nowait=True)
                        .filter(id=mg1.id)
                        .all()
                    )

                self.assertTrue(
                    isinstance(
                        err_ctx.exception.__cause__, psycopg.errors.LockNotAvailable
                    )
                )

        #
        # Check that Match Groups have been soft-deleted
        #

        mg1.refresh_from_db()
        mg2.refresh_from_db()
        mg3.refresh_from_db()
        mg4.refresh_from_db()

        self.assertTrue(mg1.deleted is not None)
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < cast(datetime, mg1.deleted)
            < timezone.now()
        )
        self.assertTrue(mg2.deleted is not None)
        self.assertTrue(
            (timezone.now() - timedelta(minutes=1))
            < cast(datetime, mg2.deleted)
            < timezone.now()
        )
        self.assertTrue(mg3.deleted is None)
        self.assertTrue(mg4.deleted is None)

    def test_extract_person_crosswalk_with_lock(self) -> None:
        #
        # Load PersonRecords, Persons
        #

        empi = EMPIService()
        config = empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.0,
                "auto_match_threshold": 1.0,
            }
        )
        job = empi.create_job("s3://tuva-health-example/test", config.id)

        person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )
        person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )
        person3 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )
        person4 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=timezone.now(),
            updated=timezone.now(),
            job=job,
            version=1,
            record_count=1,
        )

        common_person_record = {
            "created": timezone.now(),
            "job_id": job.id,
            "person_updated": timezone.now(),
            "matched_or_reviewed": None,
            "data_source": "example-ds-1",
            "source_person_id": "a1",
            "first_name": "test-first-name",
            "last_name": "test-last-name",
            "sex": "F",
            "race": "test-race",
            "birth_date": "1900-01-01",
            "death_date": "3000-01-01",
            "social_security_number": "000-00-0000",
            "address": "1 Test Address",
            "city": "Test City",
            "state": "AA",
            "zip_code": "00000",
            "county": "Test County",
            "phone": "0000000",
        }

        new_person_records = [
            {
                **common_person_record,
                "id": 0,
                "person_id": person1.id,
                "sha256": b"test-sha256-1",
            },
            {
                **common_person_record,
                "id": 1,
                "person_id": person2.id,
                "sha256": b"test-sha256-2",
            },
            {
                **common_person_record,
                "id": 2,
                "person_id": person3.id,
                "sha256": b"test-sha256-3",
            },
            {
                **common_person_record,
                "id": 3,
                "person_id": person4.id,
                "sha256": b"test-sha256-4",
            },
        ]

        pr1 = PersonRecord.objects.create(**new_person_records[0])
        pr2 = PersonRecord.objects.create(**new_person_records[1])
        pr3 = PersonRecord.objects.create(**new_person_records[2])
        _ = PersonRecord.objects.create(**new_person_records[3])

        potential_match_result_df = pd.DataFrame(
            {
                "person_record_l_id": [pr1.id, pr3.id],
                "person_record_r_id": [pr2.id, pr1.id],
            }
        )

        self.assertEqual(Person.objects.count(), 4)
        self.assertEqual(PersonRecord.objects.count(), 4)

        with transaction.atomic(durable=True):
            #
            # Extract person_crosswalk
            #

            with connection.cursor() as cursor:
                person_crosswalk_df = Matcher().extract_person_crosswalk_with_lock(
                    cursor, job, potential_match_result_df
                )

            #
            # Check crosswalk is as expected
            #

            expected_person_crosswalk_df = pd.DataFrame(
                [
                    {
                        "id": person1.id,
                        "created": person1.created.isoformat(),
                        "version": 1,
                        "record_count": 1,
                        "person_record_id": pr1.id,
                    },
                    {
                        "id": person2.id,
                        "created": person2.created.isoformat(),
                        "version": 1,
                        "record_count": 1,
                        "person_record_id": pr2.id,
                    },
                    {
                        "id": person3.id,
                        "created": person3.created.isoformat(),
                        "version": 1,
                        "record_count": 1,
                        "person_record_id": pr3.id,
                    },
                ]
            ).astype(
                {
                    "id": "int64",
                    "created": "string",
                    "version": "int64",
                    "record_count": "int64",
                    "person_record_id": "int64",
                }
            )

            self.assertEqual(len(person_crosswalk_df), 3)
            pdt.assert_frame_equal(expected_person_crosswalk_df, person_crosswalk_df)

            #
            # Check that 'for update' was used in extraction query to lock Persons and PersonRecords
            #

            # TODO: We can also check that locks are not held for other rows

            person_ids = person_crosswalk_df["id"].tolist()

            self.assertEqual(len(person_ids), 3)

            with transaction.atomic(using=self.db_alias, durable=True):
                with self.assertRaises(OperationalError) as err_ctx:
                    list(
                        Person.objects.using(self.db_alias)
                        .select_for_update(nowait=True)
                        .filter(id__in=person_ids)
                        .all()
                    )

                self.assertTrue(
                    isinstance(
                        err_ctx.exception.__cause__, psycopg.errors.LockNotAvailable
                    )
                )

            person_record_ids = person_crosswalk_df["person_record_id"].tolist()

            self.assertEqual(len(person_ids), 3)

            with transaction.atomic(using=self.db_alias, durable=True):
                with self.assertRaises(OperationalError) as err_ctx:
                    list(
                        PersonRecord.objects.using(self.db_alias)
                        .select_for_update(nowait=True)
                        .filter(id__in=person_record_ids)
                        .all()
                    )

                self.assertTrue(
                    isinstance(
                        err_ctx.exception.__cause__, psycopg.errors.LockNotAvailable
                    )
                )


class MatcherConcurrencyTestCase(TransactionTestCase):
    now: datetime
    config: Config
    job1: Job
    job2: Job

    def setUp(self) -> None:
        self.now = timezone.now()
        self.config = EMPIService().create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.001,
                "auto_match_threshold": 0.0013,
            }
        )
        self.job1 = EMPIService().create_job(
            "s3://tuva-health-example/test", self.config.id
        )
        self.job2 = EMPIService().create_job(
            "s3://tuva-health-example/test", self.config.id
        )

    @patch("main.services.matching.matcher.logging.getLogger")
    def test_process_next_job_advisory_lock(self, mock_get_logger: MagicMock) -> None:
        """Tests that only a single instance of process_next_job runs at a time."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Run process_next_job and close DB connection
        def process_next_job() -> None:
            try:
                Matcher().process_next_job()
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            patch1="main.services.matching.matcher.Matcher.load_person_records",
            # Return zero from load_person_records
            patch1_return=0,
            function1=process_next_job,
            patch2="main.services.matching.matcher.Matcher.load_person_records",
            # Return zero from load_person_records
            patch2_return=0,
            function2=process_next_job,
            post_contention_delay=3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNotNone(t2_entry)

        # Matcher 2 should only have run after Matcher 1 released the lock
        self.assertGreater(cast(float, t2_entry) - cast(float, t1_exit), 3)

        # Both jobs should have succeeded
        self.job1.refresh_from_db()
        self.assertEqual(self.job1.status, JobStatus.succeeded)
        self.assertIsNone(self.job1.reason)

        self.job2.refresh_from_db()
        self.assertEqual(self.job2.status, JobStatus.succeeded)
        self.assertIsNone(self.job2.reason)

    def test_cleanup_failed_job_row_lock(self) -> None:
        """Tests that only a single instance of cleanup_failed_job runs for a specific Job at a time."""

        # Run cleanup_failed_job and close DB connection
        def cleanup_failed_job_1() -> None:
            try:
                Matcher().cleanup_failed_job(self.job1, Exception("Test error"))
            finally:
                connection.close()

        # Run cleanup_failed_job and close DB connection
        def cleanup_failed_job_2() -> None:
            try:
                # Matcher 2 should fail to update the first job
                with self.assertRaisesMessage(
                    Exception,
                    "Failed to update Job failure status and cleanup staging records."
                    f" Job {self.job1.id} status is {JobStatus.failed.value}, expected {JobStatus.new.value}.",
                ):
                    Matcher().cleanup_failed_job(self.job1, Exception("Test error"))
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            patch1="main.services.matching.matcher.Matcher.delete_staging_records",
            patch1_return=None,
            function1=cleanup_failed_job_1,
            patch2="main.services.matching.matcher.Matcher.delete_staging_records",
            patch2_return=None,
            function2=cleanup_failed_job_2,
            post_contention_delay=3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNone(t2_entry)

        # Matcher 1 should have marked the first job as failed
        self.job1.refresh_from_db()
        self.assertEqual(self.job1.status, JobStatus.failed)
        self.assertEqual(self.job1.reason, "Error: Test error")
        self.assertLess(
            self.job1.updated, datetime.fromtimestamp(cast(float, t1_exit), tz=tz.utc)
        )

        self.job2.refresh_from_db()
        self.assertEqual(self.job2.status, JobStatus.new)
        self.assertIsNone(self.job2.reason)
