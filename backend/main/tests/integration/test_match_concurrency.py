import uuid
from datetime import datetime
from typing import cast

import pandas as pd
from django.db import connection
from django.test import TransactionTestCase
from django.utils import timezone

from main.models import (
    Config,
    Job,
    JobStatus,
    MatchEvent,
    MatchEventType,
    MatchGroup,
    Person,
    PersonRecord,
    PersonRecordStaging,
    SplinkResult,
    User,
)
from main.services.empi.empi_service import (
    ConcurrentMatchUpdates,
    EMPIService,
)
from main.services.matching.matcher import Matcher
from main.tests.testing.concurrency import run_with_lock_contention


class MatchConcurrencyTestCase(TransactionTestCase):
    """Tests concurrency properties between EMPIService.match_person_records and Matcher.process_next_job."""

    now: datetime = timezone.now()
    config: Config
    job1: Job
    job2: Job
    person1: Person
    person2: Person
    person_record1: PersonRecord
    person_record2: PersonRecord
    match_group1: MatchGroup
    result1: SplinkResult
    user: User

    def setUp(self) -> None:
        self.now = timezone.now()
        splink_settings = {
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
                            "sql_condition": '"first_name_l" IS NULL OR "first_name_r" IS NULL',
                            "label_for_charts": "first_name is NULL",
                            "is_null_level": True,
                        },
                        {
                            "sql_condition": '"first_name_l" = "first_name_r"',
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
        self.config = EMPIService().create_config(
            {
                "splink_settings": splink_settings,
                "potential_match_threshold": 0.001,
                "auto_match_threshold": 0.0013,
            }
        )

        #
        # Setup for EMPIService
        #

        self.job1 = EMPIService().create_job(
            "s3://tuva-health-example/test", self.config.id
        )
        self.job1.status = JobStatus.succeeded
        self.job1.save()

        common_person_record = {
            "created": self.now,
            "job_id": self.job1.id,
            "person_updated": self.now,
            "matched_or_reviewed": None,
            "source_person_id": "a1",
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

        self.person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job1.id,
            version=1,
            record_count=1,
        )
        self.person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job1.id,
            version=1,
            record_count=1,
        )

        self.person_record1 = PersonRecord.objects.create(
            **common_person_record,
            person_id=self.person1.id,
            sha256=b"test-sha256-1",
            data_source="ds1",
            first_name="John",
            last_name="Doe",
        )
        self.person_record2 = PersonRecord.objects.create(
            **common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-2",
            data_source="ds2",
            first_name="Jane",
            last_name="Smith",
        )

        self.match_group1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job1.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result1 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job1.id,
            match_group_id=self.match_group1.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=self.person_record1.id,
            person_record_r_id=self.person_record2.id,
            data={},
        )
        self.user = User.objects.create()

        #
        # Setup for Matcher
        #

        self.job2 = EMPIService().create_job(
            "s3://tuva-health-example/test", self.config.id
        )

        common_person_record_stg = {
            "created": timezone.now(),
            "job_id": self.job2.id,
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
        PersonRecordStaging.objects.create(
            **{**common_person_record_stg, "source_person_id": "a1"}
        )
        PersonRecordStaging.objects.create(
            **{**common_person_record_stg, "source_person_id": "a2"}
        )
        PersonRecordStaging.objects.create(
            **{**common_person_record_stg, "source_person_id": "a2"}
        )

    def test_job_waits_on_match_person_records(self) -> None:
        """Tests that if EMPIService.match_person_records holds the match advisory lock, then Matcher.process_next_job waits."""

        # Run EMPIService.match_person_records and close DB connection
        def match_person_records() -> None:
            try:
                EMPIService().match_person_records(
                    self.match_group1.id, self.match_group1.version, [], self.user
                )
            finally:
                connection.close()

        # Run Matcher.process_next_job and close DB connection
        def process_next_job() -> None:
            try:
                Matcher().process_next_job()
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            # EMPIService.validate_update_records is called by EMPIService.match_person_records after
            # the lock is obtained.
            "main.services.empi.empi_service.EMPIService.validate_update_records",
            None,
            match_person_records,
            # Matcher.extract_current_results_with_lock is called by Matcher.process_next_job after the
            # lock is obtained.
            "main.services.matching.matcher.Matcher.extract_current_results_with_lock",
            # Valid return value for extract_current_results_with_lock so that Matcher.process_next_job
            # succeeds
            pd.DataFrame(
                columns=[
                    "id",
                    "job_id",
                    "match_weight",
                    "match_probability",
                    "person_record_l_id",
                    "person_record_r_id",
                ]
            ).astype(
                {
                    "id": "int64",
                    "job_id": "int64",
                    "match_weight": "float64",
                    "match_probability": "float64",
                    "person_record_l_id": "int64",
                    "person_record_r_id": "int64",
                }
            ),
            process_next_job,
            3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNotNone(t2_entry)

        # Matcher should only have run after EMPIService released the lock
        self.assertGreater(cast(float, t2_entry) - cast(float, t1_exit), 3)

        # EMPIService.match_person_records should have succeeded
        self.assertEqual(
            MatchEvent.objects.filter(type=MatchEventType.manual_match).count(), 1
        )
        # Matcher.process_next_job should have succeeded
        self.assertEqual(
            MatchEvent.objects.filter(type=MatchEventType.auto_matches).count(), 1
        )

    def test_match_person_records_fails_when_job_runs(self) -> None:
        """Tests that if Matcher.process_next_job holds the match advisory lock, then EMPIService.match_person_records throws.

        This is the same test as above, but reversed. match_person_records doesn't wait for Matcher
        to release the lock because it might wait for a long time in the request/response cycle.
        """

        # Run Matcher.process_next_job and close DB connection
        def process_next_job() -> None:
            try:
                Matcher().process_next_job()
            finally:
                connection.close()

        # Run EMPIService.match_person_records and close DB connection
        def match_person_records() -> None:
            try:
                with self.assertRaises(ConcurrentMatchUpdates):
                    EMPIService().match_person_records(
                        self.match_group1.id, self.match_group1.version, [], self.user
                    )
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            # Matcher.extract_current_results_with_lock is called by Matcher.process_next_job after the
            # lock is obtained.
            patch1="main.services.matching.matcher.Matcher.extract_current_results_with_lock",
            # Return empty dataframe from extract_current_results_with_lock so that Matcher.process_next_job
            # succeeds
            patch1_return=pd.DataFrame(
                columns=[
                    "id",
                    "job_id",
                    "match_weight",
                    "match_probability",
                    "person_record_l_id",
                    "person_record_r_id",
                ]
            ).astype(
                {
                    "id": "int64",
                    "job_id": "int64",
                    "match_weight": "float64",
                    "match_probability": "float64",
                    "person_record_l_id": "int64",
                    "person_record_r_id": "int64",
                }
            ),
            function1=process_next_job,
            # EMPIService.validate_update_records is called by EMPIService.match_person_records after
            # the lock is obtained.
            patch2="main.services.empi.empi_service.EMPIService.validate_update_records",
            patch2_return=True,
            function2=match_person_records,
            post_contention_delay=3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNone(t2_entry)

        # Matcher.process_next_job should have succeeded
        self.assertEqual(
            MatchEvent.objects.filter(type=MatchEventType.auto_matches).count(), 1
        )
        # EMPIService.match_person_records should have failed
        self.assertEqual(
            MatchEvent.objects.filter(type=MatchEventType.manual_match).count(), 0
        )
