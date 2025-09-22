import io
import os
import threading
import uuid
import logging
import psycopg

from contextlib import contextmanager
from datetime import datetime, timedelta
from datetime import timezone as tz
from typing import IO, Any, Iterator, Mapping, Optional, cast
from unittest.mock import Mock, MagicMock, patch

from django.db import connection
from django.db.backends.utils import CursorWrapper
from django.test import TestCase, TransactionTestCase
from django.utils import timezone as django_tz


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
    User,
)
from main.services.empi.empi_service import (
    DataSourceDict,
    EMPIService,
    InvalidPersonRecordFileFormat,
    InvalidPersonUpdate,
    InvalidPotentialMatch,
    PersonDict,
    PersonRecordDict,
    PersonSummaryDict,
    PersonUpdateDict,
    PotentialMatchDict,
    PotentialMatchSummaryDict,
    PredictionResultDict,
)
from main.tests.testing.concurrency import run_with_lock_contention
from main.util.dict import select_keys


def get_path(path: str) -> str:
    return os.path.join(os.path.dirname(__file__), path)


@contextmanager
def mock_open(path: str) -> Iterator[IO[bytes]]:
    with open(get_path(path), "rb") as f:
        yield io.BytesIO(f.read())


class ImportPersonRecordsTestCase(TestCase):
    config: Config

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with connection.cursor() as cursor:
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS unaccent;")
            except Exception as e:
                pass  # Skip if no permissions

    @classmethod
    def setUpTestData(cls) -> None:
        cls.config = Config.objects.create(
            **{
                "splink_settings": {"test": 1},
                "potential_match_threshold": 1.0,
                "auto_match_threshold": 1.1,
            }
        )

    @patch("main.services.empi.empi_service.open_source")
    def test_import(self, mock_open_source: MagicMock) -> None:
        mock_open_source.side_effect = lambda _: mock_open(
            "../../resources/raw-person-records.csv"
        )

        empi = EMPIService()
        s3_uri = "s3://tuva-health-example/test"

        empi.import_person_records(s3_uri, self.config.id)

        records = PersonRecordStaging.objects.all()

        self.assertEqual(
            records.count(),
            6,
            "Number of records imported should match number of records received",
        )

        self.assertEqual(
            records.filter(source_person_id="a4")[0].first_name,
            None,  # Empty strings become NULL
            "Empty strings should be normalized to NULL",
        )

        self.assertEqual(
            records.filter(source_person_id="a5")[0].first_name,
            None,  # This should still be None
            "NULL values should remain NULL",
        )
        self.assertEqual(
            records.filter(source_person_id="a6")[0].first_name,
            None,  # "1" becomes "" becomes NULL
            "Numeric values should be stripped and result in NULL",
        )

        rec = records[0]

        self.assertTrue(
            isinstance(rec.id, int)
            and isinstance(rec.job_id, int)
            and isinstance(rec.created, datetime)
            and rec.created.tzinfo == tz.utc
            and rec.created < django_tz.now(),
            "id, job_id and created fields should get added to each record",
        )

        self.assertTrue(isinstance(rec.job.id, int), "New Job is created")

        self.assertTrue(
            isinstance(rec.job.created, datetime)
            and rec.job.created.tzinfo == tz.utc
            and rec.job.created < django_tz.now(),
            "Job created field should get added",
        )

        self.assertTrue(
            isinstance(rec.job.updated, datetime)
            and rec.job.updated.tzinfo == tz.utc
            and rec.job.updated < django_tz.now(),
            "Job updated field should get added",
        )

        self.assertEqual(
            rec.job.source_uri,
            s3_uri,
            "Job source_uri field should equal provided s3_uri",
        )

        self.assertEqual(
            rec.job.status, JobStatus.new, "Job status field should equal 'new'"
        )

        self.assertEqual(
            rec.job.config_id, self.config.id, "Job.config matches provided config"
        )

    @patch("main.services.empi.empi_service.open_source")
    def test_import_invalid_file_format(self, mock_open_source: MagicMock) -> None:
        mock_open_source.side_effect = lambda _: mock_open(
            "../../resources/raw-person-records-missing-phone-col.csv"
        )

        s3_uri = "s3://tuva-health-example/test"

        # Missing phone column only in header
        empi = EMPIService()
        with self.assertRaises(InvalidPersonRecordFileFormat) as cm:
            empi.import_person_records(s3_uri, self.config.id)
        self.assertIn(
            "Incorrectly formatted person records file due to invalid header.",
            str(cm.exception),
        )

        mock_open_source.side_effect = lambda _: mock_open(
            "../../resources/raw-person-records-missing-phone-val.csv"
        )

        # Missing phone column only in first row
        empi = EMPIService()
        with self.assertRaises(InvalidPersonRecordFileFormat) as cm:
            empi.import_person_records(s3_uri, self.config.id)
        self.assertIn(
            'Database error during CSV load: missing data for column "phone"',
            str(cm.exception),
        )

        mock_open_source.side_effect = lambda _: mock_open(
            "../../resources/raw-person-records-extra-col.csv"
        )

        # Extra column in header and first row
        empi = EMPIService()
        with self.assertRaises(InvalidPersonRecordFileFormat) as cm:
            empi.import_person_records(s3_uri, self.config.id)
        self.assertIn(
            "Incorrectly formatted person records file due to invalid header.",
            str(cm.exception),
        )


class GetDataSourcesTestCase(TestCase):
    empi: EMPIService

    def setUp(self) -> None:
        self.empi = EMPIService()

        now = django_tz.now()
        config = self.empi.create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.5,
                "auto_match_threshold": 1.0,
            }
        )
        job = self.empi.create_job("s3://tuva-health-example/test", config.id)
        person = Person.objects.create(
            uuid=uuid.uuid4(),
            created=now,
            updated=now,
            job=job,
            version=1,
            record_count=1,
        )

        common_person_record = {
            "created": now,
            "job_id": job.id,
            "person_id": person.id,
            "person_updated": now,
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

        PersonRecord.objects.create(
            **common_person_record, sha256=b"test-sha256-1", data_source="ds1"
        )
        PersonRecord.objects.create(
            **common_person_record, sha256=b"test-sha256-2", data_source="ds2"
        )
        PersonRecord.objects.create(
            **common_person_record, sha256=b"test-sha256-3", data_source="ds1"
        )

    def test_get_data_sources(self) -> None:
        """Tests that get_data_sources correctly retrieves unique data sources."""
        expected_data_sources = [
            DataSourceDict(name="ds1"),
            DataSourceDict(name="ds2"),
        ]

        data_sources = self.empi.get_data_sources()

        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources, expected_data_sources)

    def test_get_data_sources_empty(self) -> None:
        """Tests get_data_sources when no records exist."""
        PersonRecord.objects.all().delete()

        data_sources = self.empi.get_data_sources()

        self.assertEqual(len(data_sources), 0)
        self.assertEqual(data_sources, [])


class PotentialMatchesTestCase(TransactionTestCase):
    empi: EMPIService
    now: datetime
    common_person_record: Mapping[str, Any]
    config: Config
    job: Job
    person1: Person
    person2: Person
    person3: Person
    person4: Person
    person5: Person
    person6: Person
    match_group1: MatchGroup
    match_group2: MatchGroup
    result1: SplinkResult
    result2: SplinkResult
    result3: SplinkResult

    def setUp(self) -> None:
        self.maxDiff = None
        self.empi = EMPIService()
        self.now = django_tz.now()

        self.config = Config.objects.create(
            **{
                "created": self.now,
                "splink_settings": {"test": 1},
                "potential_match_threshold": 1.0,
                "auto_match_threshold": 1.1,
            }
        )
        self.job = Job.objects.create(
            source_uri="s3://example/test",
            config_id=self.config.id,
            status="new",
        )
        self.common_person_record = {
            "created": self.now,
            "job_id": self.job.id,
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
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person3 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person4 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person5 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person6 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        person_record1 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person1.id,
            sha256=b"test-sha256-1",
            data_source="ds1",
            first_name="John",
            last_name="Doe",
        )
        person_record2 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-2",
            data_source="ds2",
            first_name="Jane",
            last_name="Smith",
        )
        person_record3 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person3.id,
            sha256=b"test-sha256-3",
            data_source="ds3",
            first_name="Paul",
            last_name="Lap",
        )
        person_record4 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person4.id,
            sha256=b"test-sha256-4",
            data_source="ds4",
            first_name="Linda",
            last_name="Love",
        )
        person_record5 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person5.id,
            sha256=b"test-sha256-5",
            data_source="ds5",
            first_name="Tina",
            last_name="Smith",
        )
        person_record6 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person6.id,
            sha256=b"test-sha256-6",
            data_source="ds6",
            first_name="Tom",
            last_name="Rom",
        )

        self.match_group1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result1 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group1.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=person_record1.id,
            person_record_r_id=person_record2.id,
            data={},
        )
        self.result2 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group1.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=person_record3.id,
            person_record_r_id=person_record4.id,
            data={},
        )

        self.match_group2 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result3 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group2.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=person_record5.id,
            person_record_r_id=person_record6.id,
            data={},
        )

    #
    # get_potential_matches
    #

    def test_get_all_potential_matches(self) -> None:
        """Tests returns all matches by default."""
        matches = self.empi.get_potential_matches(
            first_name="",
            last_name="",
            birth_date="",
            person_id="",
            source_person_id="",
            data_source="",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches()
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_by_first_name(self) -> None:
        """Tests searching by first name (case-insensitive)."""
        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches(
            first_name="ohn",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_by_last_name(self) -> None:
        """Tests searching by last name (case-insensitive)."""
        matches = self.empi.get_potential_matches(
            last_name="smith",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches(
            last_name="smi",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches(
            last_name="rom",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_by_birth_date(self) -> None:
        """Tests searching by birth date."""
        matches = self.empi.get_potential_matches(
            birth_date="1900-01-01",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches(
            birth_date="1900",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_by_person_id(self) -> None:
        """Tests searching by person ID."""
        matches = self.empi.get_potential_matches(
            person_id=str(self.person1.uuid),
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches(
            person_id=str(self.person5.uuid)[:20],
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_by_source_person_id(self) -> None:
        """Tests searching by source person ID."""
        matches = self.empi.get_potential_matches(
            source_person_id="a2",
        )
        self.assertEqual(matches, [])

        matches = self.empi.get_potential_matches(
            source_person_id="a1",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            ),
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_by_data_source(self) -> None:
        """Tests searching by data source."""
        matches = self.empi.get_potential_matches(
            data_source="ds1",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_potential_matches(
            data_source="ds5",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group2.id,
                first_name="Tina",
                last_name="Smith",
                data_sources=["ds5", "ds6"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_potential_matches_no_results(self) -> None:
        """Tests empty results with no matches."""
        matches = self.empi.get_potential_matches(
            first_name="NONEXISTENT",
        )
        self.assertEqual(matches, [])

    def test_get_potential_matches_max_match_probability(self) -> None:
        """Tests max_match_probability calculation uses highest match probability."""
        # Create a new match group with different match probabilities
        match_group = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )

        # Create two person records to link
        person_a = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        person_b = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        person_record_a = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=person_a.id,
            sha256=b"test-sha256-a",
            data_source="ds_test_a",
            first_name="TestA",
            last_name="MatchTest",
        )

        person_record_b = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=person_b.id,
            sha256=b"test-sha256-b",
            data_source="ds_test_b",
            first_name="TestB",
            last_name="MatchTest",
        )

        # Create two results with different match probabilities (0.75 and 0.85)
        SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=match_group.id,
            match_group_updated=self.now,
            match_probability=0.75,
            match_weight=0.75,
            person_record_l_id=person_record_a.id,
            person_record_r_id=person_record_b.id,
            data={},
        )

        SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=match_group.id,
            match_group_updated=self.now,
            match_probability=0.85,
            match_weight=0.85,
            person_record_l_id=person_record_a.id,
            person_record_r_id=person_record_b.id,
            data={},
        )

        # Query for the match group we just created
        matches = self.empi.get_potential_matches(
            first_name="Test",
        )

        # Verify we get the match group and the max_match_probability is 0.85 (the highest value)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["max_match_probability"], 0.85)

    def test_get_potential_matches_max_match_probability_single_low_result(
        self,
    ) -> None:
        """Tests max_match_probability is equal to the probability when there's a single low-probability result."""
        # Create a new match group with a single low-probability prediction result
        match_group = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )

        # Create person records to link
        person_a = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        person_b = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        person_record_a = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=person_a.id,
            sha256=b"test-sha256-a",
            data_source="ds_low_a",
            first_name="LowProb",
            last_name="Test",
        )

        person_record_b = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=person_b.id,
            sha256=b"test-sha256-b",
            data_source="ds_low_b",
            first_name="LowProb",
            last_name="Test",
        )

        # Create a single SplinkResult with a low probability (0.55)
        SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=match_group.id,
            match_group_updated=self.now,
            match_probability=0.55,
            match_weight=0.55,
            person_record_l_id=person_record_a.id,
            person_record_r_id=person_record_b.id,
            data={},
        )

        # Query for the match group we just created
        matches = self.empi.get_potential_matches(
            first_name="LowProb",
        )

        # Verify we get the match group and the max_match_probability is 0.55
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["max_match_probability"], 0.55)

    def test_get_potential_matches_matched(self) -> None:
        """Tests does not return already matched PotentialMatches."""
        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

        self.match_group1.matched = self.now
        self.match_group1.save()

        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        self.assertEqual(matches, [])

    def test_get_potential_matches_deleted(self) -> None:
        """Tests does not return deleted PotentialMatches."""
        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

        self.match_group1.deleted = self.now
        self.match_group1.save()

        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        self.assertEqual(matches, [])

    def test_get_potential_matches_linked_records(self) -> None:
        """Tests get_potential_matches returns linked records that are not referenced by a SplinkResult."""
        matches = self.empi.get_potential_matches(
            last_name="Berry",
        )
        self.assertEqual(matches, [])

        # Record that is not included in SplinkResults, but is related to the same Person
        # as another record.
        PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person6.id,
            sha256=b"test-sha256-7",
            data_source="ds7",
            first_name="Jerry",
            last_name="Berry",
        )
        matches = self.empi.get_potential_matches(
            last_name="Berry",
        )
        self.assertEqual(
            matches,
            [
                PotentialMatchSummaryDict(
                    id=self.match_group2.id,
                    first_name="Tina",
                    last_name="Smith",
                    data_sources=["ds5", "ds6", "ds7"],
                    max_match_probability=0.95,
                )
            ],
        )

    def test_get_potential_matches_missing_results(self) -> None:
        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        expected = [
            PotentialMatchSummaryDict(
                id=self.match_group1.id,
                first_name="John",
                last_name="Doe",
                data_sources=["ds1", "ds2", "ds3", "ds4"],
                max_match_probability=0.95,
            )
        ]
        self.assertEqual(matches, expected)

        self.result2.delete()
        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        self.assertEqual(
            matches,
            [
                PotentialMatchSummaryDict(
                    id=self.match_group1.id,
                    first_name="John",
                    last_name="Doe",
                    data_sources=["ds1", "ds2"],
                    max_match_probability=0.95,
                )
            ],
        )

        self.result1.delete()
        matches = self.empi.get_potential_matches(
            first_name="john",
        )
        self.assertEqual(matches, [])

    #
    # get_potential_match
    #

    def test_get_potential_match(self) -> None:
        """Tests returns potential match."""
        match = self.empi.get_potential_match(self.match_group1.id)
        expected = PotentialMatchDict(
            id=self.match_group1.id,
            created=self.match_group1.created,
            version=self.match_group1.version,
            persons=[
                PersonDict(
                    uuid=str(person.uuid),
                    created=person.created,
                    version=person.version,
                    records=[
                        cast(
                            PersonRecordDict,
                            {
                                "id": record["id"],
                                "first_name": record["first_name"],
                                "last_name": record["last_name"],
                                "data_source": record["data_source"],
                            },
                        )
                        for record in PersonRecord.objects.filter(
                            person_id=person.id
                        ).values()
                    ],
                )
                for person in [self.person1, self.person2, self.person3, self.person4]
            ],
            results=[
                PredictionResultDict(
                    id=result.id,
                    created=result.created,
                    match_probability=result.match_probability,
                    person_record_l_id=result.person_record_l_id,
                    person_record_r_id=result.person_record_r_id,
                )
                for result in [self.result1, self.result2]
            ],
        )

        match["persons"] = sorted(match["persons"], key=lambda p: str(p["uuid"]))
        expected["persons"] = sorted(expected["persons"], key=lambda p: str(p["uuid"]))

        self.assertDictEqual(match, expected)
        self.assertEqual(
            {
                record["first_name"]
                for person in match["persons"]
                for record in person["records"]
            },
            {"John", "Jane", "Paul", "Linda"},
        )

        match = self.empi.get_potential_match(self.match_group2.id)
        expected = PotentialMatchDict(
            id=self.match_group2.id,
            created=self.match_group2.created,
            version=self.match_group2.version,
            persons=[
                PersonDict(
                    uuid=str(person.uuid),
                    created=person.created,
                    version=person.version,
                    records=[
                        cast(
                            PersonRecordDict,
                            {
                                "id": record["id"],
                                "first_name": record["first_name"],
                                "last_name": record["last_name"],
                                "data_source": record["data_source"],
                            },
                        )
                        for record in PersonRecord.objects.filter(
                            person_id=person.id
                        ).values()
                    ],
                )
                for person in [self.person5, self.person6]
            ],
            results=[
                PredictionResultDict(
                    id=result.id,
                    created=result.created,
                    match_probability=result.match_probability,
                    person_record_l_id=result.person_record_l_id,
                    person_record_r_id=result.person_record_r_id,
                )
                for result in [self.result3]
            ],
        )

        match["persons"] = sorted(match["persons"], key=lambda p: str(p["uuid"]))
        expected["persons"] = sorted(expected["persons"], key=lambda p: str(p["uuid"]))

        self.assertDictEqual(match, expected)
        self.assertEqual(
            {
                record["first_name"]
                for person in match["persons"]
                for record in person["records"]
            },
            {"Tina", "Tom"},
        )

    def test_get_potential_match_missing(self) -> None:
        """Tests throws error when match is missing."""
        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(12345689)

    def test_get_potential_match_matched(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)
        expected = PotentialMatchDict(
            id=self.match_group1.id,
            created=self.match_group1.created,
            version=self.match_group1.version,
            persons=[
                PersonDict(
                    uuid=str(person.uuid),
                    created=person.created,
                    version=person.version,
                    records=[
                        cast(
                            PersonRecordDict,
                            {
                                "id": record["id"],
                                "first_name": record["first_name"],
                                "last_name": record["last_name"],
                                "data_source": record["data_source"],
                            },
                        )
                        for record in PersonRecord.objects.filter(
                            person_id=person.id
                        ).values()
                    ],
                )
                for person in [self.person1, self.person2, self.person3, self.person4]
            ],
            results=[
                PredictionResultDict(
                    id=result.id,
                    created=result.created,
                    match_probability=result.match_probability,
                    person_record_l_id=result.person_record_l_id,
                    person_record_r_id=result.person_record_r_id,
                )
                for result in [self.result1, self.result2]
            ],
        )

        match["persons"] = sorted(match["persons"], key=lambda p: str(p["uuid"]))
        expected["persons"] = sorted(expected["persons"], key=lambda p: str(p["uuid"]))

        self.assertDictEqual(match, expected)
        self.assertEqual(
            {
                record["first_name"]
                for person in match["persons"]
                for record in person["records"]
            },
            {"John", "Jane", "Paul", "Linda"},
        )

        self.match_group1.matched = self.now
        self.match_group1.save()

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

    def test_get_potential_match_deleted(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)
        expected = PotentialMatchDict(
            id=self.match_group1.id,
            created=self.match_group1.created,
            version=self.match_group1.version,
            persons=[
                PersonDict(
                    uuid=str(person.uuid),
                    created=person.created,
                    version=person.version,
                    records=[
                        cast(
                            PersonRecordDict,
                            {
                                "id": record["id"],
                                "first_name": record["first_name"],
                                "last_name": record["last_name"],
                                "data_source": record["data_source"],
                            },
                        )
                        for record in PersonRecord.objects.filter(
                            person_id=person.id
                        ).values()
                    ],
                )
                for person in [self.person1, self.person2, self.person3, self.person4]
            ],
            results=[
                PredictionResultDict(
                    id=result.id,
                    created=result.created,
                    match_probability=result.match_probability,
                    person_record_l_id=result.person_record_l_id,
                    person_record_r_id=result.person_record_r_id,
                )
                for result in [self.result1, self.result2]
            ],
        )

        match["persons"] = sorted(match["persons"], key=lambda p: str(p["uuid"]))
        expected["persons"] = sorted(expected["persons"], key=lambda p: str(p["uuid"]))

        self.assertDictEqual(match, expected)
        self.assertEqual(
            {
                record["first_name"]
                for person in match["persons"]
                for record in person["records"]
            },
            {"John", "Jane", "Paul", "Linda"},
        )

        self.match_group1.deleted = self.now
        self.match_group1.save()

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

    def test_get_potential_match_isolation_level(self) -> None:
        """Tests that get_potential_match is using repeatable read by delaying part of the query."""
        match: Optional[PotentialMatchDict] = None
        delay1 = threading.Event()
        delay2 = threading.Event()

        def mock_logger_info(msg: str) -> None:
            if msg == "Retrieved MatchGroup":
                delay1.set()
                delay2.wait()

        def thread1() -> None:
            """Thread 1: Calls get_potential_match and waits inside a transaction."""
            nonlocal match

            with patch.object(self.empi.logger, "info", side_effect=mock_logger_info):
                match = self.empi.get_potential_match(self.match_group1.id)

        def thread2() -> None:
            """Thread 2: Updates the PotentialMatch while thread 1 is waiting."""
            delay1.wait()

            # Django deletes the id field when deleting the row in the DB
            result2_id = self.result2.id
            self.result2.delete()
            self.result2.id = result2_id

            delay2.set()

        t1 = threading.Thread(target=thread1)
        t2 = threading.Thread(target=thread2)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        expected = PotentialMatchDict(
            id=self.match_group1.id,
            created=self.match_group1.created,
            version=self.match_group1.version,
            persons=[
                PersonDict(
                    uuid=str(person.uuid),
                    created=person.created,
                    version=person.version,
                    records=[
                        cast(
                            PersonRecordDict,
                            {
                                "id": record["id"],
                                "first_name": record["first_name"],
                                "last_name": record["last_name"],
                                "data_source": record["data_source"],
                            },
                        )
                        for record in PersonRecord.objects.filter(
                            person_id=person.id
                        ).values()
                    ],
                )
                for person in [self.person1, self.person2, self.person3, self.person4]
            ],
            results=[
                PredictionResultDict(
                    id=result.id,
                    created=result.created,
                    match_probability=result.match_probability,
                    person_record_l_id=result.person_record_l_id,
                    person_record_r_id=result.person_record_r_id,
                )
                for result in [self.result1, self.result2]
            ],
        )

        if match:
            match["persons"] = sorted(match["persons"], key=lambda p: str(p["uuid"]))
        else:
            self.fail()

        expected["persons"] = sorted(expected["persons"], key=lambda p: str(p["uuid"]))

        self.assertDictEqual(match, expected)

        # TODO: Add test that disables repeatable read (for now you can comment out the code)


class MatchPersonRecordsTestCase(TransactionTestCase):
    empi: EMPIService
    now: datetime
    common_person_record: Mapping[str, Any]
    config: Config
    job: Job
    person1: Person
    person2: Person
    person3: Person
    person4: Person
    person5: Person
    person6: Person
    person_record1: PersonRecord
    person_record2: PersonRecord
    person_record3: PersonRecord
    person_record4: PersonRecord
    person_record5: PersonRecord
    person_record6: PersonRecord
    match_group1: MatchGroup
    match_group2: MatchGroup
    result1: SplinkResult
    result2: SplinkResult
    result3: SplinkResult
    user: User

    def setUp(self) -> None:
        self.maxDiff = None
        self.empi = EMPIService()
        self.now = django_tz.now()

        self.config = Config.objects.create(
            **{
                "created": self.now,
                "splink_settings": {"test": 1},
                "potential_match_threshold": 1.0,
                "auto_match_threshold": 1.1,
            }
        )
        self.job = Job.objects.create(
            source_uri="s3://example/test",
            config_id=self.config.id,
            status="new",
        )
        self.common_person_record = {
            "created": self.now,
            "job_id": self.job.id,
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
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person3 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person4 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person5 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person6 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        self.person_record1 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person1.id,
            sha256=b"test-sha256-1",
            data_source="ds1",
            first_name="John",
            last_name="Doe",
        )

        self.person_record2 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-2",
            data_source="ds2",
            first_name="Jane",
            last_name="Smith",
        )

        self.person_record3 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person3.id,
            sha256=b"test-sha256-3",
            data_source="ds3",
            first_name="Paul",
            last_name="Lap",
        )
        self.person_record4 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person4.id,
            sha256=b"test-sha256-4",
            data_source="ds4",
            first_name="Linda",
            last_name="Love",
        )
        self.person_record5 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person5.id,
            sha256=b"test-sha256-5",
            data_source="ds5",
            first_name="Tina",
            last_name="Smith",
        )
        self.person_record6 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person6.id,
            sha256=b"test-sha256-6",
            data_source="ds6",
            first_name="Tom",
            last_name="Rom",
        )

        self.match_group1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result1 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group1.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=self.person_record1.id,
            person_record_r_id=self.person_record2.id,
            data={},
        )
        self.result2 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group1.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=self.person_record3.id,
            person_record_r_id=self.person_record4.id,
            data={},
        )

        self.match_group2 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result3 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group2.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=self.person_record5.id,
            person_record_r_id=self.person_record6.id,
            data={},
        )
        self.user = User.objects.create()

    def assert_match_group_updates(
        self,
        original_match_group: MatchGroup,
        updated_match_group: MatchGroup,
        match_event: MatchEvent,
    ) -> None:
        """Assert that only the version, updated and matched fields change after a MatchGroup has been matched."""
        self.assertTrue(isinstance(updated_match_group.updated, datetime))
        self.assertEqual(updated_match_group.updated, match_event.created)
        self.assertTrue(isinstance(updated_match_group.matched, datetime))
        self.assertEqual(updated_match_group.matched, match_event.created)
        self.assertEqual(updated_match_group.version, original_match_group.version + 1)

        self.assertEqual(updated_match_group.uuid, original_match_group.uuid)
        self.assertEqual(updated_match_group.created, original_match_group.created)
        self.assertEqual(updated_match_group.deleted, original_match_group.deleted)
        self.assertEqual(updated_match_group.deleted, None)
        self.assertEqual(updated_match_group.job_id, original_match_group.job_id)

    def assert_match_event_details(self, match_event: MatchEvent) -> None:
        """Assert that a new MatchEvent related to a manual-match event has certain fields."""
        self.assertTrue(isinstance(match_event.id, int))
        self.assertTrue(
            (django_tz.now() - timedelta(minutes=1))
            < match_event.created
            < django_tz.now()
        )
        self.assertEqual(match_event.job_id, None)
        self.assertEqual(match_event.type, MatchEventType.manual_match)

    def assert_person_action_ids(self) -> None:
        """Assert that remove-record PersonAction IDs come before add-record PersonAction IDs."""
        self.assertTrue(
            max(
                set(
                    PersonAction.objects.filter(
                        type=PersonActionType.remove_record
                    ).values_list("id", flat=True)
                )
            )
            < min(
                set(
                    PersonAction.objects.filter(
                        type=PersonActionType.add_record
                    ).values_list("id", flat=True)
                )
            )
        )

    def assert_match_group_action_details(
        self, match_group: MatchGroup, match_event: MatchEvent, performed_by: User
    ) -> None:
        """Assert that manual-match MatchGroupAction has certain fields."""
        self.assertEqual(
            MatchGroupAction.objects.filter(
                match_event_id=match_event.id,
                match_group_id=match_group.id,
                splink_result=None,
                type=MatchGroupActionType.match,
                performed_by_id=performed_by.id,
            ).count(),
            1,
        )

    def test_match_empty_updates(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        # Keep PersonRecords how they are
        person_updates: list[PersonUpdateDict] = []

        match_event = self.empi.match_person_records(
            match["id"], match["version"], person_updates, self.user
        )

        #
        # MatchGroup should no longer be a PotentialMatch
        #

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

        #
        # MatchGroup should be updated
        #

        match_group1 = MatchGroup.objects.get(id=self.match_group1.id)

        self.assert_match_group_updates(self.match_group1, match_group1, match_event)

        #
        # No new Persons should have been added
        #

        self.assertEqual(Person.objects.count(), 6)

        #
        # Persons not involved should remain the same
        #

        for person in [
            self.person1,
            self.person2,
            self.person3,
            self.person4,
            self.person5,
            self.person6,
        ]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=person.updated,
                    job=person.job,
                    version=person.version,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                person.id,
            )

        #
        # No new PersonRecords should have been added
        #

        self.assertEqual(PersonRecord.objects.count(), 6)

        #
        # PersonRecords should be updated
        #

        for record, person_id, person_updated, matched_or_reviewed in [
            (
                self.person_record1,
                self.person1.id,
                self.person_record1.person_updated,
                match_event.created,
            ),
            (
                self.person_record2,
                self.person2.id,
                self.person_record2.person_updated,
                match_event.created,
            ),
            (
                self.person_record3,
                self.person3.id,
                self.person_record3.person_updated,
                match_event.created,
            ),
            (
                self.person_record4,
                self.person4.id,
                self.person_record4.person_updated,
                match_event.created,
            ),
            (
                self.person_record5,
                self.person5.id,
                self.person_record5.person_updated,
                None,
            ),
            (
                self.person_record6,
                self.person6.id,
                self.person_record6.person_updated,
                None,
            ),
        ]:
            self.assertEqual(
                PersonRecord.objects.filter(
                    id=record.id,
                    person_id=person_id,
                    person_updated=person_updated,
                    matched_or_reviewed=matched_or_reviewed,
                ).count(),
                1,
                list(PersonRecord.objects.values()),
            )

        #
        # There should only be a single MatchEvent
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        self.assert_match_event_details(match_event)

        #
        # PersonActions related to returned MatchEvent, should have review type
        # and person/record_id pairs are as expected
        #

        self.assertEqual(PersonAction.objects.count(), 4)

        for person_id, person_record_id in [
            (self.person1.id, self.person_record1.id),
            (self.person2.id, self.person_record2.id),
            (self.person3.id, self.person_record3.id),
            (self.person4.id, self.person_record4.id),
        ]:
            self.assertEqual(
                PersonAction.objects.filter(
                    match_event_id=match_event.id,
                    match_group_id=self.match_group1.id,
                    person_id=person_id,
                    person_record_id=person_record_id,
                    type=PersonActionType.review,
                    performed_by_id=self.user.id,
                ).count(),
                1,
                (person_id, person_record_id),
            )

        #
        # There should be a single MatchGroupAction
        #

        self.assertEqual(MatchGroupAction.objects.count(), 1)
        self.assert_match_group_action_details(
            self.match_group1, match_event, self.user
        )

    def test_match_no_changes(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        # Keep PersonRecords how they are
        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(person["uuid"]),
                "version": person["version"],
                "new_person_record_ids": [record["id"] for record in person["records"]],
            }
            for person in match["persons"]
        ]

        match_event = self.empi.match_person_records(
            match["id"], match["version"], person_updates, self.user
        )

        #
        # MatchGroup should no longer be a PotentialMatch
        #

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

        #
        # MatchGroup should be updated
        #

        match_group1 = MatchGroup.objects.get(id=self.match_group1.id)

        self.assert_match_group_updates(self.match_group1, match_group1, match_event)

        #
        # No new Persons should have been added
        #

        self.assertEqual(Person.objects.count(), 6)

        #
        # Persons involved in match should have been updated
        #

        for person in [self.person1, self.person2, self.person3, self.person4]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=match_event.created,
                    job=person.job,
                    version=person.version + 1,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                person.id,
            )

        #
        # Persons not involved should remain the same
        #

        for person in [self.person5, self.person6]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=person.updated,
                    job=person.job,
                    version=person.version,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                person.id,
            )

        #
        # No new PersonRecords should have been added
        #

        self.assertEqual(PersonRecord.objects.count(), 6)

        #
        # PersonRecords should be updated
        #

        for record, person_id, person_updated, matched_or_reviewed in [
            (
                self.person_record1,
                self.person1.id,
                self.person_record1.person_updated,
                match_event.created,
            ),
            (
                self.person_record2,
                self.person2.id,
                self.person_record2.person_updated,
                match_event.created,
            ),
            (
                self.person_record3,
                self.person3.id,
                self.person_record3.person_updated,
                match_event.created,
            ),
            (
                self.person_record4,
                self.person4.id,
                self.person_record4.person_updated,
                match_event.created,
            ),
            (
                self.person_record5,
                self.person5.id,
                self.person_record5.person_updated,
                None,
            ),
            (
                self.person_record6,
                self.person6.id,
                self.person_record6.person_updated,
                None,
            ),
        ]:
            self.assertEqual(
                PersonRecord.objects.filter(
                    id=record.id,
                    person_id=person_id,
                    person_updated=person_updated,
                    matched_or_reviewed=matched_or_reviewed,
                ).count(),
                1,
                list(PersonRecord.objects.values()),
            )

        #
        # There should only be a single MatchEvent
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        self.assert_match_event_details(match_event)

        #
        # PersonActions related to returned MatchEvent, should have review type
        # and person/record_id pairs are as expected
        #

        self.assertEqual(PersonAction.objects.count(), 4)

        for person_id, person_record_id in [
            (self.person1.id, self.person_record1.id),
            (self.person2.id, self.person_record2.id),
            (self.person3.id, self.person_record3.id),
            (self.person4.id, self.person_record4.id),
        ]:
            self.assertEqual(
                PersonAction.objects.filter(
                    match_event_id=match_event.id,
                    match_group_id=self.match_group1.id,
                    person_id=person_id,
                    person_record_id=person_record_id,
                    type=PersonActionType.review,
                    performed_by_id=self.user.id,
                ).count(),
                1,
                (person_id, person_record_id),
            )

        #
        # There should be a single MatchGroupAction
        #

        self.assertEqual(MatchGroupAction.objects.count(), 1)
        self.assert_match_group_action_details(
            self.match_group1, match_event, self.user
        )

    def test_match_changes(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        # Move person records around
        person_updates: list[PersonUpdateDict] = [
            # person1 keeps it's existing record and gets a new one
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record2.id,
                ],
            },
            # person2 loses it's existing record and gets a new one
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [self.person_record3.id],
            },
            # person3 loses it's existing record
            {
                "uuid": str(self.person3.uuid),
                "version": self.person3.version,
                "new_person_record_ids": [],
            },
            # person4 doesn't change
        ]

        match_event = self.empi.match_person_records(
            match["id"], match["version"], person_updates, self.user
        )

        #
        # MatchGroup should no longer be a PotentialMatch
        #

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

        #
        # MatchGroup should be updated
        #

        match_group1 = MatchGroup.objects.get(id=self.match_group1.id)

        self.assert_match_group_updates(self.match_group1, match_group1, match_event)

        #
        # No new Persons should have been added
        #

        self.assertEqual(Person.objects.count(), 6)

        #
        # Persons involved in match should have been updated
        #

        for person, record_count, updated, deleted in [
            (self.person1, 2, match_event.created, None),
            (self.person2, 1, match_event.created, None),
            (self.person3, 0, match_event.created, match_event.created),
        ]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=updated,
                    job=person.job,
                    version=person.version + 1,
                    deleted=deleted,
                    record_count=record_count,
                ).count(),
                1,
                (person.id, record_count, list(Person.objects.values())),
            )

        #
        # Persons not involved should remain the same
        #

        for person in [self.person4, self.person5, self.person6]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=person.updated,
                    job=person.job,
                    version=person.version,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                (person.id, list(Person.objects.values())),
            )

        #
        # No new PersonRecords should have been added
        #

        self.assertEqual(PersonRecord.objects.count(), 6)

        #
        # PersonRecords should be updated
        #

        for record, person_id, person_updated, matched_or_reviewed in [
            (
                self.person_record1,
                self.person1.id,
                self.person_record1.person_updated,
                match_event.created,
            ),
            (
                self.person_record2,
                self.person1.id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record3,
                self.person2.id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record4,
                self.person4.id,
                self.person_record4.person_updated,
                match_event.created,
            ),
            (
                self.person_record5,
                self.person5.id,
                self.person_record5.person_updated,
                None,
            ),
            (
                self.person_record6,
                self.person6.id,
                self.person_record6.person_updated,
                None,
            ),
        ]:
            self.assertEqual(
                PersonRecord.objects.filter(
                    id=record.id,
                    person_id=person_id,
                    person_updated=person_updated,
                    matched_or_reviewed=matched_or_reviewed,
                ).count(),
                1,
                (
                    record.id,
                    person_id,
                    person_updated,
                    matched_or_reviewed,
                    list(PersonRecord.objects.values()),
                ),
            )

        #
        # There should only be a single MatchEvent
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        self.assert_match_event_details(match_event)

        #
        # PersonActions related to returned MatchEvent, should have expected type
        # and person/record_id pairs are as expected
        #

        # 2 review actions + 2 remove actions + 2 add actions
        self.assertEqual(PersonAction.objects.count(), 2 + (2 * 2))

        for person_id, person_record_id, type in [
            (self.person1.id, self.person_record1.id, PersonActionType.review),
            (self.person2.id, self.person_record2.id, PersonActionType.remove_record),
            (self.person1.id, self.person_record2.id, PersonActionType.add_record),
            (self.person3.id, self.person_record3.id, PersonActionType.remove_record),
            (self.person2.id, self.person_record3.id, PersonActionType.add_record),
            (self.person4.id, self.person_record4.id, PersonActionType.review),
        ]:
            self.assertEqual(
                PersonAction.objects.filter(
                    match_event_id=match_event.id,
                    match_group_id=self.match_group1.id,
                    person_id=person_id,
                    person_record_id=person_record_id,
                    type=type,
                    performed_by_id=self.user.id,
                ).count(),
                1,
                (
                    person_id,
                    person_record_id,
                    type,
                    list(PersonAction.objects.values()),
                ),
            )

        # Verify that IDs of removals are less than IDs of adds
        self.assert_person_action_ids()

        #
        # There should be a single MatchGroupAction
        #

        self.assertEqual(MatchGroupAction.objects.count(), 1)
        self.assert_match_group_action_details(
            self.match_group1, match_event, self.user
        )

    def test_match_changes_merge(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        # Move person records around
        person_updates: list[PersonUpdateDict] = [
            # person1 gets all the records
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record2.id,
                    self.person_record3.id,
                    self.person_record4.id,
                ],
            },
            # person2 gets all the records removed
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [],
            },
            # person3 gets all the records removed
            {
                "uuid": str(self.person3.uuid),
                "version": self.person3.version,
                "new_person_record_ids": [],
            },
            # person4 gets all the records removed
            {
                "uuid": str(self.person4.uuid),
                "version": self.person4.version,
                "new_person_record_ids": [],
            },
        ]

        match_event = self.empi.match_person_records(
            match["id"], match["version"], person_updates, self.user
        )

        #
        # MatchGroup should no longer be a PotentialMatch
        #

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

        #
        # MatchGroup should be updated
        #

        match_group1 = MatchGroup.objects.get(id=self.match_group1.id)

        self.assert_match_group_updates(self.match_group1, match_group1, match_event)

        #
        # No new Persons should have been added
        #

        self.assertEqual(Person.objects.count(), 6)

        #
        # Persons involved in match should have been updated
        #

        for person, record_count, updated, deleted in [
            (self.person1, 4, match_event.created, None),
            (self.person2, 0, match_event.created, match_event.created),
            (self.person3, 0, match_event.created, match_event.created),
            (self.person4, 0, match_event.created, match_event.created),
        ]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=updated,
                    job=person.job,
                    version=person.version + 1,
                    deleted=deleted,
                    record_count=record_count,
                ).count(),
                1,
                (person.id, record_count, list(Person.objects.values())),
            )

        #
        # Persons not involved should remain the same
        #

        for person in [self.person5, self.person6]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=person.updated,
                    job=person.job,
                    version=person.version,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                (person.id, list(Person.objects.values())),
            )

        #
        # No new PersonRecords should have been added
        #

        self.assertEqual(PersonRecord.objects.count(), 6)

        #
        # PersonRecords should be updated
        #

        for record, person_id, person_updated, matched_or_reviewed in [
            (
                self.person_record1,
                self.person1.id,
                self.person_record1.person_updated,
                match_event.created,
            ),
            (
                self.person_record2,
                self.person1.id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record3,
                self.person1.id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record4,
                self.person1.id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record5,
                self.person5.id,
                self.person_record5.person_updated,
                None,
            ),
            (
                self.person_record6,
                self.person6.id,
                self.person_record6.person_updated,
                None,
            ),
        ]:
            self.assertEqual(
                PersonRecord.objects.filter(
                    id=record.id,
                    person_id=person_id,
                    person_updated=person_updated,
                    matched_or_reviewed=matched_or_reviewed,
                ).count(),
                1,
                (
                    record.id,
                    person_id,
                    person_updated,
                    matched_or_reviewed,
                    list(PersonRecord.objects.values()),
                ),
            )

        #
        # There should only be a single MatchEvent
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        self.assert_match_event_details(match_event)

        #
        # PersonActions related to returned MatchEvent, should have expected type
        # and person/record_id pairs are as expected
        #

        # 1 review action + 3 remove actions + 3 add actions
        self.assertEqual(PersonAction.objects.count(), 1 + (3 * 2))

        for person_id, person_record_id, type in [
            (self.person1.id, self.person_record1.id, PersonActionType.review),
            (self.person2.id, self.person_record2.id, PersonActionType.remove_record),
            (self.person1.id, self.person_record2.id, PersonActionType.add_record),
            (self.person3.id, self.person_record3.id, PersonActionType.remove_record),
            (self.person1.id, self.person_record3.id, PersonActionType.add_record),
            (self.person4.id, self.person_record4.id, PersonActionType.remove_record),
            (self.person1.id, self.person_record4.id, PersonActionType.add_record),
        ]:
            self.assertEqual(
                PersonAction.objects.filter(
                    match_event_id=match_event.id,
                    match_group_id=self.match_group1.id,
                    person_id=person_id,
                    person_record_id=person_record_id,
                    type=type,
                    performed_by_id=self.user.id,
                ).count(),
                1,
                (
                    person_id,
                    person_record_id,
                    type,
                    list(PersonAction.objects.values()),
                ),
            )

        # Verify that IDs of removals are less than IDs of adds
        self.assert_person_action_ids()

        #
        # There should be a single MatchGroupAction
        #

        self.assertEqual(MatchGroupAction.objects.count(), 1)
        self.assert_match_group_action_details(
            self.match_group1, match_event, self.user
        )

    def test_match_changes_linked_records(self) -> None:
        person_record7 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person1.id,
            sha256=b"test-sha256-7",
            data_source="ds7",
            first_name="Jerry",
            last_name="Berry",
        )
        person_record8 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-8",
            data_source="ds8",
            first_name="Larry",
            last_name="Dairy",
        )
        person_record9 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person3.id,
            sha256=b"test-sha256-9",
            data_source="ds9",
            first_name="Simone",
            last_name="Limone",
        )
        person_record10 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person4.id,
            sha256=b"test-sha256-10",
            data_source="ds10",
            first_name="Stacy",
            last_name="Lacy",
        )

        match = self.empi.get_potential_match(self.match_group1.id)

        # Move person records around
        person_updates: list[PersonUpdateDict] = [
            # person1 keeps one of it's records and gets both records from person2
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [
                    self.person_record2.id,
                    person_record7.id,
                    person_record8.id,
                ],
            },
            # person2 loses both of its existing records and gets both records from person3
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [
                    self.person_record3.id,
                    person_record9.id,
                ],
            },
            # person3 loses both of it's existing records and gets one from person1
            {
                "uuid": str(self.person3.uuid),
                "version": self.person3.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            # person4 doesn't change
        ]

        match_event = self.empi.match_person_records(
            match["id"], match["version"], person_updates, self.user
        )

        #
        # MatchGroup should no longer be a PotentialMatch
        #

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

        #
        # MatchGroup should be updated
        #

        match_group1 = MatchGroup.objects.get(id=self.match_group1.id)

        self.assert_match_group_updates(self.match_group1, match_group1, match_event)

        #
        # No new Persons should have been added
        #

        self.assertEqual(Person.objects.count(), 6)

        #
        # Persons involved in match should have been updated
        #

        for person, record_count, updated, deleted in [
            (self.person1, 3, match_event.created, None),
            (self.person2, 2, match_event.created, None),
            (self.person3, 1, match_event.created, None),
        ]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=updated,
                    job=person.job,
                    version=person.version + 1,
                    deleted=deleted,
                    record_count=record_count,
                ).count(),
                1,
                (person.id, record_count, list(Person.objects.values())),
            )

        #
        # Persons not involved should remain the same
        #

        for person in [self.person4, self.person5, self.person6]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=person.updated,
                    job=person.job,
                    version=person.version,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                (person.id, list(Person.objects.values())),
            )

        #
        # No new PersonRecords should have been added
        #

        self.assertEqual(PersonRecord.objects.count(), 10)

        #
        # PersonRecords should be updated
        #

        for record, person_id, person_updated, matched_or_reviewed in [
            # person1
            (
                self.person_record2,
                self.person1.id,
                match_event.created,
                match_event.created,
            ),
            (
                person_record7,
                self.person1.id,
                person_record7.person_updated,
                match_event.created,
            ),
            (person_record8, self.person1.id, match_event.created, match_event.created),
            # person2
            (
                self.person_record3,
                self.person2.id,
                match_event.created,
                match_event.created,
            ),
            (person_record9, self.person2.id, match_event.created, match_event.created),
            # person3
            (
                self.person_record1,
                self.person3.id,
                match_event.created,
                match_event.created,
            ),
            # person4
            (
                self.person_record4,
                self.person4.id,
                self.person_record4.person_updated,
                match_event.created,
            ),
            (
                person_record10,
                self.person4.id,
                person_record10.person_updated,
                match_event.created,
            ),
            # person 5
            (
                self.person_record5,
                self.person5.id,
                self.person_record5.person_updated,
                None,
            ),
            # person 6
            (
                self.person_record6,
                self.person6.id,
                self.person_record6.person_updated,
                None,
            ),
        ]:
            self.assertEqual(
                PersonRecord.objects.filter(
                    id=record.id,
                    person_id=person_id,
                    person_updated=person_updated,
                    matched_or_reviewed=matched_or_reviewed,
                ).count(),
                1,
                (
                    record.id,
                    person_id,
                    person_updated,
                    matched_or_reviewed,
                    list(PersonRecord.objects.values()),
                ),
            )

        #
        # There should only be a single MatchEvent
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        self.assert_match_event_details(match_event)

        #
        # PersonActions related to returned MatchEvent, should have expected type
        # and person/record_id pairs are as expected
        #

        # 3 review actions + 5 remove actions + 5 add actions
        self.assertEqual(PersonAction.objects.count(), 3 + (5 * 2))

        for person_id, person_record_id, type in [
            # person1
            (self.person1.id, person_record7.id, PersonActionType.review),
            (self.person2.id, self.person_record2.id, PersonActionType.remove_record),
            (self.person1.id, self.person_record2.id, PersonActionType.add_record),
            (self.person2.id, person_record8.id, PersonActionType.remove_record),
            (self.person1.id, person_record8.id, PersonActionType.add_record),
            # person2
            (self.person3.id, self.person_record3.id, PersonActionType.remove_record),
            (self.person2.id, self.person_record3.id, PersonActionType.add_record),
            (self.person3.id, person_record9.id, PersonActionType.remove_record),
            (self.person2.id, person_record9.id, PersonActionType.add_record),
            # person3
            (self.person1.id, self.person_record1.id, PersonActionType.remove_record),
            (self.person3.id, self.person_record1.id, PersonActionType.add_record),
            # person4
            (self.person4.id, self.person_record4.id, PersonActionType.review),
            (self.person4.id, person_record10.id, PersonActionType.review),
        ]:
            self.assertEqual(
                PersonAction.objects.filter(
                    match_event_id=match_event.id,
                    match_group_id=self.match_group1.id,
                    person_id=person_id,
                    person_record_id=person_record_id,
                    type=type,
                    performed_by_id=self.user.id,
                ).count(),
                1,
                (
                    person_id,
                    person_record_id,
                    type,
                    list(PersonAction.objects.values()),
                ),
            )

        # Verify that IDs of removals are less than IDs of adds
        self.assert_person_action_ids()

        #
        # There should be a single MatchGroupAction
        #

        self.assertEqual(MatchGroupAction.objects.count(), 1)
        self.assert_match_group_action_details(
            self.match_group1, match_event, self.user
        )

    def test_match_new_persons(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        # Move person records around
        person_updates: list[PersonUpdateDict] = [
            # person1 keeps it's existing record and gets a new one
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record2.id,
                ],
            },
            # person2 loses it's existing record
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [],
            },
            # new person
            {
                "new_person_record_ids": [self.person_record3.id],
            },
            # person3 loses it's existing record
            {
                "uuid": str(self.person3.uuid),
                "version": self.person3.version,
                "new_person_record_ids": [],
            },
            # new person
            {
                "new_person_record_ids": [self.person_record4.id],
            },
            # person4 loses it's existing record
            {
                "uuid": str(self.person4.uuid),
                "version": self.person4.version,
                "new_person_record_ids": [],
            },
        ]

        match_event = self.empi.match_person_records(
            match["id"], match["version"], person_updates, self.user
        )

        #
        # MatchGroup should no longer be a PotentialMatch
        #

        with self.assertRaises(MatchGroup.DoesNotExist):
            self.empi.get_potential_match(self.match_group1.id)

        #
        # MatchGroup should be updated
        #

        match_group1 = MatchGroup.objects.get(id=self.match_group1.id)

        self.assert_match_group_updates(self.match_group1, match_group1, match_event)

        #
        # New Persons should have been added
        #

        self.assertEqual(Person.objects.count(), 8)

        #
        # Existing persons involved in match should have been updated
        #

        for person, record_count, updated, deleted in [
            (self.person1, 2, match_event.created, None),
            (self.person2, 0, match_event.created, match_event.created),
            (self.person3, 0, match_event.created, match_event.created),
            (self.person4, 0, match_event.created, match_event.created),
        ]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=updated,
                    job=person.job,
                    version=person.version + 1,
                    deleted=deleted,
                    record_count=record_count,
                ).count(),
                1,
                (person.id, record_count, list(Person.objects.values())),
            )

        #
        # Existing persons not involved should remain the same
        #

        for person in [self.person5, self.person6]:
            self.assertEqual(
                Person.objects.filter(
                    id=person.id,
                    uuid=person.uuid,
                    created=person.created,
                    updated=person.updated,
                    job=person.job,
                    version=person.version,
                    deleted=None,
                    record_count=person.record_count,
                ).count(),
                1,
                (person.id, list(Person.objects.values())),
            )

        #
        # New persons should have been added
        #

        new_persons = list(
            Person.objects.filter(
                created=match_event.created,
                updated=match_event.created,
                job=None,
                version=1,
                deleted=None,
                record_count=1,
            ).order_by("id")
        )

        self.assertTrue(len(new_persons), 2)

        for person in new_persons:
            self.assertTrue(isinstance(person.id, int))
            self.assertTrue(
                person.id
                not in {
                    self.person1.id,
                    self.person2.id,
                    self.person3.id,
                    self.person4.id,
                    self.person5.id,
                    self.person6.id,
                }
            )
            self.assertTrue(isinstance(person.uuid, uuid.UUID))

        #
        # No new PersonRecords should have been added
        #

        self.assertEqual(PersonRecord.objects.count(), 6)

        #
        # PersonRecords should be updated
        #

        for record, person_id, person_updated, matched_or_reviewed in [
            (
                self.person_record1,
                self.person1.id,
                self.person_record1.person_updated,
                match_event.created,
            ),
            (
                self.person_record2,
                self.person1.id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record3,
                new_persons[0].id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record4,
                new_persons[1].id,
                match_event.created,
                match_event.created,
            ),
            (
                self.person_record5,
                self.person5.id,
                self.person_record5.person_updated,
                None,
            ),
            (
                self.person_record6,
                self.person6.id,
                self.person_record6.person_updated,
                None,
            ),
        ]:
            self.assertEqual(
                PersonRecord.objects.filter(
                    id=record.id,
                    person_id=person_id,
                    person_updated=person_updated,
                    matched_or_reviewed=matched_or_reviewed,
                ).count(),
                1,
                (
                    record.id,
                    person_id,
                    person_updated,
                    matched_or_reviewed,
                    list(PersonRecord.objects.values()),
                ),
            )

        #
        # There should only be a single MatchEvent
        #

        self.assertEqual(MatchEvent.objects.count(), 1)
        self.assert_match_event_details(match_event)

        #
        # PersonActions related to returned MatchEvent, should have expected type
        # and person/record_id pairs are as expected
        #

        # 1 review actions + 3 remove actions + 3 add actions
        self.assertEqual(PersonAction.objects.count(), 1 + (3 * 2))

        for person_id, person_record_id, type in [
            (self.person1.id, self.person_record1.id, PersonActionType.review),
            (self.person2.id, self.person_record2.id, PersonActionType.remove_record),
            (self.person1.id, self.person_record2.id, PersonActionType.add_record),
            (self.person3.id, self.person_record3.id, PersonActionType.remove_record),
            (new_persons[0].id, self.person_record3.id, PersonActionType.add_record),
            (self.person4.id, self.person_record4.id, PersonActionType.remove_record),
            (new_persons[1].id, self.person_record4.id, PersonActionType.add_record),
        ]:
            self.assertEqual(
                PersonAction.objects.filter(
                    match_event_id=match_event.id,
                    match_group_id=self.match_group1.id,
                    person_id=person_id,
                    person_record_id=person_record_id,
                    type=type,
                    performed_by_id=self.user.id,
                ).count(),
                1,
                (
                    person_id,
                    person_record_id,
                    type,
                    list(PersonAction.objects.values()),
                ),
            )

        # Verify that IDs of removals are less than IDs of adds
        self.assert_person_action_ids()

        #
        # There should be a single MatchGroupAction
        #

        self.assertEqual(MatchGroupAction.objects.count(), 1)
        self.assert_match_group_action_details(
            self.match_group1, match_event, self.user
        )

    def test_validation_existing_person_fields(self) -> None:
        """A PersonUpdate for an existing Person should specify a version."""
        match = self.empi.get_potential_match(self.match_group1.id)

        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": "1",
                "new_person_record_ids": [],
            },
        ]

        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "A PersonUpdate for an existing Person should specify a version",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_new_person_fields(self) -> None:
        """A PersonUpdate for a new Person should not specify a version."""
        match = self.empi.get_potential_match(self.match_group1.id)

        person_updates: list[PersonUpdateDict] = [
            {
                "version": 1,
                "new_person_record_ids": [],
            },
        ]

        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "A PersonUpdate for a new Person should not specify a version",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_new_person_missing_records(self) -> None:
        """Check that if it's a new Person it also has 1 or more records."""
        match = self.empi.get_potential_match(self.match_group1.id)

        person_updates: list[PersonUpdateDict] = [
            {
                "new_person_record_ids": [],
            },
        ]

        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "A PersonUpdate for a new Person should have 1 or more new_record_ids",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_same_person_dupe(self) -> None:
        """A PersonRecord ID cannot exist twice in the same PersonUpdate."""
        match = self.empi.get_potential_match(self.match_group1.id)

        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [],
            },
            {
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record1.id,
                ],
            },
        ]

        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            (
                "A PersonRecord ID cannot exist twice in the same PersonUpdate."
                f" PersonRecord {self.person_record1.id} exists in update for Person index 1 twice."
            ),
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

        person_updates = [
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record1.id,
                ],
            },
        ]

        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            (
                "A PersonRecord ID cannot exist twice in the same PersonUpdate."
                f" PersonRecord {self.person_record1.id} exists in update for Person {self.person2.uuid} twice."
            ),
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_other_person_dupe(self) -> None:
        """A PersonRecord ID cannot exist in more than PersonUpdate."""
        match = self.empi.get_potential_match(self.match_group1.id)

        #
        # Duplicate in two new Persons
        #

        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [],
            },
            {
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "new_person_record_ids": [self.person_record1.id],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            (
                "A PersonRecord ID cannot exist in more than PersonUpdate."
                f" PersonRecord {self.person_record1.id} exists in updates for Person index 1 and Person index 2."
            ),
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

        #
        # Duplicate in two existing Persons
        #

        person_updates = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "new_person_record_ids": [self.person_record2.id],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            (
                "A PersonRecord ID cannot exist in more than PersonUpdate."
                f" PersonRecord {self.person_record1.id} exists in updates for Person {self.person1.uuid} and Person {self.person2.uuid}."
            ),
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

        #
        # Duplicate in two existing Persons and a new Person
        #

        person_updates = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            # Third update references person_record1
            {
                "new_person_record_ids": [self.person_record1.id],
            },
        ]
        # But we get the same message
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            (
                "A PersonRecord ID cannot exist in more than PersonUpdate."
                f" PersonRecord {self.person_record1.id} exists in updates for Person {self.person1.uuid} and Person {self.person2.uuid}."
            ),
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

        #
        # Duplicate in an existing Person and a new Person
        #

        person_updates = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "new_person_record_ids": [self.person_record1.id],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            (
                "A PersonRecord ID cannot exist in more than PersonUpdate."
                f" PersonRecord {self.person_record1.id} exists in updates for Person {self.person1.uuid} and Person index 1."
            ),
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_person_dupe(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "new_person_record_ids": [self.person_record1.id],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "The same Person UUID cannot exist in more than one PersonUpdate",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_potential_match_dne(self) -> None:
        with self.assertRaisesMessage(
            MatchGroup.DoesNotExist, "Potential match does not exist"
        ):
            self.empi.match_person_records(12345, 1, [], self.user)

    def test_validation_potential_match_deleted(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        self.match_group1.deleted = django_tz.now()
        self.match_group1.save()

        with self.assertRaisesMessage(
            MatchGroup.DoesNotExist, "Potential match has been replaced"
        ):
            self.empi.match_person_records(match["id"], match["version"], [], self.user)

    def test_validation_potential_match_matched(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        self.match_group1.matched = django_tz.now()
        self.match_group1.save()

        with self.assertRaisesMessage(
            InvalidPotentialMatch, "Potential match has already been matched"
        ):
            self.empi.match_person_records(match["id"], match["version"], [], self.user)

    def test_validation_potential_match_version(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)

        self.match_group1.version = self.match_group1.version + 1
        self.match_group1.save()

        with self.assertRaisesMessage(
            InvalidPotentialMatch, "Potential match version is outdated"
        ):
            self.empi.match_person_records(match["id"], match["version"], [], self.user)

    def test_validation_person_version_outdated(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)
        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                # incorrect version
                "version": self.person1.version + 1,
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record2.id,
                ],
            },
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                "new_person_record_ids": [],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate, "Invalid Person UUID or version outdated"
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_related_records(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)
        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                # Referencing person_record5
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record5.id,
                ],
            },
            # person5/person_record5 is connected with match_group2, not match_group1
            {
                "uuid": str(self.person5.uuid),
                "version": self.person5.version,
                "new_person_record_ids": [],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "PersonRecord IDs specified in new_person_record_ids must be related to PotentialMatch",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_dne_records(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)
        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id, 12345],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "PersonRecord IDs specified in new_person_record_ids must be related to PotentialMatch",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_related_persons(self) -> None:
        match = self.empi.get_potential_match(self.match_group1.id)
        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            # person5 is connected with match_group2, not match_group1
            {
                "uuid": str(self.person5.uuid),
                "version": self.person5.version,
                "new_person_record_ids": [self.person_record5.id],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "Specified Person UUID must be related to PotentialMatch",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

    def test_validation_corresponding_add_remove(self) -> None:
        """Check that if a record_id currently exists in a Person and is not in the corresponding person_update, it exists in another person_update."""
        match = self.empi.get_potential_match(self.match_group1.id)

        person_updates: list[PersonUpdateDict] = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                # person_record2 is added here, but not removed from person2
                "new_person_record_ids": [
                    self.person_record1.id,
                    self.person_record2.id,
                ],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "PersonRecord IDs that are added to a Person, must be removed from another Person",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )

        person_updates = [
            {
                "uuid": str(self.person1.uuid),
                "version": self.person1.version,
                "new_person_record_ids": [self.person_record1.id],
            },
            {
                "uuid": str(self.person2.uuid),
                "version": self.person2.version,
                # person_record2 is removed here, but not added to another Person
                "new_person_record_ids": [],
            },
        ]
        with self.assertRaisesMessage(
            InvalidPersonUpdate,
            "PersonRecord IDs that are removed from a Person, must be added to another Person",
        ):
            self.empi.match_person_records(
                match["id"], match["version"], person_updates, self.user
            )


class MatchPersonRecordsConcurrencyTestCase(TransactionTestCase):
    """Tests concurrency properties of match_person_records."""

    now: datetime
    config: Config
    job: Job
    person1: Person
    person2: Person
    person_record1: PersonRecord
    person_record2: PersonRecord
    match_group1: MatchGroup
    match_group2: MatchGroup
    result1: SplinkResult
    result2: SplinkResult
    user: User

    def setUp(self) -> None:
        self.now = django_tz.now()
        self.config = EMPIService().create_config(
            {
                "splink_settings": {},
                "potential_match_threshold": 0.001,
                "auto_match_threshold": 0.0013,
            }
        )
        self.job = EMPIService().create_job(
            "s3://tuva-health-example/test", self.config.id
        )

        common_person_record = {
            "created": self.now,
            "job_id": self.job.id,
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
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person3 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person4 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
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
        self.person_record3 = PersonRecord.objects.create(
            **common_person_record,
            person_id=self.person3.id,
            sha256=b"test-sha256-3",
            data_source="ds3",
            first_name="Paul",
            last_name="Lap",
        )
        self.person_record4 = PersonRecord.objects.create(
            **common_person_record,
            person_id=self.person4.id,
            sha256=b"test-sha256-4",
            data_source="ds4",
            first_name="Linda",
            last_name="Love",
        )

        self.match_group1 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result1 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group1.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=self.person_record1.id,
            person_record_r_id=self.person_record2.id,
            data={},
        )

        self.match_group2 = MatchGroup.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            matched=None,
            deleted=None,
        )
        self.result2 = SplinkResult.objects.create(
            created=self.now,
            job_id=self.job.id,
            match_group_id=self.match_group2.id,
            match_group_updated=self.now,
            match_probability=0.95,
            match_weight=0.95,
            person_record_l_id=self.person_record3.id,
            person_record_r_id=self.person_record4.id,
            data={},
        )
        self.user = User.objects.create()

    def test_concurrent_match_same_match_group(self) -> None:
        """Tests that if match_person_records holds the MatchGroup row lock, then another instance of match_person_records (with the same MatchGroup) waits."""

        # Run match_person_records and close DB connection
        def match_person_records_1() -> None:
            try:
                EMPIService().match_person_records(
                    self.match_group1.id, self.match_group1.version, [], self.user
                )
            finally:
                connection.close()

        # Run match_person_records and close DB connection
        def match_person_records_2() -> None:
            try:
                with self.assertRaisesMessage(
                    InvalidPotentialMatch, "Potential match has already been matched"
                ):
                    EMPIService().match_person_records(
                        self.match_group1.id, self.match_group1.version, [], self.user
                    )
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            patch1="main.services.empi.empi_service.EMPIService.validate_update_records",
            patch1_return=True,
            function1=match_person_records_1,
            patch2="main.services.empi.empi_service.EMPIService.validate_update_records",
            patch2_return=True,
            function2=match_person_records_2,
            post_contention_delay=3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNone(t2_entry)

        # Only EMPIService 1 should have succeeded
        self.assertEqual(
            MatchEvent.objects.filter(type=MatchEventType.manual_match).count(), 1
        )

    def test_concurrent_match_different_match_groups(self) -> None:
        """Tests that if match_person_records holds the MatchGroup row lock, then another instance of match_person_records (with the a different MatchGroup) can proceed concurrently."""

        # Run match_person_records and close DB connection
        def match_person_records_1() -> None:
            try:
                EMPIService().match_person_records(
                    self.match_group1.id, self.match_group1.version, [], self.user
                )
            finally:
                connection.close()

        # Run match_person_records and close DB connection
        def match_person_records_2() -> None:
            try:
                EMPIService().match_person_records(
                    self.match_group2.id, self.match_group2.version, [], self.user
                )
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            patch1="main.services.empi.empi_service.EMPIService.validate_update_records",
            patch1_return=True,
            function1=match_person_records_1,
            patch2="main.services.empi.empi_service.EMPIService.validate_update_records",
            patch2_return=True,
            function2=match_person_records_2,
            post_contention_delay=3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNotNone(t2_entry)

        # EMPIService 2 should have started around the same time as EMPIService 1
        self.assertLess(cast(float, t2_entry) - cast(float, t1_exit), 1)

        # Both instances should have succeeded
        self.assertEqual(
            MatchEvent.objects.filter(type=MatchEventType.manual_match).count(), 2
        )


class PersonsTestCase(TransactionTestCase):
    empi: EMPIService
    now: datetime
    common_person_record: Mapping[str, Any]
    config: Config
    job: Job
    person1: Person
    person2: Person
    person3: Person
    person4: Person

    def setUp(self) -> None:
        self.maxDiff = None
        self.empi = EMPIService()
        self.now = django_tz.now()

        self.config = Config.objects.create(
            **{
                "created": self.now,
                "splink_settings": {"test": 1},
                "potential_match_threshold": 1.0,
                "auto_match_threshold": 1.1,
            }
        )
        self.job = Job.objects.create(
            source_uri="s3://example/test",
            config_id=self.config.id,
            status="new",
        )
        self.common_person_record = {
            "created": self.now,
            "job_id": self.job.id,
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
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )
        self.person3 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job_id=self.job.id,
            version=1,
            record_count=1,
        )

        PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person1.id,
            sha256=b"test-sha256-1",
            data_source="ds1",
            first_name="John",
            last_name="Doe",
        )
        # The following two records are both associated with person2
        PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-2",
            data_source="ds2",
            first_name="Jane",
            last_name="Lane",
        )
        PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-3",
            data_source="ds3",
            first_name="Paul",
            last_name="Lap",
        )
        PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person3.id,
            sha256=b"test-sha256-4",
            data_source="ds4",
            first_name="Linda",
            last_name="Love",
        )

    #
    # get_persons
    #

    def test_get_all_persons(self) -> None:
        """Tests returns all persons by default."""
        matches = self.empi.get_persons(
            first_name="",
            last_name="",
            birth_date="",
            person_id="",
            source_person_id="",
            data_source="",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            ),
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            ),
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons()
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            ),
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            ),
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_by_first_name(self) -> None:
        """Tests searching by first name (case-insensitive)."""
        matches = self.empi.get_persons(
            first_name="john",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            )
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons(
            first_name="ohn",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_by_last_name(self) -> None:
        """Tests searching by last name (case-insensitive)."""
        matches = self.empi.get_persons(
            last_name="love",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons(
            last_name="lo",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons(
            last_name="ane",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            )
        ]
        self.assertEqual(matches, expected)

        # Paul Lap is connected with Person2, but we still use the first record's first/last name
        matches = self.empi.get_persons(
            last_name="lap",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_by_birth_date(self) -> None:
        """Tests searching by birth date."""
        matches = self.empi.get_persons(
            birth_date="1900-01-01",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            ),
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            ),
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons(
            birth_date="1900",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            ),
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            ),
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_by_person_id(self) -> None:
        """Tests searching by person ID."""
        matches = self.empi.get_persons(
            person_id=str(self.person1.uuid),
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            )
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons(
            # Testing a prefix search of person_id
            person_id=str(self.person3.uuid)[:20],
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            )
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_by_source_person_id(self) -> None:
        """Tests searching by source person ID."""
        matches = self.empi.get_persons(
            source_person_id="a2",
        )
        self.assertEqual(matches, [])

        matches = self.empi.get_persons(
            source_person_id="a1",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            ),
            PersonSummaryDict(
                uuid=str(self.person2.uuid),
                first_name="Jane",
                last_name="Lane",
                data_sources=["ds2", "ds3"],
            ),
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_by_data_source(self) -> None:
        """Tests searching by data source."""
        matches = self.empi.get_persons(
            data_source="ds1",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            ),
        ]
        self.assertEqual(matches, expected)

        matches = self.empi.get_persons(
            data_source="ds4",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person3.uuid),
                first_name="Linda",
                last_name="Love",
                data_sources=["ds4"],
            ),
        ]
        self.assertEqual(matches, expected)

    def test_get_persons_no_results(self) -> None:
        """Tests when no persons are found."""
        matches = self.empi.get_persons(
            first_name="Nonexistent",
        )

        self.assertEqual(matches, [])

    def test_get_persons_deleted(self) -> None:
        """Tests does not return deleted Persons."""
        matches = self.empi.get_persons(
            first_name="john",
        )
        expected = [
            PersonSummaryDict(
                uuid=str(self.person1.uuid),
                first_name="John",
                last_name="Doe",
                data_sources=["ds1"],
            )
        ]
        self.assertEqual(matches, expected)

        self.person1.deleted = self.now
        self.person1.save()

        matches = self.empi.get_persons(
            first_name="john",
        )
        self.assertEqual(matches, [])

    #
    # get_person
    #

    def test_get_person(self) -> None:
        """Tests returns person."""
        person = self.empi.get_person(str(self.person1.uuid))
        expected = PersonDict(
            uuid=str(self.person1.uuid),
            created=self.person1.created,
            version=self.person1.version,
            records=[
                cast(
                    PersonRecordDict,
                    {
                        **select_keys(
                            record,
                            record.keys() - {"job_id", "sha256", "person_id"},
                        ),
                        "created": record["created"].isoformat(),
                        "person_uuid": str(self.person1.uuid),
                        "person_updated": record["person_updated"].isoformat(),
                    },
                )
                for record in PersonRecord.objects.filter(
                    person_id=self.person1.id
                ).values()
            ],
        )

        person["records"] = sorted(person["records"], key=lambda r: str(r["id"]))
        expected["records"] = sorted(expected["records"], key=lambda r: str(r["id"]))

        self.assertDictEqual(person, expected)
        self.assertEqual(
            {record["first_name"] for record in person["records"]},
            {"John"},
        )

        person = self.empi.get_person(str(self.person2.uuid))
        expected = PersonDict(
            uuid=str(self.person2.uuid),
            created=self.person2.created,
            version=self.person2.version,
            records=[
                cast(
                    PersonRecordDict,
                    {
                        **select_keys(
                            record,
                            record.keys() - {"job_id", "sha256", "person_id"},
                        ),
                        "created": record["created"].isoformat(),
                        "person_uuid": str(self.person2.uuid),
                        "person_updated": record["person_updated"].isoformat(),
                    },
                )
                for record in PersonRecord.objects.filter(
                    person_id=self.person2.id
                ).values()
            ],
        )

        person["records"] = sorted(person["records"], key=lambda r: str(r["id"]))
        expected["records"] = sorted(expected["records"], key=lambda r: str(r["id"]))

        self.assertDictEqual(person, expected)
        self.assertEqual(
            {record["first_name"] for record in person["records"]},
            {"Jane", "Paul"},
        )

    def test_get_person_missing(self) -> None:
        """Tests throws error when person is missing."""
        with self.assertRaises(Person.DoesNotExist):
            self.empi.get_person(str(uuid.uuid4()))

    def test_get_person_deleted(self) -> None:
        person = self.empi.get_person(str(self.person1.uuid))
        expected = PersonDict(
            uuid=str(self.person1.uuid),
            created=self.person1.created,
            version=self.person1.version,
            records=[
                cast(
                    PersonRecordDict,
                    {
                        **select_keys(
                            record,
                            record.keys() - {"job_id", "sha256", "person_id"},
                        ),
                        "created": record["created"].isoformat(),
                        "person_uuid": str(self.person1.uuid),
                        "person_updated": record["person_updated"].isoformat(),
                    },
                )
                for record in PersonRecord.objects.filter(
                    person_id=self.person1.id
                ).values()
            ],
        )

        person["records"] = sorted(person["records"], key=lambda r: str(r["id"]))
        expected["records"] = sorted(expected["records"], key=lambda r: str(r["id"]))

        self.assertDictEqual(person, expected)
        self.assertEqual(
            {record["first_name"] for record in person["records"]},
            {"John"},
        )

        self.person1.deleted = self.now
        self.person1.save()

        with self.assertRaises(Person.DoesNotExist):
            self.empi.get_person(str(self.person1.uuid))


class ExportPersonRecordsTestCase(TestCase):
    empi: EMPIService
    now: datetime
    common_person_record: Mapping[str, Any]
    config: Config
    job: Job
    person1: Person
    person2: Person
    person_record1: PersonRecord
    person_record2: PersonRecord

    def setUp(self) -> None:
        """Set up test data."""
        self.empi = EMPIService()
        self.now = django_tz.now()

        self.config = Config.objects.create(
            splink_settings={},
            potential_match_threshold=0.8,
            auto_match_threshold=0.9,
        )

        self.job = Job.objects.create(
            config=self.config,
            status=JobStatus.succeeded,
            source_uri="s3://test/test",
        )

        self.common_person_record = {
            "created": self.now,
            "job_id": self.job.id,
            "person_updated": self.now,
            "matched_or_reviewed": None,
            "race": "W",
        }

        self.person1 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job=self.job,
            version=1,
            record_count=1,
        )
        self.person2 = Person.objects.create(
            uuid=uuid.uuid4(),
            created=self.now,
            updated=self.now,
            job=self.job,
            version=1,
            record_count=1,
        )

        self.person_record1 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person1.id,
            sha256=b"test-sha256-1",
            data_source="test1",
            source_person_id="1",
            first_name="John",
            last_name="Doe",
            sex="M",
            birth_date="1900-01-01",
            death_date="3000-01-01",
            social_security_number="123-45-6789",
            address="123 Main St",
            city="Anytown",
            state="ST",
            zip_code="12345",
            county="County",
            phone="555-555-5555",
        )

        self.person_record2 = PersonRecord.objects.create(
            **self.common_person_record,
            person_id=self.person2.id,
            sha256=b"test-sha256-2",
            data_source="test2",
            source_person_id="2",
            first_name="Jane",
            last_name="Smith",
            sex="F",
            birth_date="1900-01-02",
            death_date="3000-01-02",
            social_security_number="987-65-4321",
            address="456 Oak St",
            city="Somewhere",
            state="ST",
            zip_code="54321",
            county="County",
            phone="555-555-5556",
        )

    def test_export(self) -> None:
        """Tests successful export of person records."""
        buffer = io.BytesIO()
        self.empi.export_person_records(buffer)

        # Convert bytes to string and split into lines
        csv_content = buffer.getvalue().decode("utf-8").strip().split("\n")

        # Verify CSV headers
        expected_headers = [
            "person_id",  # person_uuid -> person_id
            "source_person_id",
            "data_source",
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
        self.assertEqual(csv_content[0].split(","), expected_headers)

        # Verify CSV data
        data_rows = [row.split(",") for row in csv_content[1:]]
        self.assertEqual(len(data_rows), 2)

        # Sort rows by source_person_id for consistent comparison
        data_rows.sort(key=lambda x: x[1])

        # Verify first record
        self.assertEqual(data_rows[0][0], str(self.person1.uuid))  # person_uuid
        self.assertEqual(data_rows[0][1], "1")  # source_person_id
        self.assertEqual(data_rows[0][2], "test1")  # data_source
        self.assertEqual(data_rows[0][3], "John")  # first_name
        self.assertEqual(data_rows[0][4], "Doe")  # last_name
        self.assertEqual(data_rows[0][5], "M")  # sex
        self.assertEqual(data_rows[0][6], "W")  # race
        self.assertEqual(data_rows[0][7], "1900-01-01")  # birth_date
        self.assertEqual(data_rows[0][8], "3000-01-01")  # death_date
        self.assertEqual(data_rows[0][9], "123-45-6789")  # social_security_number
        self.assertEqual(data_rows[0][10], "123 Main St")  # address
        self.assertEqual(data_rows[0][11], "Anytown")  # city
        self.assertEqual(data_rows[0][12], "ST")  # state
        self.assertEqual(data_rows[0][13], "12345")  # zip_code
        self.assertEqual(data_rows[0][14], "County")  # county
        self.assertEqual(data_rows[0][15], "555-555-5555")  # phone

        # Verify second record
        self.assertEqual(data_rows[1][0], str(self.person2.uuid))  # person_uuid
        self.assertEqual(data_rows[1][1], "2")  # source_person_id
        self.assertEqual(data_rows[1][2], "test2")  # data_source
        self.assertEqual(data_rows[1][3], "Jane")  # first_name
        self.assertEqual(data_rows[1][4], "Smith")  # last_name
        self.assertEqual(data_rows[1][5], "F")  # sex
        self.assertEqual(data_rows[1][6], "W")  # race
        self.assertEqual(data_rows[1][7], "1900-01-02")  # birth_date
        self.assertEqual(data_rows[1][8], "3000-01-02")  # death_date
        self.assertEqual(data_rows[1][9], "987-65-4321")  # social_security_number
        self.assertEqual(data_rows[1][10], "456 Oak St")  # address
        self.assertEqual(data_rows[1][11], "Somewhere")  # city
        self.assertEqual(data_rows[1][12], "ST")  # state
        self.assertEqual(data_rows[1][13], "54321")  # zip_code
        self.assertEqual(data_rows[1][14], "County")  # county
        self.assertEqual(data_rows[1][15], "555-555-5556")  # phone

    def test_export_empty(self) -> None:
        """Tests export with no records."""
        # Delete all person records
        PersonRecord.objects.all().delete()

        buffer = io.BytesIO()
        self.empi.export_person_records(buffer)

        # Convert bytes to string and split into lines
        csv_content = buffer.getvalue().decode("utf-8").strip().split("\n")

        # Verify only headers are present
        self.assertEqual(len(csv_content), 1)
        expected_headers = [
            "person_id",  # person_uuid -> person_id
            "source_person_id",
            "data_source",
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
        self.assertEqual(csv_content[0].split(","), expected_headers)


class TestCreateRawTempTable(TestCase):
    """Test the _create_raw_temp_table helper function"""

    def setUp(self) -> None:
        self.mock_cursor = Mock(spec=CursorWrapper)
        self.mock_logger = Mock(spec=logging.Logger)
        self.valid_table_name = "test_table_123"
        self.valid_columns = ["first_name", "last_name", "email"]

    def test_create_raw_temp_table_success(self) -> None:
        """Test successful table creation"""
        # Arrange
        self.mock_cursor.execute.return_value = None

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertTrue(result["success"])
        self.assertIn("Table 'test_table_123' created successfully", result["message"])
        self.mock_cursor.execute.assert_called_once()

        # Verify SQL structure
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        self.assertIn("CREATE TEMP TABLE test_table_123", executed_sql)
        self.assertIn('"first_name" TEXT', executed_sql)
        self.assertIn('"last_name" TEXT', executed_sql)
        self.assertIn('"email" TEXT', executed_sql)

    def test_create_raw_temp_table_empty_columns(self) -> None:
        """Test error when no columns provided"""
        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            [],
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Cannot create table with no columns")
        self.mock_logger.error.assert_called_once_with("Cannot create table with no columns")
        self.mock_cursor.execute.assert_not_called()

    def test_create_raw_temp_table_programming_error_permission(self) -> None:
        """Test handling of permission denied errors"""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.ProgrammingError(
            "permission denied for schema public"
        )

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Insufficient permissions", result["error"])
        self.assertIn("test_table_123", result["error"])
        self.mock_logger.error.assert_called_once()

    def test_create_raw_temp_table_programming_error_syntax(self) -> None:
        """Test handling of SQL syntax errors"""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.ProgrammingError(
            "syntax error at or near 'CREATE'"
        )

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("SQL syntax or schema error", result["error"])
        self.assertIn("syntax error at or near 'CREATE'", result["error"])
        self.mock_logger.error.assert_called_once()

    def test_create_raw_temp_table_operational_error_disk_space(self) -> None:
        """Test handling of disk space errors"""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.OperationalError(
            "could not extend file: No space left on device"
        )

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Insufficient disk space", result["error"])
        self.assertIn("No space left on device", result["error"])
        self.mock_logger.error.assert_called_once()

    def test_create_raw_temp_table_operational_error_connection(self) -> None:
        """Test handling of connection errors"""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.OperationalError(
            "server closed the connection unexpectedly"
        )

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Database connection lost", result["error"])
        self.assertIn("server closed the connection", result["error"])

    def test_create_raw_temp_table_database_error(self) -> None:
        """Test handling of general database errors"""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.DatabaseError(
            "database is locked"
        )

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Database error creating table", result["error"])
        self.assertIn("database is locked", result["error"])

    def test_create_raw_temp_table_unexpected_error(self) -> None:
        """Test handling of unexpected errors"""
        # Arrange
        self.mock_cursor.execute.side_effect = ValueError("unexpected error")

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.valid_columns,
            self.mock_logger
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Unexpected error creating table", result["error"])
        self.assertIn("unexpected error", result["error"])

    def test_create_raw_temp_table_column_quoting(self) -> None:
        """Test that column names are properly quoted"""
        # Arrange
        self.mock_cursor.execute.return_value = None
        columns_with_special_chars = ["first name", "last-name", "email_address"]

        # Act
        result = EMPIService._create_raw_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            columns_with_special_chars,
            self.mock_logger
        )

        # Assert
        self.assertTrue(result["success"])
        executed_sql = self.mock_cursor.execute.call_args[0][0]
        self.assertIn('"first name" TEXT', executed_sql)
        self.assertIn('"last-name" TEXT', executed_sql)
        self.assertIn('"email_address" TEXT', executed_sql)


class TestLoadCSVIntoTempTable(TestCase):
    """Test the _load_csv_into_temp_table helper function"""

    def setUp(self) -> None:
        # Use MagicMock for better context manager support
        self.mock_cursor = MagicMock()
        self.mock_logger = MagicMock()

        # Set up the copy context manager properly
        mock_copy_obj = MagicMock()
        self.mock_cursor.copy.return_value.__enter__.return_value = mock_copy_obj
        self.mock_cursor.copy.return_value.__exit__.return_value = False

        # Set up other methods
        self.mock_cursor.execute.return_value = None
        self.mock_cursor.fetchone.return_value = [1000]  # Default COUNT result

        self.valid_table_name = "test_table_123"
        self.valid_columns = ["first_name", "last_name", "email"]
        self.test_source = "test_file.csv"


    @patch('main.services.empi.empi_service.DEFAULT_BUFFER_SIZE', 1024)
    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_success(self, mock_open_source: MagicMock) -> None:
        """Test successful CSV loading"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"chunk1", b"chunk2", b""]  # Simulate chunked reading
        mock_open_source.return_value.__enter__.return_value = mock_file

        # Mock the copy context manager
        mock_copy = Mock()
        self.mock_cursor.copy.return_value.__enter__.return_value = mock_copy
        self.mock_cursor.copy.return_value.__exit__.return_value = False

        self.mock_cursor.execute.return_value = None
        self.mock_cursor.fetchone.return_value = [1000]  # COUNT result

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertTrue(result["success"])
        self.assertEqual(result["message"], 1000)

        # Verify file operations
        mock_open_source.assert_called_once_with(self.test_source)
        # Verify chunked reading (3 calls: "chunk1", "chunk2", "")
        self.assertEqual(mock_file.read.call_count, 3)

        # Verify copy operations
        self.mock_cursor.copy.assert_called_once()
        # Verify data was written to copy (2 chunks)
        self.assertEqual(mock_copy.write.call_count, 2)
        mock_copy.write.assert_any_call(b"chunk1")
        mock_copy.write.assert_any_call(b"chunk2")

        # Verify database operations
        self.mock_cursor.execute.assert_called_once()  # COUNT query
        self.mock_cursor.fetchone.assert_called_once()

        # Verify logging
        self.mock_logger.info.assert_called_once()
        log_message = self.mock_logger.info.call_args[0][0]
        self.assertIn("Loaded 1,000 records", log_message)

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_copy_sql_structure(self, mock_open_source: MagicMock) -> None:
        """Test that the COPY SQL is properly structured"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]  # Single chunk
        mock_open_source.return_value.__enter__.return_value = mock_file

        mock_copy = Mock()
        self.mock_cursor.fetchone.return_value = [100]

        # Act
        EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        copy_call = self.mock_cursor.copy.call_args[0]
        copy_sql = str(copy_call[0])  # The SQL object

        # Verify SQL structure
        self.assertIn("COPY", copy_sql)
        self.assertIn("FROM STDIN WITH", copy_sql)
        self.assertIn("CSV", copy_sql)
        self.assertIn("HEADER", copy_sql)

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_file_not_found(self, mock_open_source: MagicMock) -> None:
        """Test handling of file not found errors"""
        # Arrange
        mock_open_source.side_effect = FileNotFoundError("No such file or directory")

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("CSV file not found", result["error"])
        self.assertIn("No such file or directory", result["error"])
        self.mock_logger.error.assert_called_once()

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_permission_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of file permission errors"""
        # Arrange
        mock_open_source.side_effect = PermissionError("Permission denied")

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Permission denied reading CSV file", result["error"])
        self.assertIn("Permission denied", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_encoding_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of file encoding errors"""
        # Arrange
        mock_file = Mock()
        mock_copy = Mock()
        # Encoding error occurs during read, not readline
        mock_file.read.side_effect = UnicodeDecodeError(
            'utf-8', b'\xff\xfe', 0, 2, 'invalid start byte'
        )
        mock_open_source.return_value.__enter__.return_value = mock_file

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("CSV file encoding error", result["error"])
        self.assertIn("file may not be UTF-8", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_psycopg_connection_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of database connection errors"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]
        mock_open_source.return_value.__enter__.return_value = mock_file

        # Error occurs during copy operation
        self.mock_cursor.copy.side_effect = psycopg.OperationalError(
            "connection to server lost"
        )

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("connection to server lost", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_psycopg_disk_space_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of disk space errors"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]
        mock_open_source.return_value.__enter__.return_value = mock_file

        self.mock_cursor.copy.side_effect = psycopg.OperationalError(
            "could not extend file: No space left on device"
        )

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("No space left on device", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_psycopg_data_format_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of CSV data format errors"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]
        mock_open_source.return_value.__enter__.return_value = mock_file

        self.mock_cursor.copy.side_effect = psycopg.DataError(
            "invalid input syntax for type integer"
        )

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("invalid input syntax", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_psycopg_permission_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of database permission errors"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]
        mock_open_source.return_value.__enter__.return_value = mock_file

        self.mock_cursor.copy.side_effect = psycopg.ProgrammingError(
            "permission denied for table test_table_123"
        )

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("permission denied", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_unexpected_error(self, mock_open_source: MagicMock) -> None:
        """Test handling of unexpected errors"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]
        mock_open_source.return_value.__enter__.return_value = mock_file

        self.mock_cursor.copy.side_effect = RuntimeError("unexpected error")

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Unexpected error loading CSV", result["error"])
        self.assertIn("unexpected error", result["error"])

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_zero_records(self, mock_open_source: MagicMock) -> None:
        """Test loading CSV with zero records (edge case)"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"header\n", b""]  # Only header, no data
        mock_open_source.return_value.__enter__.return_value = mock_file

        mock_copy = Mock()
        self.mock_cursor.copy.return_value.__enter__.return_value = mock_copy
        self.mock_cursor.fetchone.return_value = [0]  # No records loaded

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertTrue(result["success"])  # Still successful, just empty
        self.assertEqual(result["message"], 0)

        log_message = self.mock_logger.info.call_args[0][0]
        self.assertIn("Loaded 0 records", log_message)

    @patch('main.services.empi.empi_service.open_source')
    def test_load_csv_copy_context_manager_error(self, mock_open_source: MagicMock) -> None:
        """Test error when copy context manager fails to enter"""
        # Arrange
        mock_file = Mock()
        mock_file.read.side_effect = [b"data", b""]
        mock_open_source.return_value.__enter__.return_value = mock_file

        # Mock copy to raise exception on __enter__
        self.mock_cursor.copy.return_value.__enter__.side_effect = psycopg.OperationalError(
            "Failed to create copy context"
        )

        # Act
        result = EMPIService._load_csv_into_temp_table(
            self.mock_cursor,
            self.valid_table_name,
            self.test_source,
            self.valid_columns,
            self.mock_logger,
            1
        )

        # Assert
        self.assertFalse(result["success"])
        self.assertIn("Database error during CSV load", result["error"])
        self.assertIn("Failed to create copy context", result["error"])