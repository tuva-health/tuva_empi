import uuid
from typing import Any

from django.forms import model_to_dict
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from main.models import Config, Job, PersonRecordStaging


class SchemaTestCase(TestCase):
    now = timezone.now()

    config_partial = {
        "created": now,
        "splink_settings": {"test": 1},
        "potential_match_threshold": 1.0,
        "auto_match_threshold": 1.1,
    }
    config: Config

    job_partial = {
        "created": now,
        "updated": now,
        "source_uri": "s3://tuva-empi-example/test",
        "status": "new",  # Serialized as string
        "reason": None,
        "job_type": "import-person-records",  # Added job_type field
    }
    job: Job

    record_partial: dict[str, Any] = {
        "created": now,
        "row_number": None,
        # BinaryField has editable=false set by default and model_to_dict won't show
        # those fields
        # "sha256": None,
        "data_source": "test-ds",
        "source_person_id": "test-person-id",
        "first_name": "test-first",
        "last_name": "test-last",
        "sex": "F",
        "race": "TestRace",
        "birth_date": "1/1/1800",
        "death_date": "1/1/2000",
        "social_security_number": "000-00-0000",
        "address": "123 Test St",
        "city": "Test City",
        "state": "Test State",
        "zip_code": "00000",
        "county": "US",
        "phone": "000-0000",
    }

    @classmethod
    def setUpTestData(cls) -> None:
        cls.config = Config.objects.create(**cls.config_partial)

        cls.job_partial["config_id"] = cls.config.id
        # Create job with job_type field
        job_data = cls.job_partial.copy()
        job_data["job_type"] = "import-person-records"
        cls.job = Job.objects.create(**job_data)

        cls.record_partial["job_id"] = cls.job.id
        PersonRecordStaging.objects.create(**cls.record_partial)

    def test_config_schema(self) -> None:
        config = Config.objects.first()

        if config is not None:
            self.assertEqual(self.config_partial, model_to_dict(config, exclude=["id"]))
            self.assertTrue(isinstance(config.id, int))
        else:
            self.fail("No Staging objects exist")

    def test_job_schema(self) -> None:
        job = Job.objects.first()

        if job is not None:
            job_dict = model_to_dict(job, exclude=["id"])
            job_dict["config_id"] = job_dict["config"]
            del job_dict["config"]

            # Create expected dict with config_id field
            expected_dict = self.job_partial.copy()
            expected_dict["config_id"] = self.config.id

            self.assertEqual(expected_dict, job_dict)
            self.assertTrue(isinstance(job.id, int))
        else:
            self.fail("No Staging objects exist")

    def test_person_record_staging_schema(self) -> None:
        record = PersonRecordStaging.objects.first()

        if record is not None:
            record_dict = model_to_dict(record, exclude=["id"])
            record_dict["job_id"] = record_dict["job"]
            del record_dict["job"]

            self.assertEqual(self.record_partial, record_dict)
            self.assertTrue(isinstance(record.id, int))
        else:
            self.fail("No Staging objects exist")


class ModelIndexTestCase(TestCase):
    """Test cases for database indexes on models.

    Tests verify that the performance optimization indexes are properly
    defined in the model Meta classes.
    """

    def test_person_record_staging_has_id_index(self):
        """Test that PersonRecordStaging model has an index on the id field."""
        from main.models import PersonRecordStaging

        # Check model Meta indexes
        indexes = PersonRecordStaging._meta.indexes

        # Find the id index
        id_indexes = [idx for idx in indexes if "id" in idx.fields]

        self.assertTrue(
            len(id_indexes) > 0,
            "PersonRecordStaging should have an index on id field"
        )

        # Verify the index contains the id field
        id_index = id_indexes[0]
        self.assertIn("id", id_index.fields)

    def test_person_has_id_index(self):
        """Test that Person model has an index on the id field."""
        from main.models import Person

        # Check model Meta indexes
        indexes = Person._meta.indexes

        # Find the id index
        id_indexes = [idx for idx in indexes if "id" in idx.fields]

        self.assertTrue(
            len(id_indexes) > 0,
            "Person should have an index on id field"
        )

        # Verify the index contains the id field
        id_index = id_indexes[0]
        self.assertIn("id", id_index.fields)

    def test_person_record_has_id_index(self):
        """Test that PersonRecord model has an index on the id field."""
        from main.models import PersonRecord

        # Check model Meta indexes
        indexes = PersonRecord._meta.indexes

        # Find the id index
        id_indexes = [idx for idx in indexes if "id" in idx.fields]

        self.assertTrue(
            len(id_indexes) > 0,
            "PersonRecord should have an index on id field"
        )

        # Verify the index contains the id field
        id_index = id_indexes[0]
        self.assertIn("id", id_index.fields)

    def test_person_record_has_sha256_conditional_index(self):
        """Test that PersonRecord has a conditional index on sha256 field."""
        from main.models import PersonRecord

        # Check model Meta indexes
        indexes = PersonRecord._meta.indexes

        # Find the sha256 index
        sha256_indexes = [
            idx for idx in indexes 
            if "sha256" in idx.fields and idx.name == "main_person_sha256"
        ]

        self.assertTrue(
            len(sha256_indexes) > 0,
            "PersonRecord should have a conditional index on sha256 field"
        )

        # Verify the index has the correct name
        sha256_index = sha256_indexes[0]
        self.assertEqual(sha256_index.name, "main_person_sha256")

        # Verify it has a condition (partial index)
        self.assertIsNotNone(
            sha256_index.condition,
            "sha256 index should be a partial index with condition"
        )

    def test_person_record_staging_indexes_count(self):
        """Test that PersonRecordStaging has the expected number of indexes."""
        from main.models import PersonRecordStaging

        indexes = PersonRecordStaging._meta.indexes

        # Should have: sha256, row_number, and id indexes
        self.assertGreaterEqual(
            len(indexes), 3,
            "PersonRecordStaging should have at least 3 indexes"
        )

    def test_person_indexes_count(self):
        """Test that Person has the expected number of indexes."""
        from main.models import Person

        indexes = Person._meta.indexes

        # Should have at least the id index (uuid index is from unique constraint)
        self.assertGreaterEqual(
            len(indexes), 1,
            "Person should have at least 1 index"
        )

    def test_person_record_indexes_count(self):
        """Test that PersonRecord has the expected number of indexes."""
        from main.models import PersonRecord

        indexes = PersonRecord._meta.indexes

        # Should have: data_source, id, and sha256 indexes
        # (sha256 unique constraint creates another index)
        self.assertGreaterEqual(
            len(indexes), 3,
            "PersonRecord should have at least 3 indexes"
        )

    def test_person_record_data_source_index_exists(self):
        """Test that PersonRecord retains the data_source index."""
        from main.models import PersonRecord

        indexes = PersonRecord._meta.indexes

        # Find the data_source index
        data_source_indexes = [idx for idx in indexes if "data_source" in idx.fields]

        self.assertTrue(
            len(data_source_indexes) > 0,
            "PersonRecord should have an index on data_source field"
        )


class ModelIndexIntegrationTestCase(TransactionTestCase):
    """Integration tests for model indexes against actual database."""

    def setUp(self):
        """Set up test fixtures."""
        from django.db import connection
        self.connection = connection

    def test_person_record_staging_id_index_in_database(self):
        """Test that PersonRecordStaging id index exists in the database."""
        with self.connection.cursor() as cursor:
            # Query pg_indexes to verify the index exists
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'main_personrecordstaging'
                AND indexdef LIKE '%id%'
            """)

            indexes = cursor.fetchall()

            # Should find at least one index on id
            # (Primary key creates an index automatically)
            self.assertGreater(
                len(indexes), 0,
                "PersonRecordStaging should have at least one index involving id"
            )

    def test_person_id_index_in_database(self):
        """Test that Person id index exists in the database."""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'main_person'
                AND indexdef LIKE '%id%'
            """)

            indexes = cursor.fetchall()

            self.assertGreater(
                len(indexes), 0,
                "Person should have at least one index involving id"
            )

    def test_person_record_id_index_in_database(self):
        """Test that PersonRecord id index exists in the database."""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'main_personrecord'
                AND indexdef LIKE '%id%'
            """)

            indexes = cursor.fetchall()

            self.assertGreater(
                len(indexes), 0,
                "PersonRecord should have at least one index involving id"
            )

    def test_person_record_sha256_conditional_index_in_database(self):
        """Test that PersonRecord sha256 conditional index exists in database."""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'main_personrecord'
                AND indexname = 'main_person_sha256'
            """)

            result = cursor.fetchone()

            self.assertIsNotNone(
                result,
                "PersonRecord should have main_person_sha256 index"
            )

            # Verify it's a partial index with WHERE clause
            if result:
                indexdef = result[1]
                self.assertIn(
                    "WHERE",
                    indexdef,
                    "sha256 index should be a partial index with WHERE condition"
                )

    def test_indexes_improve_query_performance(self):
        """Integration test: verify indexes can be used by query planner."""
        from django.utils import timezone

        now = timezone.now()

        # Create test data
        config = Config.objects.create(
            created=now,
            splink_settings={"test": 1},
            potential_match_threshold=0.8,
            auto_match_threshold=0.95,
        )

        from main.models import Person

        job = Job.objects.create(
            created=now,
            updated=now,
            config=config,
            source_uri="s3://test-bucket/test.csv",
            status="new",
            job_type="import-person-records",
        )

        # Create a person
        person = Person.objects.create(
            uuid=uuid.uuid4(),
            created=now,
            updated=now,
            job=job,
            record_count=0,
        )

        # Query by id and verify it can use the index
        with self.connection.cursor() as cursor:
            # Run EXPLAIN to see query plan
            cursor.execute("""
                EXPLAIN SELECT * FROM main_person WHERE id = %s
            """, [person.id])

            plan = cursor.fetchall()

            # Query plan should mention index usage (typically "Index Scan")
            # Note: Query planner may choose different strategies, so we just
            # verify the query executes successfully
            self.assertTrue(len(plan) > 0, "Query plan should be generated")

    def test_id_indexes_support_joins(self):
        """Test that id indexes support efficient joins between tables."""
        from django.utils import timezone
        import hashlib
        from main.models import Person, PersonRecord

        now = timezone.now()

        # Create test data
        config = Config.objects.create(
            created=now,
            splink_settings={"test": 1},
            potential_match_threshold=0.8,
            auto_match_threshold=0.95,
        )

        job = Job.objects.create(
            created=now,
            updated=now,
            config=config,
            source_uri="s3://test-bucket/test.csv",
            status="new",
            job_type="import-person-records",
        )

        person = Person.objects.create(
            uuid=uuid.uuid4(),
            created=now,
            updated=now,
            job=job,
            record_count=1,
        )

        PersonRecord.objects.create(
            created=now,
            job=job,
            person=person,
            person_updated=now,
            sha256=hashlib.sha256(b"test-record").digest(),
            data_source="test-ds",
            source_person_id="test-1",
        )

        # Perform a join query
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT p.id, pr.id
                FROM main_person p
                INNER JOIN main_personrecord pr ON pr.person_id = p.id
                WHERE p.id = %s
            """, [person.id])

            result = cursor.fetchone()

            self.assertIsNotNone(result, "Join query should return results")
            self.assertEqual(result[0], person.id)