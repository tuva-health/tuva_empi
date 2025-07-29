from typing import Any

from django.forms import model_to_dict
from django.test import TestCase
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
