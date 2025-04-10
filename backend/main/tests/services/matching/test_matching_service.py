import unittest
from unittest.mock import MagicMock, patch

from django.utils import timezone

from main.models import Config, Job, JobStatus
from main.services.matching.matching_service import MatchingService


class MatchingServiceTestCase(unittest.TestCase):
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
        "s3_uri": "s3://tuva-empi-example/test",
        "status": JobStatus.new,
        "reason": None,
    }
    job: Job

    def setUp(self) -> None:
        self.matching_service = MatchingService()
        self.config = Config.objects.create(**self.config_partial)

        self.job_partial["config_id"] = self.config.id
        self.job = Job.objects.create(**self.job_partial)

    @patch("main.services.matching.process_job_runner.ProcessJobRunner.run_job")
    def test_run_next_job_failure(self, mock_run_job: MagicMock) -> None:
        """Method run_next_job should update the Job in the DB if Job runner fails to run the Job."""
        mock_run_job.return_value = (1, "Out of memory\n")

        self.matching_service.run_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is failed and the reason is saved
        self.assertEqual(self.job.status, JobStatus.failed)
        self.assertIn("Out of memory", str(self.job.reason))

    @patch("main.services.matching.process_job_runner.ProcessJobRunner.run_job")
    def test_run_next_job_failure_exc(self, mock_run_job: MagicMock) -> None:
        """Method run_next_job should update the Job in the DB if Job runner throws an exception."""
        mock_run_job.side_effect = ValueError("Out of memory exception")

        self.matching_service.run_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is failed and the reason is saved
        self.assertEqual(self.job.status, JobStatus.failed)
        self.assertIn("Out of memory exception", str(self.job.reason))

    @patch("main.services.matching.process_job_runner.ProcessJobRunner.run_job")
    def test_run_next_job_success(self, mock_run_job: MagicMock) -> None:
        """Method run_next_job should update the Job in the DB if Job runner succeeds in running the Job."""
        mock_run_job.return_value = (0, None)

        self.matching_service.run_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is succeeded and the reason is None
        self.assertEqual(self.job.status, JobStatus.succeeded)
        self.assertIsNone(self.job.reason)
