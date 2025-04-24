from datetime import datetime
from typing import cast
from unittest.mock import MagicMock, patch

from django.db import connection
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from main.models import Config, Job, JobStatus
from main.services.empi.empi_service import EMPIService
from main.services.matching.job_runner import JobResult
from main.services.matching.matching_service import MatchingService
from main.tests.util.concurrency import run_with_lock_contention


class MatchingServiceTestCase(TestCase):
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
        """Method run_next_job should log error if Job runner fails to run the Job."""
        mock_run_job.return_value = JobResult(1, "Out of memory\n")
        self.matching_service.logger = MagicMock()

        self.matching_service.run_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is still new
        self.assertEqual(self.job.status, JobStatus.new)
        self.assertIsNone(self.job.reason)

        # Failure is logged
        self.matching_service.logger.error.assert_called_with(
            "Unexpected job runner failure: Out of memory\n"
        )
        self.assertTrue(
            any(
                call.args[0].startswith("Processed job")
                for call in self.matching_service.logger.info.call_args_list
            )
        )

    @patch("main.services.matching.process_job_runner.ProcessJobRunner.run_job")
    def test_run_next_job_failure_exc(self, mock_run_job: MagicMock) -> None:
        """Method run_next_job should throw if Job runner throws an exception."""
        mock_run_job.side_effect = ValueError("Out of memory exception")

        with self.assertRaisesMessage(ValueError, "Out of memory exception"):
            self.matching_service.run_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is still new
        self.assertEqual(self.job.status, JobStatus.new)
        self.assertIsNone(self.job.reason)

    @patch("main.services.matching.process_job_runner.ProcessJobRunner.run_job")
    def test_run_next_job_success(self, mock_run_job: MagicMock) -> None:
        """Method run_next_job should return if Job runner succeeds in running the Job."""
        mock_run_job.return_value = JobResult(0, None)
        self.matching_service.logger = MagicMock()

        self.matching_service.run_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is still new
        self.assertEqual(self.job.status, JobStatus.new)
        self.assertIsNone(self.job.reason)

        # Failure is not logged
        self.assertFalse(
            any(
                call.args[0].startswith("Unexpected job runner failure")
                for call in self.matching_service.logger.error.call_args_list
            )
        )
        self.assertTrue(
            any(
                call.args[0].startswith("Processed job")
                for call in self.matching_service.logger.info.call_args_list
            )
        )


class MatchingServiceConcurrencyTestCase(TransactionTestCase):
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
    def test_run_next_job_advisory_lock(self, mock_get_logger: MagicMock) -> None:
        """Tests that only a single instance of run_next_job runs at a time."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Run run_next_job and close DB connection
        def run_next_job() -> None:
            try:
                MatchingService().run_next_job()
            finally:
                connection.close()

        t1_exit, t2_entry = run_with_lock_contention(
            patch1="main.services.matching.matching_service.ProcessJobRunner.run_job",
            patch1_return=JobResult(return_code=0, error_message=None),
            function1=run_next_job,
            patch2="main.services.matching.matching_service.ProcessJobRunner.run_job",
            patch2_return=JobResult(return_code=0, error_message=None),
            function2=run_next_job,
            post_contention_delay=3,
        )

        self.assertIsNotNone(t1_exit)
        self.assertIsNotNone(t2_entry)

        # MatchingService 2 should only have run after MatchingService 1 released the lock
        self.assertGreater(cast(float, t2_entry) - cast(float, t1_exit), 3)

        # Both jobs should have succeeded
        self.job1.refresh_from_db()
        self.assertEqual(self.job1.status, JobStatus.new)
        self.job2.refresh_from_db()
        self.assertEqual(self.job2.status, JobStatus.new)

        all_info_calls = [str(call.args[0]) for call in mock_logger.info.call_args_list]
        job_finished_logs = [msg for msg in all_info_calls if "Processed job" in msg]

        self.assertEqual(len(job_finished_logs), 2)
