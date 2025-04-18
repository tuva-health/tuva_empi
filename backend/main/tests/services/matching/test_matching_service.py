import threading
import time
from typing import Any, Optional, cast
from unittest.mock import MagicMock, patch

from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from main.models import Config, Job, JobStatus
from main.services.matching.matching_service import MatchingService


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


class MatchingServiceConcurrencyTestCase(TransactionTestCase):
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

    def setUp(self) -> None:
        self.matching_service = MatchingService()
        self.config = Config.objects.create(**self.config_partial)

        self.job_partial["config_id"] = self.config.id

    def test_run_next_job_advisory_lock(self) -> None:
        """Tests that only a single instance of run_next_job runs at a time."""
        jobs = [
            Job.objects.create(**self.job_partial),
            Job.objects.create(**self.job_partial),
        ]

        delay1 = threading.Event()
        delay2 = threading.Event()

        t1_exit: Optional[float] = None
        t2_entry: Optional[float] = None

        def mock_run_job1(self: Any, job: Job) -> tuple[int, Optional[str]]:
            nonlocal t1_exit

            # Signal that the MatchingService advisory lock should be held at this point
            delay1.set()
            # Wait for another instance of MatchingService to try to obtain the lock
            delay2.wait()

            t1_exit = time.time()
            return 0, None

        def mock_run_job2(self: Any, job: Job) -> tuple[int, Optional[str]]:
            nonlocal t2_entry

            t2_entry = time.time()
            return 0, None

        with patch(
            "main.services.matching.matching_service.ProcessJobRunner.run_job",
            new=mock_run_job1,
        ):
            t1 = threading.Thread(target=lambda: MatchingService().run_next_job())

            # Start MatchingService 1
            t1.start()

            # Wait until MatchingService 1 obtains the lock
            delay1.wait()

            with patch(
                "main.services.matching.matching_service.ProcessJobRunner.run_job",
                new=mock_run_job2,
            ):
                t2 = threading.Thread(target=lambda: MatchingService().run_next_job())

                # Start MatchingService 2
                t2.start()

                # Simulate MatchingService 1 processing the job
                time.sleep(2)

                # Signal to allow MatchingService 1 to finish and MatchingService 2 to start
                delay2.set()

                t1.join()
                t2.join()

        self.assertIsNotNone(t1_exit)
        self.assertIsNotNone(t2_entry)

        # MatchingService 2 should only have called run_job after MatchingService1 released the lock
        self.assertGreater(cast(float, t2_entry), cast(float, t1_exit))

        # Both jobs should have succeeded
        for job in jobs:
            job.refresh_from_db()
            self.assertEqual(job.status, JobStatus.succeeded, job.id)
