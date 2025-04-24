import selectors
from typing import cast
from unittest.mock import MagicMock, patch

from django.test import TestCase
from django.utils import timezone

from main.models import Config, Job, JobStatus
from main.services.matching.process_job_runner import ProcessJobRunner


class ProcessJobRunnerTestCase(TestCase):
    now = timezone.now()
    process_job_runner: ProcessJobRunner

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
        self.process_job_runner = ProcessJobRunner()
        self.config = Config.objects.create(**self.config_partial)

        self.job_partial["config_id"] = self.config.id
        self.job = Job.objects.create(**self.job_partial)

    @patch("main.services.matching.process_job_runner.selectors.DefaultSelector")
    @patch("main.services.matching.process_job_runner.subprocess.Popen")
    def test_run_job_failure(
        self, mock_popen: MagicMock, mock_selector: MagicMock
    ) -> None:
        """Method run_job should return subprocess exit code and an error message if the subprocess returns a non-zero exit code."""
        # Set up mock process
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.readline.return_value = ""  # EOF for stdout
        mock_stderr.readline.return_value = "Out of memory\n"  # Error message
        mock_process.stdout = mock_stdout
        mock_process.stderr = mock_stderr
        mock_popen.return_value = mock_process

        # Set up mock selector
        mock_sel = MagicMock()
        mock_selector.return_value = mock_sel
        # Simulate reading from stderr then EOF
        mock_sel.select.side_effect = [
            [(MagicMock(fileobj=mock_stderr), selectors.EVENT_READ)],
            [],
        ]
        mock_sel.get_map.side_effect = [True, False]

        job_result = self.process_job_runner.run_job()

        # Refresh the job from the database to check for updates
        self.job.refresh_from_db()

        self.assertEqual(
            self.job.status, JobStatus.new, "Job status should remain the same"
        )
        self.assertEqual(job_result.return_code, 1)
        self.assertIsNotNone(job_result.error_message)
        self.assertIn("Out of memory", cast(str, job_result.error_message))

    @patch("main.services.matching.process_job_runner.selectors.DefaultSelector")
    @patch("main.services.matching.process_job_runner.subprocess.Popen")
    def test_run_job_success(
        self, mock_popen: MagicMock, mock_selector: MagicMock
    ) -> None:
        """Method run_job should return subprocess exit code and no error message if the subprocess returns a zero exit code."""
        # Set up mock process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.readline.return_value = "Success\n"
        mock_stderr.readline.return_value = ""  # EOF for stderr
        mock_process.stdout = mock_stdout
        mock_process.stderr = mock_stderr
        mock_popen.return_value = mock_process

        # Set up mock selector
        mock_sel = MagicMock()
        mock_selector.return_value = mock_sel
        # Simulate reading from stdout then EOF
        mock_sel.select.side_effect = [
            [(MagicMock(fileobj=mock_stdout), selectors.EVENT_READ)],
            [],
        ]
        mock_sel.get_map.side_effect = [True, False]

        job_result = self.process_job_runner.run_job()

        # Refresh the job from the database to check for updates
        self.job.refresh_from_db()

        self.assertEqual(
            self.job.status, JobStatus.new, "Job status should remain the same"
        )
        self.assertEqual(job_result.return_code, 0)
        self.assertIsNone(job_result.error_message)
