import selectors
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
        "s3_uri": "s3://tuva-mpi-engine-example/test",
        "status": JobStatus.new,
        "reason": None,
    }
    job: Job

    def setUp(self) -> None:
        self.matching_service = MatchingService()
        self.config = Config.objects.create(**self.config_partial)

        self.job_partial["config_id"] = self.config.id
        self.job = Job.objects.create(**self.job_partial)

    @patch("main.services.matching.matching_service.selectors.DefaultSelector")
    @patch("main.services.matching.matching_service.subprocess.Popen")
    def test_process_next_job_failure(
        self, mock_popen: MagicMock, mock_selector: MagicMock
    ) -> None:
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

        self.matching_service.process_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is failed and the reason is saved
        self.assertEqual(self.job.status, JobStatus.failed)
        self.assertIn("Out of memory", str(self.job.reason))

    @patch("main.services.matching.matching_service.selectors.DefaultSelector")
    @patch("main.services.matching.matching_service.subprocess.Popen")
    def test_process_next_job_success(
        self, mock_popen: MagicMock, mock_selector: MagicMock
    ) -> None:
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

        self.matching_service.process_next_job()

        # Refresh the job from the database to get updated status
        self.job.refresh_from_db()

        # Assert that the job status is succeeded and the reason is None
        self.assertEqual(self.job.status, JobStatus.succeeded)
        self.assertIsNone(self.job.reason)


if __name__ == "__main__":
    unittest.main()
