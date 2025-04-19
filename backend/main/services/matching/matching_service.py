import logging
import signal
import sys
import threading
import time
from types import FrameType
from typing import Optional

from django.db import connection, transaction
from django.forms.models import model_to_dict
from django.utils import timezone

from main.config import JobRunnerType, get_config
from main.models import (
    DbLockId,
    Job,
    JobStatus,
    PersonRecordStaging,
)
from django.db.models import Q
from main.services.matching.job_runner import JobRunner
from main.services.matching.k8s_job_runner import K8sJobRunner
from main.services.matching.process_job_runner import ProcessJobRunner
from main.util.sql import obtain_advisory_lock


class MatchingServiceInterrupted(Exception):
    """MatchingService was interrupted by a signal."""

    pass


class MatchingService:
    logger: logging.Logger
    job_runner: JobRunner
    cancel: threading.Event

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.job_runner = self._get_job_runner()
        self.cancel = threading.Event()

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.handle_sigint)
            signal.signal(signal.SIGTERM, self.handle_sigterm)

    def _get_job_runner(self) -> JobRunner:
        job_runner = get_config()["matching_service"]["job_runner"]

        if job_runner == JobRunnerType.process.value:
            return ProcessJobRunner()
        elif job_runner == JobRunnerType.k8s.value:
            return K8sJobRunner()
        else:
            raise Exception("Job runner required")

    def handle_sigterm(self, _: int, __: Optional[FrameType]) -> None:
        self.logger.info("SIGTERM received, stopping gracefully")
        self.stop()

    def handle_sigint(self, _: int, __: Optional[FrameType]) -> None:
        if not self.cancel.is_set():
            self.logger.info(
                "Ctrl+C received, stopping gracefully. Type Ctrl+C again to stop immediately"
            )
            self.stop()
        else:
            self.logger.info("Second Ctrl+C received, stopping immediately")
            sys.exit(1)

    def get_available_jobs_count(self) -> int:
        return Job.objects.filter(Q(status=JobStatus.in_progress) | Q(status=JobStatus.new)).count()

    def run_next_job(self) -> None:
        # TODO: Check if there is a running job already and attach to it/get logs

        with transaction.atomic(durable=True):
            # Obtain (wait for) lock to prevent multiple MatchingServices from running jobs at the same time.
            # Jobs should be run sequentially.
            with connection.cursor() as cursor:
                lock_acquired = obtain_advisory_lock(cursor, DbLockId.matching_service)
                assert lock_acquired

            self.logger.info("Checking if there are any jobs available")

            jobs_available = self.get_available_jobs_count()

            if jobs_available == 0:
                self.logger.info("No new jobs found")
                time.sleep(10)
                return

            self.logger.info(f"Found {jobs_available} jobs. Running Matcher")

            start_time = time.perf_counter()

            try:
                job_result = self.job_runner.run_job()
            except Exception as e:
                # FIXME: What to do here? If we fail in run_job, either 1. the job wasn't started, 2. the job was started but we failed getting it's logs or 3. the job started and ran, but we failed in retrieving it's exit code or in some other way
                pass

            end_time = time.perf_counter()
            elapsed_time = end_time - start_time

            self.logger.info(f"Processed job in {elapsed_time:.5f} seconds")

    def start(self) -> None:
        self.logger.info("Starting MatchingService")

        try:
            while True:
                if self.cancel.is_set():
                    break

                try:
                    self.run_next_job()
                except Exception:
                    self.logger.exception("Unexpected error processing match job")
        finally:
            self.logger.info("MatchingService stopped")

    def stop(self) -> None:
        self.logger.info("Stopping MatchingService gracefully")
        self.cancel.set()
