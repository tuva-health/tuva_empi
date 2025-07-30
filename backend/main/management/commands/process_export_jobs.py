import logging
import os
import signal
import sys
import threading
import time
from types import FrameType
from typing import Any, Optional

import psutil
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.db.utils import OperationalError

from main.models import DbLockId, Job, JobStatus, JobType
from main.services.empi.empi_service import EMPIService
from main.util.sql import obtain_advisory_lock


class Command(BaseCommand):
    help = "Process export jobs in the background"
    logger: logging.Logger
    cancel: threading.Event

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.cancel = threading.Event()

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.handle_sigint)
            signal.signal(signal.SIGTERM, self.handle_sigterm)

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
        return Job.objects.filter(
            status=JobStatus.new, job_type=JobType.export_potential_matches
        ).count()

    def log_system_info(self) -> None:
        """Log current system resource usage."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            cpu_percent = process.cpu_percent()

            self.logger.info(
                f"System info - PID: {os.getpid()}, "
                f"Memory: {memory_info.rss / 1024 / 1024:.1f}MB, "
                f"CPU: {cpu_percent:.1f}%"
            )
        except Exception as e:
            self.logger.debug(f"Could not log system info: {e}")

    def process_next_job(
        self, empi_service: EMPIService, max_jobs_per_run: int
    ) -> None:
        with transaction.atomic(durable=True):
            # Obtain (wait for) lock to prevent multiple export processors from running jobs at the same time.
            # Jobs should be processed sequentially.
            with connection.cursor() as cursor:
                lock_acquired = obtain_advisory_lock(cursor, DbLockId.matching_service)
                assert lock_acquired

            # Get pending export jobs with limit
            pending_jobs = (
                Job.objects.select_for_update()
                .filter(
                    job_type=JobType.export_potential_matches,
                    status=JobStatus.new,
                )
                .order_by("created")[:max_jobs_per_run]
            )

            if not pending_jobs:
                return

            jobs_count = len(pending_jobs)
            self.logger.info(f"Found {jobs_count} new export job(s) to process")

            # Process jobs in batch
            for job in pending_jobs:
                try:
                    self.logger.info(
                        f"Processing export job {job.id} (created: {job.created})"
                    )
                    self.log_system_info()

                    start_time = time.perf_counter()

                    # Process the job
                    empi_service.process_export_job(job)

                    end_time = time.perf_counter()
                    elapsed_time = end_time - start_time

                    self.logger.info(
                        f"Export job {job.id} completed successfully in {elapsed_time:.5f} seconds"
                    )

                except Exception as e:
                    end_time = time.perf_counter()
                    elapsed_time = end_time - start_time

                    self.logger.exception(
                        f"Export job {job.id} failed after {elapsed_time:.5f} seconds: {str(e)}"
                    )
                    # Continue processing other jobs even if one fails

    def stop(self) -> None:
        self.logger.info("Stopping export job processor gracefully")
        self.cancel.set()

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--sleep",
            type=int,
            default=30,  # Increased from 10 to 30 seconds
            help="Sleep time between job checks in seconds (default: 30)",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Process jobs once and exit",
        )
        parser.add_argument(
            "--max-jobs-per-run",
            type=int,
            default=5,
            help="Maximum number of jobs to process per run (default: 5)",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        sleep_time = options["sleep"]
        once = options["once"]
        max_jobs_per_run = options["max_jobs_per_run"]

        self.logger.info(
            f"Starting export job processor (sleep: {sleep_time}s, once: {once}, max_jobs_per_run: {max_jobs_per_run})"
        )
        self.log_system_info()

        empi_service = EMPIService()

        try:
            while True:
                if self.cancel.is_set():
                    break

                try:
                    jobs_available = self.get_available_jobs_count()

                    if jobs_available == 0:
                        if once:
                            self.logger.info("No pending jobs found, exiting")
                            break
                        else:
                            # Only log sleep message at DEBUG level to reduce noise
                            self.logger.debug(
                                f"No pending jobs found, sleeping for {sleep_time} seconds"
                            )
                            time.sleep(sleep_time)
                            continue

                    self.process_next_job(empi_service, max_jobs_per_run)

                    if once:
                        break

                except OperationalError as e:
                    self.logger.warning(f"Database connection error: {str(e)}")
                    time.sleep(sleep_time)
                except Exception as e:
                    self.logger.exception(
                        f"Unexpected error in export job processor: {str(e)}"
                    )

                    if once:
                        break
                    else:
                        time.sleep(sleep_time)

        finally:
            self.logger.info("Export job processor stopped")
