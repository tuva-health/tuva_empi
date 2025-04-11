import logging
import signal
import sys
import threading
import time
from types import FrameType
from typing import Optional

import psycopg.errors
from django.db import OperationalError, connection, transaction
from django.forms.models import model_to_dict
from django.utils import timezone

from main.models import (
    DbLockId,
    Job,
    JobStatus,
    PersonRecordStaging,
)
from main.services.matching.process_job_runner import ProcessJobRunner
from main.util.sql import obtain_advisory_lock


class MatchingService:
    logger: logging.Logger
    cancel: bool

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.cancel = False

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.handle_sigint)

    def handle_sigint(self, _: int, __: Optional[FrameType]) -> None:
        if not self.cancel:
            self.logger.info(
                "Ctrl+C received, stopping gracefully. Type Ctrl+C again to stop immediately"
            )
            self.stop()
        else:
            self.logger.info("Second Ctrl+C received, stopping immediately")
            sys.exit(1)

    def get_next_job(self) -> Optional[Job]:
        return (
            # nowait - throws if the lock is already held
            # no_key - doesn't lock on the primary key column (if it did, then txs in the child
            # process would wait trying to reference it as a foreign key)
            Job.objects.select_for_update(nowait=True, no_key=True)
            .filter(status=JobStatus.new)
            .order_by("id")
            .first()
        )

    def run_next_job(self) -> None:
        with transaction.atomic(durable=True):
            self.logger.info("Retrieving next job")

            # Obtain (wait for) lock to prevent multiple MatchingServices from running jobs at the same time.
            # Jobs should be run sequentially.
            with connection.cursor() as cursor:
                lock_acquired = obtain_advisory_lock(cursor, DbLockId.matching_service)
                assert lock_acquired

            # Throws if cannot lock latest job
            job = self.get_next_job()

            if not job:
                self.logger.info("No new jobs found")
                time.sleep(20)
                return

            self.logger.info("Found job: %s", model_to_dict(job))

            start_time = time.perf_counter()

            try:
                return_code, error_message = ProcessJobRunner().run_job(job)

                if return_code == 0:
                    self.logger.info(f"Job {job.id} succeeded")
                    Job.objects.filter(id=job.id).update(
                        status=JobStatus.succeeded, updated=timezone.now(), reason=None
                    )
                else:
                    self.logger.error(f"Job {job.id} failed: {error_message}")
                    Job.objects.filter(id=job.id).update(
                        status=JobStatus.failed,
                        updated=timezone.now(),
                        reason=f"Job process failed with exit code {return_code}: {error_message}",
                    )
            except Exception as e:
                self.logger.error(
                    f"Failed to run subprocess for job {job.id}: {str(e)}"
                )
                Job.objects.filter(id=job.id).update(
                    status=JobStatus.failed,
                    updated=timezone.now(),
                    reason=f"Failed to run job process: {str(e)}",
                )

            self.logger.info(f"Deleting staging records with job ID {job.id}")
            deleted_count, _ = PersonRecordStaging.objects.filter(
                job_id=job.id
            ).delete()
            self.logger.info(
                f"Deleted {deleted_count} staging records with job ID {job.id}"
            )

            end_time = time.perf_counter()
            elapsed_time = end_time - start_time

            self.logger.info(f"Processed job in {elapsed_time:.5f} seconds")

    def start(self) -> None:
        self.logger.info("Starting match worker")

        try:
            while True:
                if self.cancel:
                    break

                try:
                    self.run_next_job()
                except OperationalError as e:
                    if isinstance(e.__cause__, psycopg.errors.LockNotAvailable):
                        self.logger.error("Another match worker is already running")
                        self.stop()
                    else:
                        raise
                except Exception:
                    self.logger.error(
                        "Unexpected error processing match job", exc_info=True
                    )
                    raise
        finally:
            self.logger.info("Match worker stopped")

    def stop(self) -> None:
        self.logger.info("Stopping match worker gracefully")
        self.cancel = True
