import logging
import selectors
import signal
import subprocess
import sys
import time
from types import FrameType
from typing import List, Optional, cast

import psycopg.errors
from django.db import OperationalError, connection, transaction
from django.db.backends.utils import CursorWrapper
from django.forms.models import model_to_dict
from django.utils import timezone
from psycopg import sql

from main.models import (
    MATCHING_SERVICE_LOCK_ID,
    Job,
    JobStatus,
    PersonRecordStaging,
)


class MatchingService:
    logger: logging.Logger
    cancel: bool

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.cancel = False

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

    def try_advisory_lock(self, cursor: CursorWrapper) -> bool:
        advisory_lock_sql = sql.SQL(
            """
                select pg_try_advisory_xact_lock(%(lock_id)s)
            """
        )
        cursor.execute(
            advisory_lock_sql,
            {"lock_id": MATCHING_SERVICE_LOCK_ID},
        )

        if cursor.rowcount > 0:
            row = cursor.fetchone()
            result = cast(bool, row[0])

            return result
        else:
            return False

    def get_next_job(self) -> Optional[Job]:
        return (
            # nowait - throws if the lock is already held
            # no_key - doesn't lock on the primary key column (if it did, then txs in the child
            # process would wait trying to reference it as a foreign key)
            Job.objects.select_for_update(nowait=True, no_key=True)
            .filter(status=JobStatus.new)
            .order_by("-created")
            .first()
        )

    def process_next_job(self) -> None:
        with transaction.atomic(durable=True):
            self.logger.info("Retrieving next job")

            with connection.cursor() as cursor:
                if not self.try_advisory_lock(cursor):
                    self.logger.error("Another match worker is already running")
                    self.stop()

            # Throws if cannot lock latest job
            job = self.get_next_job()

            if not job:
                self.logger.info("No new jobs found")
                time.sleep(20)
                return

            self.logger.info("Found job: %s", model_to_dict(job))

            start_time = time.perf_counter()

            try:
                process = subprocess.Popen(
                    ["python", "manage.py", "run_matcher_process", f"{job.id}"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    close_fds=True,
                    text=True,
                    bufsize=1,
                )
                sel = selectors.DefaultSelector()
                stderr_lines: List[str] = []

                if process.stderr is not None and process.stdout is not None:
                    sel.register(process.stdout, selectors.EVENT_READ)
                    sel.register(process.stderr, selectors.EVENT_READ)

                    # While there are still streams registered
                    while sel.get_map():
                        # Wait for new data on a stream
                        for key, _ in sel.select():
                            file_obj = key.fileobj

                            if hasattr(file_obj, "readline"):
                                line = file_obj.readline()

                                # Received EOF from stream (it's closed or broken), so unregister it
                                if not line:
                                    sel.unregister(file_obj)
                                    continue

                                if file_obj is process.stdout:
                                    print(line.rstrip())
                                else:
                                    print(line.rstrip(), file=sys.stderr)
                                    stderr_lines.append(line)

                    sel.close()
                    if process.stdout is not None:
                        process.stdout.close()
                    if process.stderr is not None:
                        process.stderr.close()

                # Let's make sure the process is finished before checking the return code
                process.wait()

                self.logger.info(f"Job process exited with code {process.returncode}")

                if process.returncode == 0:
                    self.logger.info(f"Job {job.id} succeeded")
                    Job.objects.filter(id=job.id).update(
                        status=JobStatus.succeeded, updated=timezone.now(), reason=None
                    )
                else:
                    error_message = (
                        "".join(stderr_lines)
                        if stderr_lines
                        else "Unknown error occurred"
                    )
                    self.logger.error(f"Job {job.id} failed: {error_message}")
                    Job.objects.filter(id=job.id).update(
                        status=JobStatus.failed,
                        updated=timezone.now(),
                        reason=f"Job process failed with exit code {process.returncode}: {error_message}",
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
                    self.process_next_job()
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
