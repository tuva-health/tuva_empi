import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, cast
from unittest.mock import patch

from django.db import connection

logger = logging.getLogger(__name__)


def get_lock_waiting_count() -> int:
    """Return number of locks requested by running client transactions."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
                select count(*)
                from pg_locks l
                join pg_stat_activity a
                    on l.pid = a.pid
                where a.backend_type = 'client backend'
                and not l.granted
            """
        )
        row = cursor.fetchone()

        return cast(int, row[0])


class TooManyLocksRequested(Exception):
    """Did not expect so many processes to be waiting on locks."""


def run_with_lock_contention(
    patch1: str,
    patch1_return: Any,
    function1: Callable[[], Any],
    patch2: str,
    patch2_return: Any,
    function2: Callable[[], Any],
    post_contention_delay: int,
) -> tuple[Optional[float], Optional[float]]:
    """Runs two threads in such a way that they both compete for the same lock.

    NOTE: This depends on the two threads being the only two threads currently
    obtaining locks in PGSQL.
    """
    delay1 = threading.Event()
    delay2 = threading.Event()

    t1_exit: Optional[float] = None
    t2_entry: Optional[float] = None

    # mock_1 should be called after a lock is obtained in the first thread. This mock helps
    # us verify that a lock is obtained and allows us to introduce an artifical delay which
    # helps with verifying that the second thread waited for the first to finish.
    def mock1(*args: Any) -> Any:
        nonlocal t1_exit

        # Signal that thread 1 should hold the lock at this point
        delay1.set()
        # Wait for thread 2 to try to obtain the lock
        delay2.wait()

        t1_exit = time.time()

        # Add delay so that we can verify the lock is being held
        time.sleep(post_contention_delay)

        return patch1_return

    # mock_2 should be called after a lock is obtained in the second thread. This mock helps us
    # verify that the second thread waited for the first to finish.
    def mock2(*args: Any) -> Any:
        nonlocal t2_entry

        t2_entry = time.time()

        return patch2_return

    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            with patch(
                patch1,
                new=mock1,
            ):
                # Start thread 1
                future1 = executor.submit(function1)

                # Wait until thread 1 obtains the lock
                delay1.wait()

                # Querying PGSQL to ensure no other process is waiting on a lock
                waiting_count = get_lock_waiting_count()

                if waiting_count != 0:
                    raise TooManyLocksRequested(
                        f"Did not expect processes to be waiting on locks at this point. Found {waiting_count}"
                    )

                with patch(
                    patch2,
                    new=mock2,
                ):
                    # Start thread 2
                    future2 = executor.submit(function2)

                    # Querying PGSQL to ensure thread 2 is waiting on the lock
                    while True:
                        waiting_count = get_lock_waiting_count()

                        logger.info(
                            f"{waiting_count} client processes waiting for locks"
                        )

                        if waiting_count == 1:
                            break
                        elif future2.done():
                            logger.info("Thread 2 finished early")
                            break

                        time.sleep(0.1)

                    # Signal to allow thread 1 to release the lock and thread 2 to obtain the lock
                    delay2.set()

                    future1.result()
                    future2.result()
        finally:
            if delay2:
                # Unblock thread 1
                delay2.set()

    return t1_exit, t2_entry
