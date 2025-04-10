import logging
import selectors
import subprocess
import sys
from typing import Optional

from main.models import Job


class ProcessJobRunner:
    logger: logging.Logger

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run_job(self, job: Job) -> tuple[int, Optional[str]]:
        process = subprocess.Popen(
            ["python", "manage.py", "run_matcher_process", f"{job.id}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            text=True,
            bufsize=1,
        )
        sel = selectors.DefaultSelector()
        stderr_lines: list[str] = []

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
            error_message = None
        else:
            error_message = (
                "".join(stderr_lines) if stderr_lines else "Unknown error occurred"
            )

        return process.returncode, error_message
