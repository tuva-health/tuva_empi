import logging
import threading
from pathlib import Path
from typing import Optional

from main.config import get_config
from main.models import Job
from main.services.matching.job_runner import JobResult, JobRunner
from main.util.k8s import K8sJobClient, SecretVolume


class K8sJobRunner(JobRunner):
    logger: logging.Logger
    k8s: K8sJobClient

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.k8s = K8sJobClient()

    def _stream_job_pod_logs(self, job_name: str, pod_name: str) -> None:
        # TODO: Can we separate stdout from stderr and print them to their respective files?
        # See ProcessJobRunner
        for log_line in self.k8s.stream_pod_logs(pod_name):
            print(job_name + ": " + log_line)

    def _run_job(
        self,
        job_id: int,
        job_name: str,
        job_image: str,
        secret_volume: Optional[SecretVolume],
    ) -> None:
        self.k8s.run_job(
            job_name=job_name,
            # FIXME: Use production image, even in dev
            image=job_image,
            command=["python", "manage.py", "run_matcher_job", str(job_id)],
            secret_volume=secret_volume,
            termination_grace_period_secords=0,
            # Only launch a single pod
            parallelism=1,
            # Job succeeds when a single pod succeeds
            completions=1,
            # Retry 0 times before considering Job as failed
            backoff_limit=0,
            env=(
                {
                    "CONFIG_FILE": str(
                        Path(secret_volume.mount_path) / secret_volume.secret_key
                    )
                }
                if secret_volume
                else {}
            ),
        )

    def run_job(self, job: Job) -> JobResult:
        """Run Tuva EMPI Job as a k8s job.

        NOTE: Currently if anything fails interacting with the k8s API, we just throw
        (which will result in marking the MatchingService as failed). This is easy
        enough to get started with, but there are probably a lot of opportunities
        to retry in case of intermittent API/network issues.
        """
        job_name = f"matcher-job-{job.id}"
        config = get_config()["matching_service"]["k8s_job_runner"]

        # Run the k8s job
        try:
            self._run_job(
                job_id=job.id,
                job_name=job_name,
                job_image=config["job_image"],
                secret_volume=(
                    SecretVolume(
                        secret_name=config["job_config_secret_volume"]["secret_name"],
                        secret_key=config["job_config_secret_volume"]["secret_key"],
                        mount_path=config["job_config_secret_volume"]["mount_path"],
                    )
                    if config["job_config_secret_volume"]
                    else None
                ),
            )
        except Exception as e:
            self.logger.exception(f"Failed to submit k8s job {job_name}: {e}")
            raise

        # Wait for k8s job pod to start
        try:
            pod_states = self.k8s.wait_for_job_pods(
                job_name,
                expected_count=1,
                expected_pod_phases={"Running", "Succeeded", "Failed"},
                timeout_seconds=300,
            )
        except TimeoutError:
            # TODO: Get pod container statuses (gives reason the pod failed to start)
            self.logger.exception(f"Timed out waiting for k8s job {job_name} pod")
            raise
        except Exception as e:
            self.logger.exception(f"Failed waiting for k8s job {job_name} pod: {e}")
            raise

        # There should only be a single pod per job
        assert len(pod_states) == 1
        pod_state = pod_states[0]

        # If the job pod has already succeeded or failed, get the pod logs, container state and return
        if pod_states[0].phase in {"Succeeded", "Failed"}:
            print(job_name + ": " + self.k8s.get_pod_logs(pod_state.name))

            pod_container_states = self.k8s.get_pod_container_states(pod_state.name)

            # We only expect a single container for now
            assert len(pod_container_states) == 1
            pod_container_state = pod_container_states[0]

            # The container should be terminated if the pod phase is Succeeded or Failed
            assert pod_container_state.terminated
            return JobResult(
                return_code=pod_container_state.terminated.exit_code,
                # The reason is just 'Error'.
                # TODO: If we can't separate stderr from stdout, then we could just use the last
                # n lines.
                error_message=pod_container_state.terminated.reason,
            )

        # If the job is running, stream its pod logs
        log_thread = threading.Thread(
            target=lambda: self._stream_job_pod_logs(job_name, pod_state.name)
        )
        try:
            log_thread.start()

            # Wait for the job to finish
            try:
                self.k8s.wait_for_job_completion(job_name)
            except Exception as e:
                self.logger.exception(f"Failed waiting for k8s job to complete: {e}")
                raise
        finally:
            log_thread.join()

        pod_container_states = self.k8s.get_pod_container_states(pod_state.name)

        # We only expect a single container for now
        assert len(pod_container_states) == 1
        pod_container_state = pod_container_states[0]

        # The container should be terminated if the job is finished
        assert pod_container_state.terminated
        return JobResult(
            return_code=pod_container_state.terminated.exit_code,
            error_message=pod_container_state.terminated.reason,
        )
