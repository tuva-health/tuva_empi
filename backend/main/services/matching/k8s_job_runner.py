import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path

from main.config import get_config
from main.services.matching.job_runner import JobResult, JobRunner
from main.util.k8s import (
    ContainerState,
    K8sJobAlreadyExists,
    K8sJobClient,
    PodState,
    SecretVolume,
)


class K8sJobRunner(JobRunner):
    logger: logging.Logger
    k8s: K8sJobClient

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.k8s = K8sJobClient()

    def _run_job(self, job_name: str) -> None:
        self.logger.info(f"Creating K8s job {job_name}")

        config = get_config()
        version = config.version

        runner_config = config.matching_service.k8s_job_runner
        assert runner_config

        secret_volume = (
            SecretVolume(
                secret_name=runner_config.job_config_secret_volume.secret_name,
                secret_key=runner_config.job_config_secret_volume.secret_key,
                mount_path=runner_config.job_config_secret_volume.mount_path,
            )
            if runner_config.job_config_secret_volume
            else None
        )
        env = {"TUVA_EMPI_EXPECTED_VERSION": version}

        if secret_volume:
            env["TUVA_EMPI_CONFIG_FILE"] = str(
                Path(secret_volume.mount_path) / secret_volume.secret_key
            )
        elif "TUVA_EMPI_CONFIG_AWS_SECRET_ARN" in os.environ:
            env["TUVA_EMPI_CONFIG_AWS_SECRET_ARN"] = os.environ[
                "TUVA_EMPI_CONFIG_AWS_SECRET_ARN"
            ]

        try:
            self.k8s.run_job(
                job_name=job_name,
                image=runner_config.job_image,
                image_pull_policy=runner_config.job_image_pull_policy,
                args=["matching-job"],
                secret_volume=secret_volume,
                termination_grace_period_seconds=0,
                # Only launch a single pod
                parallelism=1,
                # Job succeeds when a single pod succeeds
                completions=1,
                # Retry 0 times before considering Job as failed
                backoff_limit=0,
                env=env,
                service_account_name=runner_config.job_service_account_name,
            )
        except K8sJobAlreadyExists:
            self.logger.info(
                f"K8s job {job_name} already exists. Resuming where we left off"
            )
        except Exception as e:
            self.logger.exception(f"Failed to create K8s job {job_name}: {e}")
            raise

    def _wait_for_pod(self, job_name: str) -> PodState:
        self.logger.info(
            f"Waiting for K8s job {job_name} pod to enter phase 'Running', 'Succeeded' or 'Failed'"
        )
        try:
            pod_states = self.k8s.wait_for_job_pods(
                job_name,
                expected_count=1,
                expected_pod_phases={"Running", "Succeeded", "Failed"},
                timeout_seconds=300,
            )
        except TimeoutError:
            pod_states = self.k8s.get_job_pods(job_name)

            if pod_states:
                assert len(pod_states) == 1
                container_state = self._get_pod_container_state(pod_states[0].name)

                self.logger.exception(
                    f"Timed out waiting for K8s job {job_name} pod."
                    f" Pod container state: {asdict(container_state)}"
                )
            else:
                self.logger.exception(
                    f"Timed out waiting for K8s job {job_name} pod. No pods created yet."
                )
            raise
        except Exception as e:
            self.logger.exception(f"Failed waiting for K8s job {job_name} pod: {e}")
            raise

        # There should only be a single pod per job
        assert len(pod_states) == 1
        return pod_states[0]

    def _get_pod_logs(self, pod_name: str) -> None:
        for log_line in self.k8s.get_pod_logs(pod_name).split("\n"):
            print(pod_name + ": " + log_line)

    def _stream_pod_logs(self, pod_name: str) -> None:
        for log_line in self.k8s.stream_pod_logs(pod_name):
            print(pod_name + ": " + log_line)

    def _stream_pod_logs_until_job_completion(
        self, job_name: str, pod_name: str
    ) -> None:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._stream_pod_logs, pod_name)

            try:
                # Wait for the job to finish
                self.k8s.wait_for_job_completion(job_name)
            except Exception as e:
                self.logger.exception(f"Failed waiting for K8s job to complete: {e}")
                raise

            try:
                # Wait for thread to finish and surface any exceptions
                future.result()
            except Exception as e:
                self.logger.exception(
                    f"Exception while streaming logs for job {job_name}: {e}"
                )
                raise

    def _get_pod_container_state(self, pod_name: str) -> ContainerState:
        pod_container_states = self.k8s.get_pod_container_states(pod_name)

        # We only expect a single container for now
        assert len(pod_container_states) == 1

        return pod_container_states[0]

    def run_job(self) -> JobResult:
        """Run Tuva EMPI Job as a K8s job.

        NOTE: If anything fails interacting with the K8s API, we just throw and allow the
        MatchingService to retry.
        """
        job_name = "matching-job"

        # Run the K8s job
        self._run_job(job_name)

        # Wait for K8s job pod to start
        pod_state = self._wait_for_pod(job_name)
        pod_name = pod_state.name
        pod_phase = pod_state.phase

        # If the job pod has already succeeded or failed, get the pod logs
        if pod_phase in {"Succeeded", "Failed"}:
            self.logger.info(
                f"K8s job {job_name} pod {pod_name} has already finished with phase {pod_phase}"
            )
            self._get_pod_logs(pod_name)
        else:
            # If the job is running, stream its pod logs
            self.logger.info(
                f"K8s job {job_name} is running, streaming pod {pod_name} logs"
            )
            self._stream_pod_logs_until_job_completion(job_name, pod_name)

        self.logger.info(f"K8s job {job_name} finished")

        # Retrieve JobResult based on pod container state
        container_state = self._get_pod_container_state(pod_name)

        # The container should be terminated by now
        assert container_state.terminated
        return_code = container_state.terminated.exit_code

        if return_code == 0:
            error_message = None
            self.logger.info(f"K8s job {job_name} succeeded")
        else:
            # The reason is just 'Error'
            error_message = container_state.terminated.reason
            self.logger.error(
                f"K8s job {job_name} failed."
                f" Pod {pod_name} container exited with code {return_code} and reason {error_message}."
                f" See pod logs for more details"
            )
            # Slow down retry
            time.sleep(5)

        # Delete job and wait for deletion
        self.k8s.delete_job(job_name)
        self.k8s.wait_for_job_deletion(job_name)

        return JobResult(
            return_code=return_code,
            error_message=error_message,
        )
