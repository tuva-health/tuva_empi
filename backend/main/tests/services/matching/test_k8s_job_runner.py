import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from main.config import (
    AppConfig,
    JobRunnerType,
    K8sJobRunnerConfig,
    K8sJobRunnerSecretVolumeConfig,
    MatchingServiceConfig,
)
from main.services.matching.job_runner import JobResult
from main.services.matching.k8s_job_runner import K8sJobRunner
from main.util.k8s import (
    ContainerState,
    ContainerTerminatedState,
    K8sJobAlreadyExists,
    PodState,
    SecretVolume,
)


class K8sJobRunnerTestCase(unittest.TestCase):
    mock_logger: MagicMock
    mock_k8s: MagicMock
    runner: K8sJobRunner
    config: AppConfig

    def setUp(self) -> None:
        get_logger_patcher = patch(
            "main.services.matching.k8s_job_runner.logging.getLogger"
        )
        k8s_client_class_patcher = patch(
            "main.services.matching.k8s_job_runner.K8sJobClient"
        )
        get_config_patcher = patch("main.services.matching.k8s_job_runner.get_config")

        self.addCleanup(get_logger_patcher.stop)
        self.addCleanup(k8s_client_class_patcher.stop)
        self.addCleanup(get_config_patcher.stop)

        mock_get_logger = get_logger_patcher.start()
        self.mock_logger = mock_get_logger.return_value

        mock_k8s_client_class = k8s_client_class_patcher.start()
        self.mock_k8s = mock_k8s_client_class.return_value

        mock_get_config = get_config_patcher.start()
        self.config = AppConfig.model_construct(
            version="test-version",
            matching_service=MatchingServiceConfig.model_construct(
                job_runner=JobRunnerType.k8s,
                k8s_job_runner=K8sJobRunnerConfig.model_construct(
                    job_image="tuva-empi:latest",
                    job_image_pull_policy="IfNotPresent",
                    job_config_secret_volume=K8sJobRunnerSecretVolumeConfig.model_construct(
                        secret_name="my-secret",
                        secret_key="config.json",
                        mount_path="/etc/config",
                    ),
                ),
            ),
        )
        mock_get_config.return_value = self.config

        self.runner = K8sJobRunner()

    def test__run_job_success(self) -> None:
        """Method _run_job should call K8sJobClient.run_job with expected parameters."""
        k8s_runner_config = self.config.matching_service.k8s_job_runner
        assert k8s_runner_config
        secret_volume = k8s_runner_config.job_config_secret_volume
        assert secret_volume

        self.runner._run_job("matching-job")

        self.mock_k8s.run_job.assert_called_once_with(
            job_name="matching-job",
            image=k8s_runner_config.job_image,
            image_pull_policy=k8s_runner_config.job_image_pull_policy,
            args=["matching-job"],
            secret_volume=SecretVolume(
                secret_name=secret_volume.secret_name,
                secret_key=secret_volume.secret_key,
                mount_path=secret_volume.mount_path,
            ),
            termination_grace_period_seconds=0,
            parallelism=1,
            completions=1,
            backoff_limit=0,
            env={
                "TUVA_EMPI_EXPECTED_VERSION": "test-version",
                "TUVA_EMPI_CONFIG_FILE": str(
                    Path(secret_volume.mount_path) / secret_volume.secret_key
                ),
            },
            service_account_name=None,
        )

    def test__run_job_already_exists(self) -> None:
        """Method _run_job should ignore a K8sJobAlreadyExists exception."""
        self.mock_k8s.run_job.side_effect = K8sJobAlreadyExists()

        self.runner._run_job("matching-job")

        self.mock_k8s.run_job.assert_called_once()

        self.mock_logger.info.assert_has_calls(
            [call("K8s job matching-job already exists. Resuming where we left off")]
        )

    def test__run_job_other_exception(self) -> None:
        """Method _run_job should raise exceptions besides K8sJobAlreadyExists."""
        self.mock_k8s.run_job.side_effect = ValueError("test")

        with self.assertRaises(ValueError):
            self.runner._run_job("matching-job")

    def test__wait_for_pod_success(self) -> None:
        """Method _wait_for_pod should return the PodState from K8sJobClient.wait_for_job_pods."""
        pod_state = PodState(name="pod-name", phase="Running")
        self.mock_k8s.wait_for_job_pods.return_value = [pod_state]

        result = self.runner._wait_for_pod("matching-job")

        self.assertEqual(result, pod_state)

    def test__wait_for_pod_timeout(self) -> None:
        """Method _wait_for_pod should log container state and raise if K8sJobClient.wait_for_job_pods times out."""
        pod_state = PodState(name="pod-name", phase="Running")
        container_state = ContainerState(waiting=None, terminated=None)
        self.mock_k8s.wait_for_job_pods.side_effect = TimeoutError()
        self.mock_k8s.get_job_pods.return_value = [pod_state]
        self.mock_k8s.get_pod_container_states.return_value = [container_state]

        with self.assertRaises(TimeoutError):
            self.runner._wait_for_pod("matching-job")

        self.mock_logger.exception.assert_has_calls(
            [
                call(
                    f"Timed out waiting for K8s job matching-job pod."
                    f" Pod container state: {asdict(container_state)}"
                )
            ]
        )

    def test__get_pod_logs(self) -> None:
        """Method _get_pod_logs should split lines returns from K8sJobClient.get_pod_logs and print them individually."""
        self.mock_k8s.get_pod_logs.return_value = "line1\nline2"

        with patch("builtins.print") as mock_print:
            self.runner._get_pod_logs("pod-name")

        mock_print.assert_has_calls([call("pod-name: line1"), call("pod-name: line2")])

    def test__stream_pod_logs(self) -> None:
        """Method _stream_pod_logs should print each line returned from K8sJobClient.stream_pod_logs."""
        self.mock_k8s.stream_pod_logs.return_value = ["line1", "line2"]

        with patch("builtins.print") as mock_print:
            self.runner._stream_pod_logs("pod-name")

        mock_print.assert_has_calls([call("pod-name: line1"), call("pod-name: line2")])

    def test__stream_pod_logs_until_job_completion(self) -> None:
        """Method _stream_pod_logs_until_job_completion should pring streamed logs until K8sJobClient.wait_for_job_completion returns."""
        self.mock_k8s.wait_for_job_completion.return_value = MagicMock()
        self.mock_k8s.stream_pod_logs.return_value = ["log"]

        with patch("builtins.print") as mock_print:
            self.runner._stream_pod_logs_until_job_completion("matching-job", "pod-1")

        self.mock_k8s.wait_for_job_completion.assert_called_once_with("matching-job")
        mock_print.assert_called_with("pod-1: log")

    def test__get_pod_container_state(self) -> None:
        """Method _get_pod_container_state should return first value from K8sJobClient.get_pod_container_states."""
        state = ContainerState(
            waiting=None,
            terminated=ContainerTerminatedState(
                exit_code=0,
                finished_at="",
                reason="Completed",
                started_at="",
            ),
        )
        self.mock_k8s.get_pod_container_states.return_value = [state]

        result = self.runner._get_pod_container_state("pod-1")
        self.assertEqual(result, state)


class RunJobTestCase(unittest.TestCase):
    mock_logger: MagicMock
    mock_k8s: MagicMock
    runner: K8sJobRunner
    config: AppConfig

    def setUp(self) -> None:
        # Patch logger, config, and K8s client
        logger_patcher = patch(
            "main.services.matching.k8s_job_runner.logging.getLogger"
        )
        k8s_patcher = patch("main.services.matching.k8s_job_runner.K8sJobClient")

        self.mock_logger = logger_patcher.start().return_value
        self.mock_k8s = k8s_patcher.start().return_value

        self.addCleanup(logger_patcher.stop)
        self.addCleanup(k8s_patcher.stop)

        self.runner = K8sJobRunner()

        # Patch core runner methods
        patchers = {
            "_run_job": patch.object(self.runner, "_run_job"),
            "_wait_for_pod": patch.object(
                self.runner, "_wait_for_pod", wraps=self.runner._wait_for_pod
            ),
            "_get_pod_logs": patch.object(self.runner, "_get_pod_logs"),
            "_stream_pod_logs_until_job_completion": patch.object(
                self.runner,
                "_stream_pod_logs_until_job_completion",
                wraps=self.runner._stream_pod_logs_until_job_completion,
            ),
            "_get_pod_container_state": patch.object(
                self.runner,
                "_get_pod_container_state",
                wraps=self.runner._get_pod_container_state,
            ),
        }

        patches = {}
        for name, patcher in patchers.items():
            patches[name] = patcher.start()
            self.addCleanup(patcher.stop)

        self.mock_run_job = patches["_run_job"]
        self.mock_wait_for_pod = patches["_wait_for_pod"]
        self.mock_get_pod_logs = patches["_get_pod_logs"]
        self.mock_stream_logs = patches["_stream_pod_logs_until_job_completion"]
        self.mock_get_state = patches["_get_pod_container_state"]

    def test_run_job_completed_success(self) -> None:
        """Method run_job should retrieve logs if the pod has already completed/succeeded."""
        self.mock_k8s.wait_for_job_pods.return_value = [
            PodState(name="pod-1", phase="Succeeded")
        ]
        self.mock_k8s.get_pod_logs.return_value = "line1\nline2"
        self.mock_k8s.get_pod_container_states.return_value = [
            ContainerState(
                waiting=None,
                terminated=ContainerTerminatedState(
                    exit_code=0,
                    finished_at="",
                    reason="Completed",
                    started_at="",
                ),
            )
        ]

        result = self.runner.run_job()

        self.assertEqual(result, JobResult(return_code=0, error_message=None))

        self.mock_run_job.assert_called_once_with("matching-job")
        self.mock_wait_for_pod.assert_called_once_with("matching-job")
        self.mock_get_pod_logs.assert_called_once_with("pod-1")
        self.mock_stream_logs.assert_not_called()
        self.mock_get_state.assert_called_once_with("pod-1")
        self.mock_k8s.delete_job.assert_called_once_with("matching-job")
        self.mock_k8s.wait_for_job_deletion.assert_called_once_with("matching-job")

    def test_run_job_completed_failure(self) -> None:
        """Method run_job should retrieve logs if the pod has already completed/failed."""
        self.mock_k8s.wait_for_job_pods.return_value = [
            PodState(name="pod-1", phase="Failed")
        ]
        self.mock_k8s.get_pod_logs.return_value = "line1\nline2"
        self.mock_k8s.get_pod_container_states.return_value = [
            ContainerState(
                waiting=None,
                terminated=ContainerTerminatedState(
                    exit_code=1,
                    finished_at="",
                    reason="Error",
                    started_at="",
                ),
            )
        ]

        result = self.runner.run_job()

        self.assertEqual(result, JobResult(return_code=1, error_message="Error"))

        self.mock_run_job.assert_called_once_with("matching-job")
        self.mock_wait_for_pod.assert_called_once_with("matching-job")
        self.mock_get_pod_logs.assert_called_once_with("pod-1")
        self.mock_stream_logs.assert_not_called()
        self.mock_get_state.assert_called_once_with("pod-1")
        self.mock_k8s.delete_job.assert_called_once_with("matching-job")
        self.mock_k8s.wait_for_job_deletion.assert_called_once_with("matching-job")

    def test_run_job_running_success(self) -> None:
        """Method run_job should stream logs if pod is running."""
        self.mock_k8s.wait_for_job_pods.return_value = [
            PodState(name="pod-1", phase="Running")
        ]
        self.mock_k8s.stream_pod_logs.return_value = ["log-1", "log-2"]
        self.mock_k8s.get_pod_container_states.return_value = [
            ContainerState(
                waiting=None,
                terminated=ContainerTerminatedState(
                    exit_code=0,
                    finished_at="",
                    reason="Completed",
                    started_at="",
                ),
            )
        ]

        result = self.runner.run_job()

        self.assertEqual(result, JobResult(return_code=0, error_message=None))

        self.mock_run_job.assert_called_once_with("matching-job")
        self.mock_wait_for_pod.assert_called_once_with("matching-job")
        self.mock_get_pod_logs.assert_not_called()
        self.mock_stream_logs.assert_called_once_with("matching-job", "pod-1")
        self.mock_get_state.assert_called_once_with("pod-1")
        self.mock_k8s.delete_job.assert_called_once_with("matching-job")
        self.mock_k8s.wait_for_job_deletion.assert_called_once_with("matching-job")

    def test_run_job_running_failure(self) -> None:
        """Method run_job should stream logs if the pod is running and return error_message if the pod container fails."""
        self.mock_k8s.wait_for_job_pods.return_value = [
            PodState(name="pod-1", phase="Running")
        ]
        self.mock_k8s.stream_pod_logs.return_value = ["log-1", "log-2"]
        self.mock_k8s.get_pod_container_states.return_value = [
            ContainerState(
                waiting=None,
                terminated=ContainerTerminatedState(
                    exit_code=1,
                    finished_at="",
                    reason="Error",
                    started_at="",
                ),
            )
        ]

        result = self.runner.run_job()

        self.assertEqual(result, JobResult(return_code=1, error_message="Error"))

        self.mock_run_job.assert_called_once_with("matching-job")
        self.mock_wait_for_pod.assert_called_once_with("matching-job")
        self.mock_get_pod_logs.assert_not_called()
        self.mock_stream_logs.assert_called_once_with("matching-job", "pod-1")
        self.mock_get_state.assert_called_once_with("pod-1")
        self.mock_k8s.delete_job.assert_called_once_with("matching-job")
        self.mock_k8s.wait_for_job_deletion.assert_called_once_with("matching-job")
