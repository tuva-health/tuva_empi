import unittest
from io import BytesIO
from typing import cast
from unittest.mock import MagicMock, patch

from kubernetes.client.exceptions import ApiException  # type: ignore[import-untyped]
from urllib3 import HTTPResponse

from main.util.k8s import (
    ContainerWaitingState,
    K8sJobAlreadyExists,
    K8sJobClient,
    K8sJobNotFound,
    UnexpectedStopIteration,
)


class K8sJobClientTestCase(unittest.TestCase):
    def setUp(self) -> None:
        with (
            patch("main.util.k8s.config.load_incluster_config"),
            patch("main.util.k8s.client.BatchV1Api"),
            patch("main.util.k8s.client.CoreV1Api"),
        ):
            self.k8s = K8sJobClient(namespace="test")

    def test_run_job_already_exists(self) -> None:
        """Method run_job throws a K8sJobAlreadyExists exception if k8s client throws a 409 ApiException with reason 'AlreadyExists'."""
        self.k8s.batch.create_namespaced_job = MagicMock(
            side_effect=ApiException(
                status=409,
                http_resp=HTTPResponse(
                    status=409,
                    body=BytesIO('{"reason": "AlreadyExists"}'.encode("utf-8")),
                    preload_content=False,
                ),
            )
        )

        with self.assertRaises(K8sJobAlreadyExists):
            self.k8s.run_job("test-job", "image")

    def test_run_job_other_409(self) -> None:
        """Method run_job re-raises ApiException if there is a reason other than 'AlreadyExists'."""
        self.k8s.batch.create_namespaced_job = MagicMock(
            side_effect=ApiException(
                status=409,
                http_resp=HTTPResponse(
                    status=409,
                    body=BytesIO('{"reason": "SomethingElse"}'.encode("utf-8")),
                    preload_content=False,
                ),
            )
        )

        with self.assertRaises(ApiException):
            self.k8s.run_job("test-job", "image")

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_pods_early_success(self, mock_stream: MagicMock) -> None:
        """Method wait_for_job_pods returns a pod's PodState if list_namespaced_pod returns a pod."""
        pod = MagicMock()
        pod.metadata.name = "test-pod"
        pod.status.phase = "Running"

        self.k8s.core.list_namespaced_pod = MagicMock()
        self.k8s.core.list_namespaced_pod.return_value.items = [pod]

        pods = self.k8s.wait_for_job_pods(
            "test-job", expected_count=1, expected_pod_phases={"Running"}
        )

        self.assertEqual(len(pods), 1)
        self.assertEqual(pods[0].name, pod.metadata.name)
        self.assertEqual(pods[0].phase, pod.status.phase)
        mock_stream.assert_not_called()

    @patch("main.util.k8s.watch.Watch.stream")
    @patch("main.util.k8s.watch.Watch.stop")
    def test_wait_for_job_pods_success(
        self, mock_stop: MagicMock, mock_stream: MagicMock
    ) -> None:
        """Method wait_for_job_pods returns a pod's PodState if stream returns an event with a pod with expected phase."""
        pod = MagicMock()
        pod.metadata.name = "test-pod"
        pod.status.phase = "Running"

        self.k8s.core.list_namespaced_pod = MagicMock()
        self.k8s.core.list_namespaced_pod.return_value.items = []
        self.k8s.core.list_namespaced_pod.return_value.metadata.resource_version = "100"
        mock_stream.return_value = [{"object": pod, "type": "test-event"}]

        pods = self.k8s.wait_for_job_pods(
            "test-job", expected_count=1, expected_pod_phases={"Running"}
        )

        self.assertEqual(len(pods), 1)
        self.assertEqual(pods[0].name, pod.metadata.name)
        self.assertEqual(pods[0].phase, pod.status.phase)
        mock_stream.assert_called()
        mock_stop.assert_called()

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_pods_timeout(self, mock_stream: MagicMock) -> None:
        """Method wait_for_job_pods raises TimeoutError if pods don't reach expected state within timeout."""
        pod = MagicMock()
        pod.metadata.name = "test-pod"
        pod.status.phase = "SomethingElse"

        self.k8s.core.list_namespaced_pod = MagicMock()
        self.k8s.core.list_namespaced_pod.return_value.items = []
        self.k8s.core.list_namespaced_pod.return_value.metadata.resource_version = "100"

        mock_stream.return_value = [{"object": pod, "type": "test-event"}]

        with self.assertRaises(TimeoutError):
            self.k8s.wait_for_job_pods(
                "test-job",
                expected_count=1,
                expected_pod_phases={"Running"},
                timeout_seconds=0,
            )

    @patch("main.util.k8s.watch.Watch.stream")
    def test_stream_pod_logs_yields_lines(self, mock_stream: MagicMock) -> None:
        """Method stream_pod_logs yields log lines."""
        mock_stream.return_value = ["line1", "line2"]

        with patch.object(self.k8s, "core"):
            logs = list(self.k8s.stream_pod_logs("some-pod"))

        self.assertEqual(logs, mock_stream.return_value)

    def test_get_pod_logs(self) -> None:
        """Method get_pod_logs returns logs as a single string."""
        self.k8s.core.read_namespaced_pod_log = MagicMock()
        self.k8s.core.read_namespaced_pod_log.return_value = "some-logs"

        result = self.k8s.get_pod_logs("some-pod")

        self.assertEqual(result, "some-logs")

    def test_wait_for_job_completion_job_not_found(self) -> None:
        """Method wait_for_job_completion raises K8sJobNotFound exception if job doesn't exist."""
        self.k8s.batch.list_namespaced_job = MagicMock()
        self.k8s.batch.list_namespaced_job.return_value.items = []

        with self.assertRaises(K8sJobNotFound):
            self.k8s.wait_for_job_completion("nonexistent")

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_completion_early_success(
        self, mock_stream: MagicMock
    ) -> None:
        """Method wait_for_job_completion returns job status if list_namespaced_job returns a job with succeeded/failed status."""
        job = MagicMock()
        job.status.succeeded = 1
        job.status.failed = None
        job.metadata.name = "test-job"

        self.k8s.batch.list_namespaced_job = MagicMock()
        self.k8s.batch.list_namespaced_job.return_value.items = [job]

        result = self.k8s.wait_for_job_completion("test-job")

        self.assertEqual(result.succeeded, job.status.succeeded)
        self.assertEqual(result.failed, job.status.failed)
        mock_stream.assert_not_called()

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_completion_success(self, mock_stream: MagicMock) -> None:
        """Method wait_for_job_completion returns job status if stream returns an event with a job with succeeded/failed status."""
        job = MagicMock()
        job.status.succeeded = 1
        job.status.failed = None
        job.metadata.name = "test-job"

        self.k8s.batch.list_namespaced_job = MagicMock()
        self.k8s.batch.list_namespaced_job.return_value.items = [MagicMock(status=None)]
        self.k8s.batch.list_namespaced_job.return_value.metadata.resource_version = (
            "100"
        )
        mock_stream.return_value = [{"object": job, "type": "test-event"}]

        result = self.k8s.wait_for_job_completion("test-job")

        self.assertEqual(result.succeeded, job.status.succeeded)
        self.assertEqual(result.failed, job.status.failed)
        mock_stream.assert_called()

    def test_get_pod_container_states(self) -> None:
        """Method get_pod_container_states retrieves pod container states correctly."""
        container_status = MagicMock()
        container_status.state.waiting.message = "waiting-msg"
        container_status.state.waiting.reason = "waiting-reason"
        container_status.state.terminated = None

        pod = MagicMock()
        pod.status.container_statuses = [container_status]

        self.k8s.core.read_namespaced_pod = MagicMock()
        self.k8s.core.read_namespaced_pod.return_value = pod

        states = self.k8s.get_pod_container_states("some-pod")

        self.assertEqual(len(states), 1)
        self.assertIsNotNone(states[0].waiting)
        self.assertEqual(
            cast(ContainerWaitingState, states[0].waiting).message,
            container_status.state.waiting.message,
        )
        self.assertEqual(
            cast(ContainerWaitingState, states[0].waiting).reason,
            container_status.state.waiting.reason,
        )
        self.assertEqual(states[0].terminated, container_status.state.terminated)

    def test_delete_job_ignores_404(self) -> None:
        """Method delete_job only ignores ApiExceptions if they have status 404."""
        self.k8s.batch.delete_namespaced_job = MagicMock()
        self.k8s.batch.delete_namespaced_job.side_effect = ApiException(status=405)
        with self.assertRaises(ApiException):
            self.k8s.delete_job("not-found")

        self.k8s.batch.delete_namespaced_job = MagicMock()
        self.k8s.batch.delete_namespaced_job.side_effect = ApiException(status=404)
        self.k8s.delete_job("not-found")  # Should not raise

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_deletion_early_success(self, mock_stream: MagicMock) -> None:
        """Method wait_for_job_deletion returns early if list_namespaced_job returns an empty list."""
        self.k8s.batch.list_namespaced_job = MagicMock()
        self.k8s.batch.list_namespaced_job.return_value.items = []  # Empty means it's been deleted

        self.k8s.wait_for_job_deletion("test-job")

        mock_stream.assert_not_called()

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_deletion_success(self, mock_stream: MagicMock) -> None:
        """Method wait_for_job_deletion returns if stream returns an deleted event for the job."""
        self.k8s.batch.list_namespaced_job = MagicMock()
        self.k8s.batch.list_namespaced_job.return_value.items = [MagicMock()]
        self.k8s.batch.list_namespaced_job.return_value.metadata.resource_version = (
            "123"
        )
        mock_stream.return_value = [{"type": "DELETED", "object": MagicMock()}]

        self.k8s.wait_for_job_deletion("test-job")

        mock_stream.assert_called()

    @patch("main.util.k8s.watch.Watch.stream")
    def test_wait_for_job_deletion_unexpected_stop(
        self, mock_stream: MagicMock
    ) -> None:
        """Method wait_for_job_deletion raises UnexpectedStopIteration if the stream ends without returning a deleted event."""
        self.k8s.batch.list_namespaced_job = MagicMock()
        self.k8s.batch.list_namespaced_job.return_value.items = [MagicMock()]
        self.k8s.batch.list_namespaced_job.return_value.metadata.resource_version = (
            "123"
        )
        mock_stream.return_value = [{"type": "NOT DELETED", "object": MagicMock()}]

        with self.assertRaises(UnexpectedStopIteration):
            self.k8s.wait_for_job_deletion("test-job")
