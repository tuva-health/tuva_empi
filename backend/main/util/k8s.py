import logging
import time
from dataclasses import dataclass
from typing import Generator, Literal, Optional, cast

from kubernetes import client, config, watch  # type: ignore[import-untyped]
from kubernetes.client import (  # type: ignore[import-untyped]
    V1Container,
    V1Job,
    V1JobSpec,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
)

logger = logging.getLogger(__name__)


@dataclass
class SecretVolume:
    secret_name: str
    secret_key: str
    mount_path: str


@dataclass
class JobCompleteStatus:
    succeeded: int
    failed: int


@dataclass
class ContainerWaitingState:
    message: str
    reason: str


@dataclass
class ContainerTerminatedState:
    exit_code: int
    finished_at: str
    reason: str
    started_at: str


@dataclass
class ContainerState:
    waiting: Optional[ContainerWaitingState]
    terminated: Optional[ContainerTerminatedState]


@dataclass
class PodState:
    name: str
    phase: str


class UnexpectedStopIteration(Exception):
    """A stream (generator) stopped unexpectedly."""

    pass


class K8sJobNotFound(Exception):
    """Could not find a k8s job."""

    pass


class K8sJobClient:
    batch: client.BatchV1Api  # type: ignore[no-any-unimported]
    core: client.CoreV1Api  # type: ignore[no-any-unimported]
    namespace: str

    def __init__(self, namespace: str = "default") -> None:
        config.load_incluster_config()

        self.batch = client.BatchV1Api()
        self.core = client.CoreV1Api()

        if namespace is not None:
            self.namespace = namespace
        else:
            self.namespace = self._get_current_namespace()

    @staticmethod
    def _get_current_namespace() -> str:
        try:
            with open(
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r"
            ) as f:
                return f.read().strip()
        except FileNotFoundError:
            return "default"

    def run_job(
        self,
        job_name: str,
        image: str,
        command: Optional[list[str]] = None,
        secret_volume: Optional[SecretVolume] = None,
        termination_grace_period_secords: int = 0,
        parallelism: int = 1,
        completions: int = 1,
        backoff_limit: int = 0,
        env: dict[str, str] = {},
    ) -> None:
        logger.info(f"Creating k8s job {job_name} in namespace {self.namespace}")

        volumes: client.V1Volume = []  # type: ignore[no-any-unimported]
        volume_mounts: client.V1VolumeMount = []  # type: ignore[no-any-unimported]

        if secret_volume:
            volumes.append(
                client.V1Volume(
                    name="secret-volume",
                    secret=client.V1SecretVolumeSource(
                        secret_name=secret_volume.secret_name
                    ),
                )
            )
            volume_mounts.append(
                client.V1VolumeMount(
                    name="secret-volume",
                    mount_path=secret_volume.mount_path,
                    read_only=True,
                )
            )

        job = V1Job(
            metadata=V1ObjectMeta(name=job_name),
            spec=V1JobSpec(
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels={"job-name": job_name}),
                    spec=V1PodSpec(
                        restart_policy="Never",
                        termination_grace_period_seconds=termination_grace_period_secords,
                        containers=[
                            V1Container(
                                name=job_name,
                                image=image,
                                # FIXME: Make configurable
                                image_pull_policy="Never",
                                command=command,
                                volume_mounts=volume_mounts,
                                env=[
                                    client.V1EnvVar(name=key, value=value)
                                    for key, value in env.items()
                                ],
                            )
                        ],
                        volumes=volumes,
                    ),
                ),
                parallelism=parallelism,
                completions=completions,
                backoff_limit=backoff_limit,
            ),
        )

        try:
            self.batch.create_namespaced_job(namespace=self.namespace, body=job)
            logger.info(f"Job {job_name} successfully submitted")
        except Exception as e:
            logger.error(f"Failed to submit k8s job {job_name}: {e}")
            raise

    def wait_for_job_pods(
        self,
        job_name: str,
        expected_count: int,
        expected_pod_phases: set[
            Literal["Pending", "Running", "Succeeded", "Failed", "Unknown"]
        ],
        timeout_seconds: int = 300,
    ) -> list[PodState]:
        """Wait for k8s job pods to meet expected conditions.

        Args:
            job_name: Name of the k8s job
            expected_count: Number of pods to wait for
            expected_pod_phases: Pod phases to wait for
            timeout_seconds: Timeout in seconds

        Returns:
            List of pod names that matched
        """
        logger.info(
            f"Waiting for k8s job {job_name} pods to meet expected conditions:"
            f" count={expected_count}, pod_phases={expected_pod_phases}"
        )

        label_selector = f"job-name={job_name}"
        pod_states: dict[str, PodState] = {}

        pod_list = self.core.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=label_selector,
        )

        for pod in pod_list.items:
            pod_name = cast(str, pod.metadata.name)
            pod_phase = cast(str, pod.status.phase) if pod.status else None

            if pod_phase in expected_pod_phases:
                logger.info(
                    f"Found k8s job {job_name} pod {pod_name} with phase {pod_phase}"
                )
                pod_states[pod_name] = PodState(pod_name, pod_phase)

        if len(pod_states) >= expected_count:
            return list(pod_states.values())

        pod_list_resource_version = pod_list.metadata.resource_version
        start = time.time()
        w = watch.Watch()

        try:
            for event in w.stream(
                self.core.list_namespaced_pod,
                namespace=self.namespace,
                label_selector=label_selector,
                resource_version=pod_list_resource_version,
            ):
                pod = event["object"]
                pod_name = cast(str, pod.metadata.name)
                pod_phase = cast(str, pod.status.phase) if pod.status else None

                # TODO: Should we check event type is in {added, modified}?
                logger.info(f"Pod {pod_name} event: {event['type']}")

                if pod_phase in expected_pod_phases:
                    logger.info(
                        f"Found k8s job {job_name} pod {pod_name} with phase {pod_phase}"
                    )
                    pod_states[pod_name] = PodState(pod_name, pod_phase)

                    if len(pod_states) >= expected_count:
                        return list(pod_states.values())

                if time.time() - start > timeout_seconds:
                    raise TimeoutError(f"Timed out waiting for k8s job {job_name} pods")

            # TODO: Can stream end without throwing? If not, this might be unnecessary.
            if len(pod_states) < expected_count:
                raise TimeoutError(f"Timed out waiting for k8s job {job_name} pods")
            else:
                return list(pod_states.values())
        finally:
            w.stop()

    def stream_pod_logs(self, pod_name: str) -> Generator[str, None, None]:
        """Stream pod logs.

        Args:
            pod_name: Name of the pod
        """
        logger.info(f"Streaming logs for pod {pod_name}")

        w = watch.Watch()

        try:
            for line in w.stream(
                self.core.read_namespaced_pod_log,
                name=pod_name,
                namespace=self.namespace,
                follow=True,
                # timestamps=True,
            ):
                yield line
        except Exception as e:
            logger.warning(f"Failed to stream logs for pod {pod_name}: {e}")
        finally:
            w.stop()

    def get_pod_logs(self, pod_name: str) -> str:
        """Get pod logs.

        Args:
            pod_name: Name of the pod
            stream: Either "All", "Stdout" or "Stderr"
        """
        logger.info(f"Retrieving logs for pod {pod_name}")

        return cast(
            str,
            self.core.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                # Returns 422 from kind when passing this. Would be nice to get only stderr logs
                # for error messages.
                # stream=stream,
                # timestamps=True,
            ),
        )

    def wait_for_job_completion(
        self,
        job_name: str,
    ) -> JobCompleteStatus:
        """Wait for a k8s job to complete.

        Args:
            job_name: Name of the Job

        Returns:
            JobCompleteStatus object

        Raises:
            TimeoutError: if the job does not complete in time
            RuntimeError: if job cannot be found or read
        """
        logger.info(f"Waiting for k8s job {job_name} to complete")

        field_selector = f"metadata.name={job_name}"
        job_list = self.batch.list_namespaced_job(
            namespace=self.namespace,
            field_selector=field_selector,
        )

        if not job_list.items:
            raise K8sJobNotFound(f"k8s job {job_name} not found")

        job = job_list.items[0]

        if job.status and (job.status.succeeded or job.status.failed):
            return JobCompleteStatus(
                succeeded=job.status.succeeded, failed=job.status.failed
            )

        resource_version = job_list.metadata.resource_version
        w = watch.Watch()

        try:
            for event in w.stream(
                self.batch.list_namespaced_job,
                namespace=self.namespace,
                field_selector=field_selector,
                resource_version=resource_version,
            ):
                job = event["object"]

                logger.debug(f"k8s job {job.metadata.name} event: {event['type']}")

                if job.status and (job.status.succeeded or job.status.failed):
                    return JobCompleteStatus(
                        succeeded=job.status.succeeded, failed=job.status.failed
                    )
        finally:
            w.stop()

        # TODO: Can stream end without throwing? This might be unnecessary.
        raise UnexpectedStopIteration("Unexpected end of stream")

    def get_pod_container_states(self, pod_name: str) -> list[ContainerState]:
        pod = self.core.read_namespaced_pod(name=pod_name, namespace=self.namespace)

        return [
            ContainerState(
                waiting=(
                    ContainerWaitingState(
                        message=status.state.waiting.message,
                        reason=status.state.waiting.reason,
                    )
                    if status.state and status.state.waiting
                    else None
                ),
                terminated=(
                    ContainerTerminatedState(
                        exit_code=status.state.terminated.exit_code,
                        reason=status.state.terminated.reason,
                        started_at=status.state.terminated.started_at,
                        finished_at=status.state.terminated.finished_at,
                    )
                    if status.state and status.state.terminated
                    else None
                ),
            )
            for status in (pod.status.container_statuses or [])
        ]
