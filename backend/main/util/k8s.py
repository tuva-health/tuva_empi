import json
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
from kubernetes.client.exceptions import ApiException  # type: ignore[import-untyped]
from kubernetes.config.config_exception import (  # type: ignore[import-untyped]
    ConfigException,
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
    phase: Optional[str]


class UnexpectedStopIteration(Exception):
    """A stream (generator) stopped unexpectedly."""


class K8sJobNotFound(Exception):
    """Could not find a K8s job."""


class K8sJobAlreadyExists(Exception):
    """K8s job already exists."""


class K8sJobClient:
    batch: client.BatchV1Api  # type: ignore[no-any-unimported]
    core: client.CoreV1Api  # type: ignore[no-any-unimported]
    namespace: str

    def __init__(self, namespace: str = "default") -> None:
        try:
            logger.info("Loading in-cluster config")
            config.load_incluster_config()
        except ConfigException:
            logger.info("Failed to load in-cluster config. Loading kube config")
            config.load_kube_config()

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
        image_pull_policy: Literal["Always", "IfNotPresent", "Never"] = "IfNotPresent",
        command: Optional[list[str]] = None,
        secret_volume: Optional[SecretVolume] = None,
        termination_grace_period_seconds: int = 0,
        parallelism: int = 1,
        completions: int = 1,
        backoff_limit: int = 0,
        env: dict[str, str] = {},
        service_account_name: Optional[str] = None,
    ) -> None:
        logger.info(f"Creating K8s job {job_name} in namespace {self.namespace}")

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
                        termination_grace_period_seconds=termination_grace_period_seconds,
                        containers=[
                            V1Container(
                                name=job_name,
                                image=image,
                                image_pull_policy=image_pull_policy,
                                command=command,
                                volume_mounts=volume_mounts,
                                env=[
                                    client.V1EnvVar(name=key, value=value)
                                    for key, value in env.items()
                                ],
                            )
                        ],
                        volumes=volumes,
                        service_account_name=service_account_name,
                    ),
                ),
                parallelism=parallelism,
                completions=completions,
                backoff_limit=backoff_limit,
            ),
        )

        try:
            self.batch.create_namespaced_job(namespace=self.namespace, body=job)
            logger.info(f"K8s job {job_name} successfully submitted")
        except ApiException as e:
            logger.error(f"Failed to submit K8s job {job_name}: {e}")
            if e.status == 409 and json.loads(e.body)["reason"] == "AlreadyExists":
                logger.error("K8s job already exists")
                raise K8sJobAlreadyExists("K8s job already exists") from e
            else:
                raise
        except Exception as e:
            logger.error(f"Failed to submit K8s job {job_name}: {e}")
            raise

    def get_job_pods(
        self,
        job_name: str,
    ) -> list[PodState]:
        """List K8s job pods.

        Args:
            job_name: Name of the K8s job

        Returns:
            List of pod states
        """
        logger.info(f"Retrieving K8s job {job_name} pod(s)")

        label_selector = f"job-name={job_name}"

        pod_list = self.core.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=label_selector,
        )

        return [
            PodState(
                cast(str, pod.metadata.name),
                cast(str, pod.status.phase) if pod.status else None,
            )
            for pod in pod_list.items
        ]

    def wait_for_job_pods(
        self,
        job_name: str,
        expected_count: int,
        expected_pod_phases: set[
            Literal["Pending", "Running", "Succeeded", "Failed", "Unknown"]
        ],
        timeout_seconds: int = 300,
    ) -> list[PodState]:
        """Wait for K8s job pods to meet expected conditions.

        Args:
            job_name: Name of the K8s job
            expected_count: Number of pods to wait for
            expected_pod_phases: Pod phases to wait for
            timeout_seconds: Timeout in seconds

        Returns:
            List of pod names that matched
        """
        logger.info(
            f"Waiting for K8s job {job_name} pod(s) to meet expected conditions:"
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
                    f"Found K8s job {job_name} pod {pod_name} with phase {pod_phase}"
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
                timeout_seconds=timeout_seconds,
            ):
                pod = event["object"]
                pod_name = cast(str, pod.metadata.name)
                pod_phase = cast(str, pod.status.phase) if pod.status else None

                # TODO: Should we check event type is in {added, modified}?
                logger.debug(f"Pod {pod_name} event: {event['type']}")

                if pod_phase in expected_pod_phases:
                    logger.info(
                        f"Found K8s job {job_name} pod {pod_name} with phase {pod_phase}"
                    )
                    pod_states[pod_name] = PodState(pod_name, pod_phase)

                    if len(pod_states) >= expected_count:
                        return list(pod_states.values())

                if time.time() - start > timeout_seconds:
                    raise TimeoutError(
                        f"Timed out waiting for K8s job {job_name} pod(s)"
                    )

            # TODO: Can stream end without throwing? If not, this might be unnecessary.
            if len(pod_states) < expected_count:
                raise TimeoutError(f"Timed out waiting for K8s job {job_name} pod(s)")
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
        """Wait for a K8s job to complete.

        Args:
            job_name: Name of the job

        Returns:
            JobCompleteStatus object
        """
        logger.info(f"Waiting for K8s job {job_name} to complete")

        field_selector = f"metadata.name={job_name}"
        job_list = self.batch.list_namespaced_job(
            namespace=self.namespace,
            field_selector=field_selector,
        )

        if not job_list.items:
            raise K8sJobNotFound(f"K8s job {job_name} not found")

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

                logger.debug(f"K8s job {job.metadata.name} event: {event['type']}")

                if job.status and (job.status.succeeded or job.status.failed):
                    return JobCompleteStatus(
                        succeeded=job.status.succeeded, failed=job.status.failed
                    )
        finally:
            w.stop()

        # TODO: Can stream end without throwing? This might be unnecessary.
        raise UnexpectedStopIteration("Unexpected end of stream")

    def get_pod_container_states(self, pod_name: str) -> list[ContainerState]:
        logger.info(f"Retrieving pod {pod_name} container states")

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

    def delete_job(self, job_name: str) -> None:
        """Delete a K8s job with foreground propagation.

        Args:
            job_name: The name of the job to delete.

        Raises:
            ApiException: if the deletion fails with anything other than 404.
        """
        logger.info(f"Deleting K8s job {job_name} in namespace {self.namespace}")

        try:
            self.batch.delete_namespaced_job(
                name=job_name,
                namespace=self.namespace,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
            )
        except ApiException as e:
            if e.status == 404:
                logger.info(f"K8s job {job_name} does not existing. Ignoring")
                return
            logger.error(f"Failed to delete K8s job {job_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to delete K8s job {job_name}: {e}")
            raise

    def wait_for_job_deletion(self, job_name: str) -> None:
        """Wait until the given K8s job is fully deleted.

        Args:
            job_name: The name of the job to wait for.

        Raises:
            ApiException: if job read fails for other reasons.
        """
        logger.info(f"Waiting for deletion of K8s job {job_name}")

        field_selector = f"metadata.name={job_name}"
        job_list = self.batch.list_namespaced_job(
            namespace=self.namespace,
            field_selector=field_selector,
        )

        if not job_list.items:
            logger.info(f"K8s job {job_name} not found. Assuming it's been deleted")
            return

        resource_version = job_list.metadata.resource_version
        w = watch.Watch()

        try:
            for event in w.stream(
                self.batch.list_namespaced_job,
                namespace=self.namespace,
                field_selector=field_selector,
                resource_version=resource_version,
            ):
                event_type = event["type"]

                logger.debug(f"K8s job {job_name} event: {event_type}")

                if event_type == "DELETED":
                    logger.info(f"K8s job {job_name} has been deleted")
                    return

        finally:
            w.stop()

        # TODO: Can stream end without throwing? This might be unnecessary.
        raise UnexpectedStopIteration("Unexpected end of stream")
