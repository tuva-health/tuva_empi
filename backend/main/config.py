import json
import os
import sys
from enum import Enum
from functools import cache
from typing import Any, Literal, Optional

from pydantic import BaseModel, ValidationError, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticCustomError
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class JsonConfigSettingsSource(PydanticBaseSettingsSource):
    @cache
    def _load_config(self) -> dict[str, Any]:
        with open("VERSION", "r") as f:
            version = f.read().strip()

        config_file = os.environ["TUVA_EMPI_CONFIG_FILE"]

        with open(config_file, "r") as f:
            print(f"Loading JSON config from file: {config_file}")

            config = json.load(f)
            config["version"] = version

            if "job_image" in config.get("matching_service", {}).get(
                "k8s_job_runner", {}
            ):
                # We don't accept job_image via config file since it's easy to let the matching service and
                # job versions get out of sync
                del config["matching_service"]["k8s_job_runner"]["job_image"]

            if not isinstance(config, dict):
                raise PydanticCustomError(
                    "invalid_json", "JSON config should be a dictionary"
                )
            return config

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        config = self._load_config()
        field_value = config.get(field_name)

        return field_value, field_name, False

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> dict[str, Any]:
        d: dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            d[field_key] = field_value

        return d


class DbConfig(BaseModel):
    user: str
    password: str
    name: str
    host: str
    port: str


class DjangoConfig(BaseModel):
    debug: bool
    secret_key: str
    allowed_hosts: list[str]
    cors_allowed_origins: list[str] = []


class IdpBackend(Enum):
    aws_cognito = "aws-cognito"
    keycloak = "keycloak"


class AwsCognitoConfig(BaseModel):
    cognito_user_pool_id: str
    jwt_header: str
    jwks_url: str
    client_id: str


class KeycloakConfig(BaseModel):
    server_url: str
    realm: str
    jwt_header: str
    jwks_url: str
    client_id: str
    client_secret: str
    jwt_aud: str


class IdpConfig(BaseModel):
    backend: IdpBackend
    aws_cognito: Optional[AwsCognitoConfig] = None
    keycloak: Optional[KeycloakConfig] = None

    @model_validator(mode="after")
    def validate_backend_specific_config(self) -> "IdpConfig":
        if self.backend == IdpBackend.aws_cognito and self.aws_cognito is None:
            raise PydanticCustomError(
                "missing",
                "aws_cognito config is required when backend is aws-cognito",
            )
        if self.backend == IdpBackend.keycloak and self.keycloak is None:
            raise PydanticCustomError(
                "missing",
                "keycloak config is required when backend is keycloak",
            )
        return self


class InitialSetupConfig(BaseModel):
    admin_email: str


class K8sJobRunnerSecretVolumeConfig(BaseModel):
    secret_name: str
    secret_key: str
    mount_path: str


class K8sJobRunnerConfig(BaseModel):
    job_image: str = ""
    job_image_pull_policy: Literal["Always", "IfNotPresent", "Never"]
    job_config_secret_volume: Optional[K8sJobRunnerSecretVolumeConfig] = None
    job_service_account_name: Optional[str] = None

    @model_validator(mode="after")
    def validate_job_image_if_needed(self) -> "K8sJobRunnerConfig":
        if "run_matching_service" in sys.argv and not self.job_image:
            raise PydanticCustomError(
                "missing",
                "job_image is required when running Matching Service with k8s job runner",
            )
        return self


class JobRunnerType(Enum):
    process = "process"
    k8s = "k8s"


class MatchingServiceConfig(BaseModel):
    job_runner: JobRunnerType
    k8s_job_runner: Optional[K8sJobRunnerConfig] = None

    @model_validator(mode="after")
    def validate_runner_specific_config(self) -> "MatchingServiceConfig":
        if self.job_runner == JobRunnerType.k8s and self.k8s_job_runner is None:
            raise ValueError("k8s_job_runner config is required when job_runner is k8s")
        return self


class AppConfig(BaseSettings):
    env: str
    version: str
    db: DbConfig
    django: DjangoConfig
    idp: IdpConfig
    initial_setup: InitialSetupConfig
    matching_service: MatchingServiceConfig

    model_config = SettingsConfigDict(
        env_prefix="TUVA_EMPI_", env_nested_delimiter="__"
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            env_settings,
            JsonConfigSettingsSource(settings_cls),
        )


@cache
def get_config() -> AppConfig:
    try:
        return AppConfig()
    except ValidationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
