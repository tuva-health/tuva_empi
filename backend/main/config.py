import json
import os
from enum import Enum
from functools import cache
from typing import Literal, TypedDict, cast


class DbConfigDict(TypedDict):
    user: str
    password: str
    name: str
    host: str
    port: str


class DjangoConfigDict(TypedDict):
    debug: str
    secret_key: str
    allowed_hosts: list[str]


class IdpBackend(Enum):
    aws_cognito = "aws-cognito"
    keycloak = "keycloak"


class AwsCognitoConfigDict(TypedDict):
    cognito_user_pool_id: str
    jwt_header: str
    jwks_url: str
    client_id: str


class KeycloakConfigDict(TypedDict):
    server_url: str
    realm: str
    jwt_header: str
    jwks_url: str
    client_id: str
    client_secret: str
    jwt_aud: str


class IdpConfigDict(TypedDict):
    backend: Literal[IdpBackend.aws_cognito, IdpBackend.keycloak]
    aws_cognito: AwsCognitoConfigDict
    keycloak: KeycloakConfigDict


class InitialSetupConfigDict(TypedDict):
    admin_email: str


class ConfigDict(TypedDict):
    env: str
    db: DbConfigDict
    django: DjangoConfigDict
    idp: IdpConfigDict
    initial_setup: InitialSetupConfigDict


# FIXME: Add validation with DRF serializer or Pydantic model
@cache
def get_config() -> ConfigDict:
    with open(os.environ["TUVA_EMPI_BACKEND_CONFIG_FILE"], "r") as f:
        config = cast(ConfigDict, json.load(f))

        return config
