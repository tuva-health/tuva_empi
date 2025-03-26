import json
import os
from enum import Enum
from functools import cache
from typing import Literal, NotRequired, TypedDict, cast


class DbConfigDict(TypedDict):
    user: str
    password: str
    host: str
    port: str


class DjangoConfigDict(TypedDict):
    debug: str
    secret_key: str
    allowed_hosts: list[str]


class IdpBackend(Enum):
    aws_cognito = "aws-cognito"


class IdpConfigDict(TypedDict):
    backend: Literal[IdpBackend.aws_cognito]
    aws_cognito_user_pool_id: NotRequired[str]
    jwt_header: str
    jwks_url: str
    client_id: str


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
    with open(os.environ["CONFIG_FILE"], "r") as f:
        config = cast(ConfigDict, json.load(f))

        return config
