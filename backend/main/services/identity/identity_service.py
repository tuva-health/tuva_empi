import logging
from dataclasses import dataclass
from typing import Optional, TypedDict, cast

from django.db import transaction
from django.utils import timezone

from main.config import AwsCognitoConfig, IdpBackend, KeycloakConfig, get_config
from main.models import User, UserRole
from main.services.identity.cognito_provider import CognitoIdentityProvider
from main.services.identity.identity_provider import IdentityProvider, IdpUser
from main.services.identity.keycloak_provider import KeycloakIdentityProvider


@dataclass
class UserWithMetadata:
    id: int
    email: str
    role: Optional[UserRole]
    idp_user_id: str


class JwtConfigDict(TypedDict):
    jwt_header: str
    jwks_url: str
    client_id: str
    jwt_aud: Optional[str]


class UserAlreadyExists(Exception):
    """User already exists."""


class IdentityService:
    logger: logging.Logger

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def sync_users(self, idp_users: list[IdpUser]) -> None:
        """Sync existing users from identity provider with Tuva EMPI users."""
        # FIXME: Remove users if they are no longer returned from IDP
        # Ideally, poll and sync users regularly
        for idp_user in idp_users:
            user, created = User.objects.get_or_create(idp_user_id=idp_user.id)

            if created:
                self.logger.info(f"Added user {user.id}")

    def _get_identity_provider(self) -> IdentityProvider:
        idp: CognitoIdentityProvider | KeycloakIdentityProvider
        config = get_config()
        backend = config.idp.backend

        if backend == IdpBackend.aws_cognito:
            idp = CognitoIdentityProvider()
        elif backend == IdpBackend.keycloak:
            idp = KeycloakIdentityProvider()
        else:
            raise Exception("IDP backend required")

        return idp

    def _get_identity_provider_config(
        self,
    ) -> tuple[IdpBackend, AwsCognitoConfig | KeycloakConfig]:
        idp_config: AwsCognitoConfig | KeycloakConfig
        config = get_config()
        backend = config.idp.backend

        if backend == IdpBackend.aws_cognito:
            assert config.idp.aws_cognito
            idp_config = config.idp.aws_cognito
        elif backend == IdpBackend.keycloak:
            assert config.idp.keycloak
            idp_config = config.idp.keycloak
        else:
            raise Exception("IDP backend required")

        return backend, idp_config

    def get_users(self) -> list[UserWithMetadata]:
        """Get Tuva EMPI users."""
        idp = self._get_identity_provider()
        idp_users = idp.get_users()
        idp_users_by_id: dict[str, IdpUser] = {}

        for idp_user in idp_users:
            assert idp_user.id not in idp_users_by_id
            idp_users_by_id[idp_user.id] = idp_user

        with transaction.atomic():
            self.sync_users(idp_users)

            return [
                UserWithMetadata(
                    id=user.id,
                    email=(
                        idp_users_by_id[user.idp_user_id].email
                        if user.idp_user_id in idp_users_by_id
                        else ""
                    ),
                    role=UserRole(user.role) if user.role else None,
                    idp_user_id=user.idp_user_id,
                )
                for user in User.objects.all()
            ]

    def update_user_role(self, user_id: int, role: Optional[UserRole]) -> None:
        """Update role for Tuva EMPI user."""
        self.logger.info("Adding user")

        User.objects.filter(id=user_id).update(
            role=role.value if role else None, updated=timezone.now()
        )

    def get_internal_user_by_idp_user_id(self, idp_user_id: str) -> User:
        """Get Tuva EMPI user (without email) by IDP user ID."""
        return User.objects.get(idp_user_id=idp_user_id)

    def get_jwt_config(self) -> JwtConfigDict:
        idp_backend, idp_config = self._get_identity_provider_config()

        return JwtConfigDict(
            jwt_header=idp_config.jwt_header,
            jwks_url=idp_config.jwks_url,
            client_id=idp_config.client_id,
            jwt_aud=(
                cast(KeycloakConfig, idp_config).jwt_aud
                if idp_backend == IdpBackend.keycloak
                else None
            ),
        )
