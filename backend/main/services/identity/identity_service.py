import logging
from dataclasses import dataclass
from typing import Any, Optional, TypedDict

from django.db import transaction

from main.config import IdpBackend, get_config
from main.models import User, UserRole
from main.util.cognito import CognitoAttributeName, CognitoClient


@dataclass
class IdpUser:
    id: str
    email: str


class PartialUserDict(TypedDict):
    idp_user_id: str
    role: UserRole


@dataclass
class UserWithMetadata:
    id: int
    email: str
    role: UserRole


class UserAlreadyExists(Exception):
    """User already exists."""


class IdentityService:
    """This should likely be it's own completely separate application."""

    logger: logging.Logger
    cognito: CognitoClient

    def __init__(self, cognito: Optional[CognitoClient] = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.cognito = cognito or CognitoClient()

    def get_idp_user(self, idp_user_id: str) -> IdpUser:
        """Retrieves a user by their IDP ID.

        Throws an Exception if the user does not exist or more than one is returned.
        """
        config = get_config()

        if config["idp"]["backend"] == IdpBackend.aws_cognito.value:
            self.logger.info("Retrieving user from AWS Cognito IDP backend")
            cognito_user_pool_id = config["idp"]["aws_cognito_user_pool_id"]
            cognito_users = self.cognito.list_users_by_attr(
                cognito_user_pool_id, CognitoAttributeName.sub, idp_user_id
            )

            if not cognito_users:
                raise Exception(f"No user found with ID: {idp_user_id}")

            if len(cognito_users) > 1:
                raise Exception(
                    f"Expected to find a single user, found {len(cognito_users)}"
                )

            cognito_user = cognito_users[0]

            assert (
                self.cognito.get_attr(cognito_user, CognitoAttributeName.sub)
                == idp_user_id
            )

            return IdpUser(
                id=self.cognito.get_attr(cognito_user, CognitoAttributeName.sub),
                email=self.cognito.get_attr(cognito_user, CognitoAttributeName.email),
            )
        else:
            raise Exception("IDP backend required")

    def get_idp_users(self) -> list[IdpUser]:
        config = get_config()

        if config["idp"]["backend"] == IdpBackend.aws_cognito.value:
            self.logger.info("Retrieving users from AWS Cognito IDP backend")
            cognito_user_pool_id = config["idp"]["aws_cognito_user_pool_id"]
            cognito_users = self.cognito.list_users(cognito_user_pool_id)

            return [
                IdpUser(
                    id=self.cognito.get_attr(cognito_user, CognitoAttributeName.sub),
                    email=self.cognito.get_attr(
                        cognito_user, CognitoAttributeName.email
                    ),
                )
                for cognito_user in cognito_users
            ]
        else:
            raise Exception("IDP backend required")

    def add_user(self, user: PartialUserDict) -> User:
        """Add existing user from identity service to Tuva EMPI."""
        self.logger.info("Adding user")

        with transaction.atomic():
            idp_user = IdentityService().get_idp_user(user["idp_user_id"])

            if User.objects.filter(idp_user_id=idp_user.id).exists():
                raise UserAlreadyExists()

            user_added = User.objects.create(
                idp_user_id=idp_user.id,
                role=user["role"].value,
            )
            self.logger.info(f"Added user {user_added.id}")

            return user_added

    def get_users(self) -> list[UserWithMetadata]:
        """Get Tuva EMPI users.

        Retrive users from identity service and only return those users that have been
        added to Tuva EMPI.
        """
        idp_service = IdentityService()
        idp_users = idp_service.get_idp_users()
        users: list[UserWithMetadata] = []

        # FIXME: Soft-delete users if they are no longer returned from IDP or just don't return them
        for idp_user in idp_users:
            try:
                user = User.objects.get(idp_user_id=idp_user.id)
                users.append(
                    UserWithMetadata(
                        id=user.id,
                        email=idp_user.email,
                        role=UserRole(user.role),
                    )
                )
            except User.DoesNotExist:
                pass

        return users

    def get_user_by_idp_user_id(self, idp_user_id: str) -> UserWithMetadata:
        """Get Tuva EMPI user by IDP user ID."""
        idp_service = IdentityService()
        idp_user = idp_service.get_idp_user(idp_user_id)

        # FIXME: Soft-delete users if they are no longer returned from IDP or just don't return them
        user = User.objects.get(idp_user_id=idp_user.id)

        return UserWithMetadata(
            id=user.id,
            email=idp_user.email,
            role=UserRole(user.role),
        )

    def remove_user(self, **kwargs: Any) -> tuple[int, dict[str, int]]:
        """Remove user from Tuva EMPI."""
        deleted_count, deleted_details = User.objects.get(**kwargs).delete()

        if deleted_count == 0:
            raise Exception("Failed to remove user")

        return (deleted_count, deleted_details)
