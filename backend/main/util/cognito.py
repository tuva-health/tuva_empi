import logging
from enum import Enum
from typing import TypedDict

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class CognitoAttributeName(Enum):
    email = "email"
    sub = "sub"


class CognitoUserAttributeDict(TypedDict):
    Name: str
    Value: str


class CognitoUserDict(TypedDict):
    Username: str
    Attributes: list[CognitoUserAttributeDict]


class CognitoClient:
    def __init__(self) -> None:
        self.client = boto3.client("cognito-idp")

    def list_users_by_attr(
        self, user_pool_id: str, attr_name: CognitoAttributeName, attr_value: str
    ) -> list[CognitoUserDict]:
        try:
            logger.info(
                f"Fetching users from Cognito by {attr_name.value}: {attr_value}"
            )

            response = self.client.list_users(
                UserPoolId=user_pool_id,
                Filter=f'{attr_name.value} = "{attr_value}"',
            )

            return [
                CognitoUserDict(
                    Username=user["Username"], Attributes=user["Attributes"]
                )
                for user in response.get("Users", [])
            ]

        except ClientError as e:
            logger.error(f"Failed to fetch users by {attr_name.value}: {e}")
            raise

    def list_users(self, user_pool_id: str) -> list[CognitoUserDict]:
        try:
            logger.info("Fetching users from Cognito")

            response = self.client.list_users(
                UserPoolId=user_pool_id,
            )

            return [
                CognitoUserDict(
                    Username=user["Username"], Attributes=user["Attributes"]
                )
                for user in response.get("Users", [])
            ]

        except ClientError as e:
            logger.error(f"Failed to fetch users: {e}")
            raise

    @staticmethod
    def get_attr(cognito_user: CognitoUserDict, attr_name: CognitoAttributeName) -> str:
        sub_attrs = [
            attr
            for attr in cognito_user["Attributes"]
            if attr["Name"] == attr_name.value
        ]

        # This should never happen, attributes should be unique
        if len(sub_attrs) != 1:
            raise Exception(
                f"Failed to get {attr_name.value} from AWS Cognito user. Expected a single {attr_name.value} attribute",
                cognito_user,
            )

        return sub_attrs[0]["Value"]
