import logging
from typing import Optional

from main.config import get_config
from main.services.identity.identity_provider import IdentityProvider, IdpUser
from main.util.cognito import CognitoAttributeName, CognitoClient


class CognitoIdentityProvider(IdentityProvider):
    logger: logging.Logger
    cognito: CognitoClient

    def __init__(self, cognito: Optional[CognitoClient] = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.cognito = cognito or CognitoClient()

    def get_users(self) -> list[IdpUser]:
        config = get_config()

        self.logger.info("Retrieving users from AWS Cognito IDP backend")
        cognito_user_pool_id = config["idp"]["aws_cognito"]["cognito_user_pool_id"]
        cognito_users = self.cognito.list_users(cognito_user_pool_id)

        return [
            IdpUser(
                id=self.cognito.get_attr(cognito_user, CognitoAttributeName.sub),
                email=self.cognito.get_attr(cognito_user, CognitoAttributeName.email),
            )
            for cognito_user in cognito_users
        ]
