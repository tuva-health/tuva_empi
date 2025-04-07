import logging
from typing import Optional

from main.config import get_config
from main.services.identity.identity_provider import IdentityProvider, IdpUser
from main.util.keycloak import KeycloakClient


class KeycloakIdentityProvider(IdentityProvider):
    logger: logging.Logger
    keycloak: KeycloakClient

    def __init__(self, keycloak: Optional[KeycloakClient] = None) -> None:
        config = get_config()
        self.logger = logging.getLogger(__name__)
        self.keycloak = keycloak or KeycloakClient(
            server_url=config["idp"]["keycloak"]["server_url"],
            realm=config["idp"]["keycloak"]["realm"],
            client_id=config["idp"]["keycloak"]["client_id"],
            client_secret=config["idp"]["keycloak"]["client_secret"],
        )

    def get_users(self) -> list[IdpUser]:
        self.logger.info("Retrieving users from Keycloak IDP backend")

        keycloak_users = self.keycloak.list_users()

        return [
            IdpUser(
                id=keycloak_user["id"],
                email=keycloak_user["email"],
            )
            for keycloak_user in keycloak_users
        ]
