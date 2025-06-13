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

        keycloak_config = config.idp.keycloak
        assert keycloak_config

        self.keycloak = keycloak or KeycloakClient(
            server_url=keycloak_config.server_url,
            realm=keycloak_config.realm,
            client_id=keycloak_config.client_id,
            client_secret=keycloak_config.client_secret,
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
