import logging
from typing import TypedDict, cast

import requests

logger = logging.getLogger(__name__)


class KeycloakUserDict(TypedDict):
    id: str
    email: str


class KeycloakClient:
    base_url: str
    realm: str
    token_url: str
    admin_api_url: str
    client_id: str
    client_secret: str
    token: str

    def __init__(
        self,
        server_url: str,
        realm: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        self.base_url = server_url.rstrip("/")
        self.realm = realm
        self.token_url = f"{self.base_url}/realms/{realm}/protocol/openid-connect/token"
        self.admin_api_url = f"{self.base_url}/admin/realms/{realm}"
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self._get_access_token()

    def _get_access_token(self) -> str:
        logger.info(
            "Requesting Keycloak service account token using client credentials"
        )

        try:
            resp = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=10,
            )
            resp.raise_for_status()

            return cast(str, resp.json()["access_token"])

        except requests.RequestException as e:
            logger.error(f"Failed to get Keycloak token: {e}")
            raise

    def list_users(self, max_results: int = 100) -> list[KeycloakUserDict]:
        logger.info("Fetching users from Keycloak")

        headers = {"Authorization": f"Bearer {self.token}"}
        users: list[KeycloakUserDict] = []
        first = 0

        while True:
            try:
                resp = requests.get(
                    f"{self.admin_api_url}/users",
                    headers=headers,
                    params={"first": first, "max": max_results},
                    timeout=10,
                )
                resp.raise_for_status()
                batch = resp.json()

                if not batch:
                    break

                users.extend(
                    KeycloakUserDict(
                        id=u["id"],
                        email=u["email"],
                    )
                    for u in batch
                )
                first += max_results

            except requests.RequestException as e:
                logger.error(f"Failed to fetch users: {e}")
                raise

        return users
