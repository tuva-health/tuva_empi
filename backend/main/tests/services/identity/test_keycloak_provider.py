from unittest.mock import Mock, patch

from django.test import TestCase

from main.services.identity.keycloak_provider import KeycloakIdentityProvider


class KeycloakIdentityProviderTests(TestCase):
    @patch("main.services.identity.keycloak_provider.get_config")
    @patch("main.services.identity.keycloak_provider.KeycloakClient")
    def test_get_users(self, mock_kc_client: Mock, mock_get_config: Mock) -> None:
        mock_kc_client.return_value.list_users.return_value = [
            {"id": "user-123", "email": "test@example.com"}
        ]
        mock_get_config.return_value = {
            "idp": {
                "backend": "keycloak",
                "keycloak": {
                    "server_url": "http://example.com",
                    "realm": "example-realm",
                    "client_id": "client-id",
                    "client_secret": "client-secret",
                },
            }
        }

        provider = KeycloakIdentityProvider()
        users = provider.get_users()

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, "user-123")
        self.assertEqual(users[0].email, "test@example.com")
