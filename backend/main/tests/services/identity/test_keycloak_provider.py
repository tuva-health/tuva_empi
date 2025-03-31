from unittest.mock import Mock, patch

from django.test import TestCase

from main.services.identity.keycloak_provider import KeycloakIdentityProvider


class KeycloakIdentityProviderTests(TestCase):
    @patch("main.services.identity.identity_service.get_config")
    @patch("main.util.keycloak.KeycloakClient.list_users")
    @patch("main.util.keycloak.KeycloakClient.__init__", return_value=None)
    def test_get_users(
        self, mock_init: Mock, mock_list_users: Mock, mock_get_config: Mock
    ) -> None:
        mock_list_users.return_value = [{"id": "user-123", "email": "test@example.com"}]
        mock_get_config.return_value = {
            "idp": {
                "backend": "keycloak",
                "keycloak": {
                    "jwt_header": "Authorization",
                    "jwks_url": "https://example.com/jwks.json",
                    "client_id": "client-id",
                    "jwt_aud": "client-id",
                },
            }
        }

        provider = KeycloakIdentityProvider()
        users = provider.get_users()

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, "user-123")
        self.assertEqual(users[0].email, "test@example.com")
