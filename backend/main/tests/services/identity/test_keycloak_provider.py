from unittest.mock import Mock, patch

from django.test import TestCase

from main.services.identity.keycloak_provider import KeycloakIdentityProvider


class KeycloakIdentityProviderTests(TestCase):
    @patch("main.util.keycloak.KeycloakClient.list_users")
    def test_get_users(self, mock_list_users: Mock) -> None:
        mock_list_users.return_value = [{"id": "user-123", "email": "test@example.com"}]

        provider = KeycloakIdentityProvider()
        users = provider.get_users()

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, "user-123")
        self.assertEqual(users[0].email, "test@example.com")
