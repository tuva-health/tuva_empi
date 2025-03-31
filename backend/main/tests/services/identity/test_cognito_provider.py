from unittest import TestCase
from unittest.mock import Mock, patch

from main.services.identity.cognito_provider import CognitoIdentityProvider
from main.util.cognito import CognitoAttributeName


class CognitoIdentityProviderTests(TestCase):
    @patch("main.util.cognito.CognitoClient.list_users")
    @patch("main.util.cognito.CognitoClient.__init__", return_value=None)
    def test_get_users(self, mock_init: Mock, mock_list_users: Mock) -> None:
        mock_user = {
            "Attributes": [
                {"Name": CognitoAttributeName.sub.value, "Value": "user-123"},
                {"Name": CognitoAttributeName.email.value, "Value": "test@example.com"},
            ]
        }
        mock_list_users.return_value = [mock_user]

        provider = CognitoIdentityProvider()

        users = provider.get_users()

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, "user-123")
        self.assertEqual(users[0].email, "test@example.com")
