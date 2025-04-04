from unittest import TestCase
from unittest.mock import Mock, patch

from main.services.identity.cognito_provider import CognitoIdentityProvider
from main.util.cognito import CognitoAttributeName, CognitoClient


class CognitoIdentityProviderTests(TestCase):
    @patch("main.services.identity.cognito_provider.get_config")
    @patch("main.services.identity.cognito_provider.CognitoClient")
    def test_get_users(self, mock_cognito_client: Mock, mock_get_config: Mock) -> None:
        mock_user = {
            "Attributes": [
                {"Name": CognitoAttributeName.sub.value, "Value": "user-123"},
                {"Name": CognitoAttributeName.email.value, "Value": "test@example.com"},
            ]
        }
        mock_cognito_client.return_value.list_users.return_value = [mock_user]
        mock_cognito_client.return_value.get_attr = CognitoClient.get_attr
        mock_get_config.return_value = {
            "idp": {
                "backend": "aws-cognito",
                "aws_cognito": {
                    "cognito_user_pool_id": "test-user-pool-1234",
                },
            }
        }

        provider = CognitoIdentityProvider()

        users = provider.get_users()

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].id, "user-123")
        self.assertEqual(users[0].email, "test@example.com")

    @patch("main.services.identity.cognito_provider.get_config")
    @patch("main.services.identity.cognito_provider.CognitoClient")
    def test_get_users_empty(
        self, mock_cognito_client: Mock, mock_get_config: Mock
    ) -> None:
        mock_cognito_client.return_value.list_users.return_value = []
        mock_get_config.return_value = {
            "idp": {
                "backend": "aws-cognito",
                "aws_cognito": {
                    "cognito_user_pool_id": "test-user-pool-1234",
                },
            }
        }

        provider = CognitoIdentityProvider()

        users = provider.get_users()

        self.assertEqual(len(users), 0)

    @patch("main.services.identity.cognito_provider.get_config")
    @patch("main.services.identity.cognito_provider.CognitoClient")
    def test_get_users_exception(
        self, mock_cognito_client: Mock, mock_get_config: Mock
    ) -> None:
        mock_cognito_client.return_value.list_users.side_effect = Exception(
            "mocked exception"
        )
        mock_get_config.return_value = {
            "idp": {
                "backend": "aws-cognito",
                "aws_cognito": {
                    "cognito_user_pool_id": "test-user-pool-1234",
                },
            }
        }

        provider = CognitoIdentityProvider()

        with self.assertRaises(Exception):
            provider.get_users()
