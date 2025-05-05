from unittest.mock import Mock, patch

from django.test import TestCase
from django.utils import timezone

from main.config import AppConfig, IdpBackend, IdpConfig, KeycloakConfig
from main.models import User, UserRole
from main.services.identity.identity_provider import IdpUser
from main.services.identity.identity_service import IdentityService


class IdentityServiceTests(TestCase):
    idp_users: list[IdpUser]

    def setUp(self) -> None:
        self.idp_users = [
            IdpUser(id="idp-1", email="user1@example.com"),
            IdpUser(id="idp-2", email="user2@example.com"),
        ]

    @patch("main.services.identity.identity_service.KeycloakIdentityProvider")
    @patch("main.services.identity.identity_service.get_config")
    def test_get_users_with_keycloak(
        self, mock_get_config: Mock, mock_keycloak: Mock
    ) -> None:
        """Test get_users returns Keycloak users if Keycloak backend is configured."""
        mock_get_config.return_value = AppConfig.model_construct(
            idp=IdpConfig.model_construct(
                backend=IdpBackend.keycloak,
            ),
        )
        mock_keycloak.return_value.get_users.return_value = self.idp_users

        users = IdentityService().get_users()

        self.assertEqual(len(users), 2)
        self.assertEqual(users[0].email, "user1@example.com")
        self.assertEqual(users[1].email, "user2@example.com")

        self.assertEqual(User.objects.count(), 2)

    @patch("main.services.identity.identity_service.get_config")
    def test_get_users_invalid_backend(self, mock_get_config: Mock) -> None:
        mock_get_config.return_value = AppConfig.model_construct(
            idp=IdpConfig.model_construct(
                backend="invalid-backend",  # type: ignore[arg-type]
            ),
        )

        with self.assertRaises(Exception) as ctx:
            IdentityService().get_users()

        self.assertEqual(str(ctx.exception), "IDP backend required")

    def test_sync_users_creates_and_does_not_duplicate(self) -> None:
        """Test sync_users creates new users in the DB and doesn't create duplicates."""
        IdentityService().sync_users(self.idp_users)

        self.assertEqual(User.objects.count(), 2)

        # Syncing again should not create new users
        IdentityService().sync_users(self.idp_users)

        self.assertEqual(User.objects.count(), 2)

    def test_update_user_role_sets_role_and_updated(self) -> None:
        """Test update_user_role updates a User's role and updated timestamp."""
        user = User.objects.create(idp_user_id="idp-3", role=None)
        self.assertIsNone(user.role)

        before = timezone.now()
        IdentityService().update_user_role(user.id, UserRole.admin)

        user.refresh_from_db()
        self.assertEqual(user.role, UserRole.admin)
        self.assertTrue(user.updated >= before)

    def test_get_internal_user_by_idp_user_id(self) -> None:
        """Test get_internal_user_by_idp_user_id retrieves a User by their idp_user_id field."""
        user = User.objects.create(idp_user_id="idp-4")
        fetched = IdentityService().get_internal_user_by_idp_user_id("idp-4")

        self.assertEqual(user, fetched)

    @patch("main.services.identity.identity_service.get_config")
    def test_get_jwt_config_keycloak(self, mock_get_config: Mock) -> None:
        """Test get_jwt_config gets the JWT config from the configured backend."""
        mock_get_config.return_value = AppConfig.model_construct(
            idp=IdpConfig.model_construct(
                backend=IdpBackend.keycloak,
                keycloak=KeycloakConfig.model_construct(  # type: ignore[call-arg]
                    jwt_header="Authorization",
                    jwks_url="https://example.com/jwks.json",
                    client_id="client-id",
                    jwt_aud="client-id",
                ),
            ),
        )

        config = IdentityService().get_jwt_config()

        self.assertEqual(config["jwt_header"], "Authorization")
        self.assertEqual(config["jwks_url"], "https://example.com/jwks.json")
        self.assertEqual(config["client_id"], "client-id")
        self.assertEqual(config["jwt_aud"], "client-id")

    @patch("main.services.identity.identity_service.get_config")
    def test_get_jwt_config_invalid_backend(self, mock_get_config: Mock) -> None:
        """Test get_jwt_config throws error if backend isn't configured."""
        mock_get_config.return_value = AppConfig.model_construct(
            idp=IdpConfig.model_construct(
                backend="invalid-backend",  # type: ignore[arg-type]
            ),
        )

        with self.assertRaises(Exception) as ctx:
            IdentityService().get_jwt_config()

        self.assertEqual(str(ctx.exception), "IDP backend required")
