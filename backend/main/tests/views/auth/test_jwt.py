from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from django.test import TestCase
from jwcrypto import jwk
from rest_framework.exceptions import AuthenticationFailed, PermissionDenied
from rest_framework.request import Request

from main.models import User
from main.services.identity.identity_service import JwtConfigDict
from main.views.auth.jwt import (
    JwtAuthentication,
    decode_jwt,
    extract_token_from_request,
    get_jwt_payload,
    get_key_for_kid,
)


def get_jwks(kid: str) -> tuple[dict[str, Any], bytes]:
    """Generates JWKS and private key PEM."""
    key = jwk.JWK.generate(kty="RSA", size=2048)

    key.update({"kid": kid, "alg": "RS256", "use": "sig"})

    public_jwk = key.export(private_key=False, as_dict=True)

    return ({"keys": [public_jwk]}, key.export_to_pem(private_key=True, password=None))  # type: ignore[arg-type]


class JwtAuthenticationTests(TestCase):
    jwt_config: JwtConfigDict
    user: User
    private_key_object: rsa.RSAPrivateKey
    private_key_pem: bytes
    jwks: dict[str, Any]
    token: str

    def setUp(self) -> None:
        self.jwt_config = {
            "jwt_header": "Authorization",
            "jwks_url": "https://example.com/.well-known/jwks.json",
            "client_id": "client-123",
            "jwt_aud": "client-123",
        }
        self.user = User.objects.create(idp_user_id="user-123")

        self.jwks, self.private_key_pem = get_jwks("kid1")

        self.token = jwt.encode(
            {
                "sub": self.user.idp_user_id,
                "client_id": self.jwt_config["client_id"],
                "aud": self.jwt_config["jwt_aud"],
                "exp": datetime.now(tz=timezone.utc) + timedelta(hours=1),
            },
            self.private_key_pem,
            algorithm="RS256",
            headers={"kid": self.jwks["keys"][0]["kid"]},
        )

        # get_key_for_kid is memoized so we need to reset that if we regenerate the key on each test
        get_key_for_kid.cache_clear()

    def test_extract_token_with_bearer(self) -> None:
        """extract_token_from_request should remove Bearer and return token when header is "Authorization"."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        token = extract_token_from_request(request, "Authorization")

        self.assertEqual(token, "abc.def.ghi")

    def test_extract_token_custom_header(self) -> None:
        """extract_token_from_request should return token from headers other than "Authorization"."""
        request = MagicMock(spec=Request)
        request.headers = {"X-Token": "abc.def.ghi"}

        token = extract_token_from_request(request, "X-Token")

        self.assertEqual(token, "abc.def.ghi")

    def test_extract_token_missing(self) -> None:
        """extract_token_from_request should return None if the header doesn't have a value."""
        request = MagicMock(spec=Request)
        request.headers = {}

        token = extract_token_from_request(request, "Authorization")

        self.assertIsNone(token)

    @patch("main.views.auth.jwt.decode_jwt")
    def test_get_jwt_payload_valid(self, mock_decode: Mock) -> None:
        """get_jwt_payload should return payload for valid token."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        mock_decode.return_value = {
            "sub": self.user.idp_user_id,
            "client_id": self.jwt_config["client_id"],
        }

        payload = get_jwt_payload(
            request,
            "Authorization",
            self.jwt_config["jwks_url"],
            self.jwt_config["jwt_aud"],
        )

        self.assertEqual(payload, mock_decode.return_value)

    @patch("main.views.auth.jwt.decode_jwt")
    def test_get_jwt_payload_missing_token(self, mock_decode: Mock) -> None:
        """get_jwt_payload should raise AuthenticationFailed if there is no token."""
        request = MagicMock(spec=Request)
        request.headers = {}

        mock_decode.return_value = None

        with self.assertRaises(AuthenticationFailed) as ctx:
            get_jwt_payload(
                request,
                "Authorization",
                self.jwt_config["jwks_url"],
                self.jwt_config["jwt_aud"],
            )

        self.assertEqual(str(ctx.exception.detail), "Missing JWT")

    @patch("main.views.auth.jwt.decode_jwt")
    def test_get_jwt_payload_non_dict(self, mock_decode: Mock) -> None:
        """get_jwt_payload should raise AuthenticationFailed if payload is not a dict."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        mock_decode.return_value = "not-a-dict"

        with self.assertRaises(AuthenticationFailed) as ctx:
            get_jwt_payload(
                request,
                "Authorization",
                self.jwt_config["jwks_url"],
                self.jwt_config["jwt_aud"],
            )

        self.assertEqual(str(ctx.exception.detail), "Expected JWT payload to be object")

    @patch("main.views.auth.jwt.decode_jwt")
    def test_get_jwt_payload_decode_error(self, mock_decode: Mock) -> None:
        """get_jwt_payload should raise AuthenticationFailed if decode_jwt raises an error."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        mock_decode.side_effect = ValueError("JWT decode error")

        with self.assertRaises(AuthenticationFailed) as ctx:
            get_jwt_payload(
                request,
                "Authorization",
                self.jwt_config["jwks_url"],
                self.jwt_config["jwt_aud"],
            )

        self.assertEqual(str(ctx.exception.detail), "Invalid JWT")

    @patch("main.services.identity.identity_service.IdentityService.get_jwt_config")
    @patch("main.views.auth.jwt.decode_jwt")
    def test_authenticate_success(
        self, mock_decode: Mock, mock_get_jwt_config: Mock
    ) -> None:
        """Method authenticate succeeds if token contains valid payload with existing user ID in sub."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        mock_get_jwt_config.return_value = self.jwt_config
        mock_decode.return_value = {
            "sub": self.user.idp_user_id,
            "client_id": self.jwt_config["client_id"],
        }

        user_tuple = JwtAuthentication().authenticate(request)

        if user_tuple is None:
            self.fail()

        user, auth = user_tuple

        self.assertEqual(user, self.user)
        self.assertIsNone(auth)

    @patch("main.services.identity.identity_service.IdentityService.get_jwt_config")
    @patch("main.views.auth.jwt.decode_jwt")
    def test_authenticate_invalid_client_id(
        self, mock_decode: Mock, mock_get_jwt_config: Mock
    ) -> None:
        """Method authenticate raises PermissionDenied if token payload contains invalid client_id claim."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        mock_get_jwt_config.return_value = self.jwt_config
        mock_decode.return_value = {
            "sub": self.user.idp_user_id,
            "client_id": "invalid",
        }

        with self.assertRaises(PermissionDenied):
            JwtAuthentication().authenticate(request)

    @patch("main.services.identity.identity_service.IdentityService.get_jwt_config")
    @patch("main.views.auth.jwt.decode_jwt")
    def test_authenticate_user_does_not_exist(
        self, mock_decode: Mock, mock_get_jwt_config: Mock
    ) -> None:
        """Method authenticate raises PermissionDenied if user does not exist."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "Bearer abc.def.ghi"}

        mock_get_jwt_config.return_value = self.jwt_config
        mock_decode.return_value = {
            "sub": "user-999",
            "client_id": self.jwt_config["client_id"],
        }

        with self.assertRaises(PermissionDenied):
            JwtAuthentication().authenticate(request)

    def test_authenticate_header(self) -> None:
        """Should return Bearer."""
        request = MagicMock(spec=Request)

        value = JwtAuthentication().authenticate_header(request)

        self.assertEqual(value, "Bearer")

    @patch("requests.get")
    def test_decode_jwt_valid(self, mock_get: Mock) -> None:
        """Tests decode_jwt with an actual JWT token."""
        mock_get.return_value.json.return_value = self.jwks
        mock_get.return_value.raise_for_status = lambda: None

        payload = decode_jwt(
            jwks_url=self.jwt_config["jwks_url"],
            token=self.token,
            audience=self.jwt_config["jwt_aud"],
        )

        self.assertEqual(payload["sub"], self.user.idp_user_id)
        self.assertEqual(payload["client_id"], self.jwt_config["client_id"])

    @patch("requests.get")
    @patch("main.services.identity.identity_service.IdentityService.get_jwt_config")
    def test_authenticate_with_valid_token(
        self, mock_get_config: Mock, mock_get: Mock
    ) -> None:
        """Tests authenticate with an actual JWT token."""
        mock_get_config.return_value = self.jwt_config
        mock_get.return_value.json.return_value = self.jwks
        mock_get.return_value.raise_for_status = lambda: None

        request = Mock(spec=Request)
        request.headers = {
            self.jwt_config["jwt_header"]: f"Bearer {self.token}",
        }

        user_tuple = JwtAuthentication().authenticate(request)

        if user_tuple is None:
            self.fail()

        user, _ = user_tuple

        self.assertEqual(user, self.user)
