import logging
from functools import lru_cache
from typing import Any, Optional, cast

import jwt
import requests
from django.core.exceptions import PermissionDenied
from rest_framework import authentication
from rest_framework.request import Request

from main.config import get_config
from main.models import User
from main.services.identity.identity_service import IdentityService, UserWithMetadata

LOGGER = logging.getLogger(__name__)


def extract_token_from_request(request: Request, jwt_header_name: str) -> Optional[str]:
    """Extracts JWT from the Authorization header."""
    LOGGER.info(f"Extracting JWT from request header {jwt_header_name}")

    jwt_header_value = request.headers.get(jwt_header_name, None)

    if (
        jwt_header_name == "Authorization"
        and jwt_header_value
        and jwt_header_value.startswith("Bearer ")
    ):
        return jwt_header_value.split(" ")[1]

    return jwt_header_value


@lru_cache(maxsize=32)
def get_key_for_kid(jwks_url: str, kid: str) -> Any:
    LOGGER.info(f"Retrieving JWKS from {jwks_url}")

    response = requests.get(jwks_url)
    response.raise_for_status()
    jwks = response.json()
    keys = jwks.get("keys", [])

    for jwk in keys:
        if jwk.get("kid") == kid and jwk.get("kty") == "RSA":
            return jwt.algorithms.RSAAlgorithm.from_jwk(jwk)

    raise ValueError(f"No matching key found for kid: {kid}")


def decode_jwt(jwks_url: str, token: str) -> Any:
    """Decodes (and verifies) a JWT."""
    LOGGER.info("Decoding JWT")

    headers = jwt.get_unverified_header(token)
    kid: Optional[str] = headers.get("kid")

    if not kid:
        raise ValueError("No 'kid' found in token header")

    key = get_key_for_kid(jwks_url, kid)

    return jwt.decode(token, key=key, algorithms=["RS256"])


def get_jwt_payload(
    request: Request, jwt_header_name: str, jwks_url: str
) -> dict[str, Any]:
    LOGGER.info(f"Getting JWT payload. jwks_url={jwks_url}")

    token = extract_token_from_request(request, jwt_header_name)

    if not token:
        raise PermissionDenied("Missing JWT")

    try:
        payload = decode_jwt(jwks_url, token)

        if not isinstance(payload, dict):
            raise PermissionDenied("Expected JWT payload to be object")

        return cast(dict[str, Any], payload)
    except Exception as err:
        LOGGER.exception(f"Failed to validate JWT: {err}")
        raise PermissionDenied("Invalid JWT")


class JwtAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request: Request) -> Optional[tuple[UserWithMetadata, Any]]:
        """Assigns User to request from JWT.

        Does not verify JWT. That should be done by external identity provider
        before the application receives the request.

        Raises PermissionDenied if User cannot be found or there is an issue with
        the JWT.
        """
        config = get_config()
        jwt_header = config["idp"]["jwt_header"]
        jwks_url = config["idp"]["jwks_url"]
        client_id = config["idp"]["client_id"]
        payload = get_jwt_payload(request, jwt_header, jwks_url)

        try:
            if payload.get("client_id") != client_id:
                raise ValueError("Unexpected client_id claim")

            LOGGER.info("Found JWT payload, retrieving user by sub")

            user = IdentityService().get_user_by_idp_user_id(payload["sub"])

            return (user, None)
        except User.DoesNotExist:
            LOGGER.exception("User does not exist")
            raise PermissionDenied("You do not have permission to perform this action.")
        except Exception as err:
            LOGGER.exception(f"Unexpected authentication error: {err}")
            raise PermissionDenied("You do not have permission to perform this action.")
