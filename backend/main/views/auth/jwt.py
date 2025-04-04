import logging
from functools import lru_cache
from typing import Any, Optional, cast

import jwt
import requests
from rest_framework import authentication
from rest_framework.exceptions import AuthenticationFailed, PermissionDenied
from rest_framework.request import Request

from main.models import User
from main.services.identity.identity_service import IdentityService

LOGGER = logging.getLogger(__name__)


class InvalidClientIdClaim(Exception):
    """Invalid client_id claim in JWT payload."""


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


def decode_jwt(jwks_url: str, token: str, audience: Optional[str]) -> Any:
    """Decodes (and verifies) a JWT."""
    LOGGER.info("Decoding JWT")

    headers = jwt.get_unverified_header(token)
    kid: Optional[str] = headers.get("kid")

    if not kid:
        raise ValueError("No 'kid' found in token header")

    key = get_key_for_kid(jwks_url, kid)

    return jwt.decode(
        token,
        key=key,
        algorithms=["RS256"],
        audience=audience,
        options={"verify_aud": bool(audience)},
    )


def get_jwt_payload(
    request: Request, jwt_header_name: str, jwks_url: str, audience: Optional[str]
) -> dict[str, Any]:
    LOGGER.info(f"Getting JWT payload. jwks_url={jwks_url}")

    token = extract_token_from_request(request, jwt_header_name)

    if not token:
        raise AuthenticationFailed("Missing JWT")

    try:
        payload = decode_jwt(jwks_url=jwks_url, token=token, audience=audience)

        if not isinstance(payload, dict):
            raise AuthenticationFailed("Expected JWT payload to be object")

        return cast(dict[str, Any], payload)
    except AuthenticationFailed as err:
        LOGGER.exception(f"Failed to validate JWT: {err}")
        raise
    except Exception as err:
        LOGGER.exception(f"Failed to validate JWT: {err}")
        raise AuthenticationFailed("Invalid JWT")


class JwtAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request: Request) -> Optional[tuple[User, Any]]:
        """Assigns User to request from JWT."""
        identity_service = IdentityService()
        jwt_config = identity_service.get_jwt_config()
        payload = get_jwt_payload(
            request=request,
            jwt_header_name=jwt_config["jwt_header"],
            jwks_url=jwt_config["jwks_url"],
            audience=jwt_config["jwt_aud"],
        )

        try:
            payload_client_id = payload.get("client_id")

            if payload_client_id and payload_client_id != jwt_config["client_id"]:
                raise InvalidClientIdClaim("Invalid client_id claim")

            LOGGER.info("Found JWT payload, retrieving user by sub")

            # NOTE: We assume that the sub claim is a unique user ID
            user = IdentityService().get_internal_user_by_idp_user_id(payload["sub"])

            return (user, None)
        except User.DoesNotExist as err:
            LOGGER.exception("User does not exist")
            raise PermissionDenied(
                "You do not have permission to perform this action."
            ) from err
        except Exception as err:
            LOGGER.exception(f"Unexpected authentication error: {err}")
            raise PermissionDenied(
                "You do not have permission to perform this action."
            ) from err

    def authenticate_header(self, request: Request) -> str:
        return "Bearer"
