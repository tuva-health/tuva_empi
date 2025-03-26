import logging
from typing import TYPE_CHECKING, Type

from rest_framework import permissions
from rest_framework.request import Request

from main.models import UserRole
from main.services.identity.identity_service import UserWithMetadata

# There is a circular dependency issue with DRF because we use IsMember
# as a default permission class. Below we type `view` using "APIView".
# There might be a more modern best-practice for situations like this.
if TYPE_CHECKING:
    from rest_framework.views import APIView

LOGGER = logging.getLogger(__name__)


class IsAdmin(permissions.BasePermission):
    """Custom permission to only allow admins."""

    def has_permission(self, request: Request, view: "APIView") -> bool:
        LOGGER.info("Checking if request user is an admin")

        if isinstance(request.user, UserWithMetadata):
            return request.user.role == UserRole.admin
        else:
            return False


class IsMember(permissions.BasePermission):
    """Custom permission to only allow members."""

    def has_permission(self, request: Request, view: "APIView") -> bool:
        LOGGER.info("Checking if request user is a member")

        if isinstance(request.user, UserWithMetadata):
            return request.user.role == UserRole.member
        else:
            return False


class AnyOf(permissions.BasePermission):
    def __init__(self, *perms: Type[permissions.BasePermission]) -> None:
        self.perms = perms

    def has_permission(self, request: Request, view: "APIView") -> bool:
        return any(perm().has_permission(request, view) for perm in self.perms)


class IsMemberOrAdmin(AnyOf):
    def __init__(self) -> None:
        super().__init__(IsMember, IsAdmin)
