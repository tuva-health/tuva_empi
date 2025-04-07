from unittest.mock import MagicMock

from django.test import TestCase
from rest_framework.request import Request
from rest_framework.views import APIView

from main.models import User, UserRole
from main.views.auth.permissions import IsAdmin, IsMember, IsMemberOrAdmin


class DummyView(APIView):
    pass


class PermissionTests(TestCase):
    def setUp(self) -> None:
        self.view = DummyView()

    def test_is_admin_permission_granted(self) -> None:
        """Tests IsAdmin returns True if the request User has an admin role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(
            idp_user_id="admin", role=UserRole.admin.value
        )

        self.assertTrue(IsAdmin().has_permission(request, self.view))

    def test_is_admin_non_admin(self) -> None:
        """Tests IsAdmin returns False if the request User doesn't have an admin role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(
            idp_user_id="member", role=UserRole.member.value
        )

        self.assertFalse(IsAdmin().has_permission(request, self.view))

    def test_is_admin_non_user_instance(self) -> None:
        """Tests IsAdmin returns False if the request User is not a User instance."""
        request = MagicMock(spec=Request)
        request.user = "not-a-user-instance"

        self.assertFalse(IsAdmin().has_permission(request, self.view))

    def test_is_member_permission_granted(self) -> None:
        """Tests IsMember returns True if the request User has a member role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(
            idp_user_id="member", role=UserRole.member.value
        )

        self.assertTrue(IsMember().has_permission(request, self.view))

    def test_is_member_non_member(self) -> None:
        """Tests IsMember returns False if the request User doesn't have a member role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(
            idp_user_id="admin", role=UserRole.admin.value
        )

        self.assertFalse(IsMember().has_permission(request, self.view))

    def test_is_member_non_user_instance(self) -> None:
        """Tests IsMember returns False if the request User is not a User instance."""
        request = MagicMock(spec=Request)
        request.user = "not-a-user-instance"

        self.assertFalse(IsMember().has_permission(request, self.view))

    def test_is_member_or_admin_grants_for_member(self) -> None:
        """Tests AnyOf returns True if the request User has member role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(
            idp_user_id="member", role=UserRole.member.value
        )

        self.assertTrue(IsMemberOrAdmin().has_permission(request, self.view))

    def test_is_member_or_admin_grants_for_admin(self) -> None:
        """Tests AnyOf returns True if the request User has admin role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(
            idp_user_id="admin", role=UserRole.admin.value
        )

        self.assertTrue(IsMemberOrAdmin().has_permission(request, self.view))

    def test_is_member_or_admin_denies_for_other_roles(self) -> None:
        """Tests AnyOf returns False if the request User doesn't have admin or member role."""
        request = MagicMock(spec=Request)
        request.user = User.objects.create(idp_user_id="unknown", role="other")

        self.assertFalse(IsMemberOrAdmin().has_permission(request, self.view))
