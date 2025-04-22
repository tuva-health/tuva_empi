from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import UserRole
from main.services.identity.identity_service import IdentityService
from main.util.object_id import get_id, get_object_id, get_prefix, is_object_id
from main.views.auth.permissions import IsAdmin
from main.views.serializer import Serializer


class GetUsersRequest(Serializer):
    pass


class UserSummarySerializer(Serializer):
    id = serializers.CharField()
    email = serializers.EmailField()
    role = serializers.CharField()


class GetUsersResponse(Serializer):
    users = UserSummarySerializer(many=True)


@extend_schema(
    summary="Retrieve users",
    request=GetUsersRequest,
    responses={200: GetUsersResponse},
)
@api_view(["GET"])
@permission_classes([IsAdmin])
def get_users(request: Request) -> Response:
    """Get User objects."""
    serializer = GetUsersRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        users = IdentityService().get_users()

        return Response(
            {
                "users": [
                    {
                        "id": get_object_id(user.id, "User"),
                        "email": user.email,
                        "role": user.role,
                    }
                    for user in users
                ]
            },
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UpdateUserRoleRequest(Serializer):
    user_id = serializers.CharField()
    role = serializers.ChoiceField(
        choices=[role.value for role in UserRole], allow_null=True
    )

    def validate_user_id(self, value: str) -> str:
        if value.startswith(get_prefix("User") + "_") and is_object_id(value, "int"):
            return value
        else:
            raise serializers.ValidationError("Invalid User ID")


class UpdatedUserSerializer(Serializer):
    id = serializers.CharField()


class UpdateUserRoleResponse(Serializer):
    user = UpdatedUserSerializer()


@extend_schema(
    summary="Update user role",
    request=UpdateUserRoleRequest,
    responses={200: UpdateUserRoleResponse},
)
@api_view(["POST"])
@permission_classes([IsAdmin])
def update_user(request: Request, id: int) -> Response:
    """Update User role."""
    serializer = UpdateUserRoleRequest(data={**request.data, "user_id": id})

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data

        # User role is the only thing that can be updated
        IdentityService().update_user_role(
            get_id(data["user_id"]), UserRole(data["role"]) if data["role"] else None
        )

        # TODO: Ideally return the full user
        return Response(
            {"user": {"id": data["user_id"]}},
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
