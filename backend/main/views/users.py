from rest_framework import serializers, status
from rest_framework.parsers import JSONParser
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from main.models import UserRole
from main.services.identity.identity_service import IdentityService
from main.util.object_id import get_object_id
from main.views.auth.permissions import IsAdmin
from main.views.serializer import Serializer


class GetUsersRequest(Serializer):
    pass


class CreateUserRequest(Serializer):
    idp_user_id = serializers.CharField()
    role = serializers.ChoiceField(choices=[role.value for role in UserRole])


class UserView(APIView):
    parser_classes = [JSONParser]
    permission_classes = [IsAdmin]

    # FIXME: Add "all" filter to retrieve all users, even those not added to Tuva EMPI yet
    def get_users(self, request: Request) -> Response:
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

    def add_user(self, request: Request) -> Response:
        """Add User to Tuva EMPI."""
        serializer = CreateUserRequest(data=request.data)

        if serializer.is_valid(raise_exception=True):
            data = serializer.validated_data

            user = IdentityService().add_user(
                {"idp_user_id": data["idp_user_id"], "role": data["role"]}
            )

            return Response(
                {"user": {"id": get_object_id(user.id, "User")}},
                status=status.HTTP_200_OK,
            )

        return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def get(self, request: Request) -> Response:
        return self.get_users(request)

    def post(self, request: Request) -> Response:
        return self.add_user(request)
