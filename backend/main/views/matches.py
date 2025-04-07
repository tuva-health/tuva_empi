from typing import Any

from rest_framework import serializers, status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import MatchGroup, User
from main.services.empi.empi_service import (
    EMPIService,
    InvalidPersonUpdate,
    InvalidPotentialMatch,
    PersonUpdateDict,
)
from main.util.object_id import get_id, get_uuid
from main.views.errors import validation_error_data
from main.views.serializer import Serializer


class PersonRecordCommentSerializer(Serializer):
    person_record_id = serializers.CharField()
    comment = serializers.CharField()


class PersonUpdateSerializer(Serializer):
    id = serializers.CharField(required=False)
    version = serializers.IntegerField(required=False)
    new_person_record_ids = serializers.ListField(child=serializers.CharField())


class CreateMatchRequest(Serializer):
    potential_match_id = serializers.CharField()
    potential_match_version = serializers.IntegerField()
    person_updates = PersonUpdateSerializer(many=True)
    comments = PersonRecordCommentSerializer(many=True, required=False)


def get_person_update(update: Any) -> PersonUpdateDict:
    person_update: PersonUpdateDict = {
        "new_person_record_ids": [get_id(id) for id in update["new_person_record_ids"]],
    }
    if "id" in update:
        person_update["uuid"] = get_uuid(update["id"])
    if "version" in update:
        person_update["version"] = update["version"]

    return person_update


@api_view(["POST"])
def create_match(request: Request) -> Response:
    """Create a person record match."""
    serializer = CreateMatchRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        # We only expect User objects in request.user for protected endpoints
        assert isinstance(request.user, User)

        try:
            empi.match_person_records(
                potential_match_id=get_id(data["potential_match_id"]),
                potential_match_version=data["potential_match_version"],
                person_updates=[
                    get_person_update(update) for update in data["person_updates"]
                ],
                performed_by=request.user,
                comments=[
                    {
                        "person_record_id": get_id(comment["person_record_id"]),
                        "comment": comment["comment"],
                    }
                    for comment in data.get("comments", [])
                ],
            )
        except (
            MatchGroup.DoesNotExist,
            InvalidPotentialMatch,
            InvalidPersonUpdate,
        ) as e:
            return Response(
                validation_error_data(details=[str(e)]),
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response({}, status=status.HTTP_200_OK)

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
