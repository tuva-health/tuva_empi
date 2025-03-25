from rest_framework import serializers, status
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import MatchGroup
from main.services.empi.empi_service import (
    EMPIService,
)
from main.util.dict import select_keys
from main.util.object_id import (
    get_id,
    get_object_id,
    get_prefix,
    is_object_id,
    remove_prefix,
)
from main.views.errors import error_data
from main.views.serializer import Serializer


class GetPotentialMatchesRequest(Serializer):
    first_name = serializers.CharField(required=False)
    last_name = serializers.CharField(required=False)
    birth_date = serializers.CharField(required=False)
    person_id = serializers.CharField(required=False)
    source_person_id = serializers.CharField(required=False)
    data_source = serializers.CharField(required=False)


@api_view(["GET"])
@parser_classes([JSONParser])
def get_potential_matches(request: Request) -> Response:
    """Get/search for potential matches."""
    serializer = GetPotentialMatchesRequest(data=request.query_params)

    if serializer.is_valid(raise_exception=True):
        data = {**serializer.validated_data}
        empi = EMPIService()

        if data.get("person_id"):
            data["person_id"] = remove_prefix(data.get("person_id", ""))

        potential_matches = empi.get_potential_matches(**data)

        return Response(
            {
                "potential_matches": [
                    {
                        **potential_match,
                        "id": get_object_id(potential_match["id"], "PotentialMatch"),
                    }
                    for potential_match in potential_matches
                ]
            },
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class GetPotentialMatchRequest(Serializer):
    potential_match_id = serializers.CharField()

    def validate_potential_match_id(self, value: str) -> str:
        if value.startswith(get_prefix("PotentialMatch") + "_") and is_object_id(
            value, "int"
        ):
            return value
        else:
            raise serializers.ValidationError("Invalid PotentialMatch ID")


@api_view(["GET"])
@parser_classes([JSONParser])
def get_potential_match(request: Request, id: int) -> Response:
    """Get PotentialMatch by ID."""
    serializer = GetPotentialMatchRequest(
        data={**request.query_params, "potential_match_id": id}
    )

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        try:
            potential_match = empi.get_potential_match(
                id=get_id(data["potential_match_id"])
            )
        except MatchGroup.DoesNotExist:
            message = "Resource not found"

            return Response(error_data(message), status=status.HTTP_404_NOT_FOUND)

        return Response(
            {
                "potential_match": {
                    **potential_match,
                    "id": get_object_id(potential_match["id"], "PotentialMatch"),
                    "results": [
                        {
                            **result,
                            "id": get_object_id(result["id"], "PredictionResult"),
                            "person_record_l_id": get_object_id(
                                result["person_record_l_id"], "PersonRecord"
                            ),
                            "person_record_r_id": get_object_id(
                                result["person_record_r_id"], "PersonRecord"
                            ),
                        }
                        for result in potential_match["results"]
                    ],
                    "persons": [
                        {
                            "id": get_object_id(str(person["uuid"]), "Person"),
                            "created": person["created"],
                            "version": person["version"],
                            "records": [
                                {
                                    **select_keys(
                                        record, record.keys() - {"person_uuid"}
                                    ),
                                    "id": get_object_id(record["id"], "PersonRecord"),
                                    "person_id": get_object_id(
                                        record["person_uuid"], "Person"
                                    ),
                                }
                                for record in person["records"]
                            ],
                        }
                        for person in potential_match["persons"]
                    ],
                }
            },
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
