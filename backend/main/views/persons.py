from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import Person
from main.services.empi.empi_service import (
    EMPIService,
)
from main.util.dict import select_keys
from main.util.object_id import (
    get_object_id,
    get_prefix,
    get_uuid,
    is_object_id,
    remove_prefix,
)
from main.views.errors import error_data
from main.views.serializer import Serializer


class GetPersonsRequest(Serializer):
    first_name = serializers.CharField(required=False)
    last_name = serializers.CharField(required=False)
    birth_date = serializers.CharField(required=False)
    person_id = serializers.CharField(required=False)
    source_person_id = serializers.CharField(required=False)
    data_source = serializers.CharField(required=False)


class PersonSummarySerializer(Serializer):
    id = serializers.CharField()
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    data_sources = serializers.ListField(child=serializers.CharField())


class GetPersonsResponse(Serializer):
    persons = PersonSummarySerializer(many=True)


@extend_schema(
    summary="Retrieve persons",
    request=GetPersonsRequest,
    responses={200: GetPersonsResponse},
)
@api_view(["GET"])
def get_persons(request: Request) -> Response:
    """Get/search for persons."""
    serializer = GetPersonsRequest(data=request.query_params)

    if serializer.is_valid(raise_exception=True):
        data = {**serializer.validated_data}
        empi = EMPIService()

        if data.get("person_id"):
            data["person_id"] = remove_prefix(data.get("person_id", ""))

        persons = empi.get_persons(**data)

        return Response(
            {
                "persons": [
                    {
                        "id": get_object_id(person["uuid"], "Person"),
                        "first_name": person["first_name"],
                        "last_name": person["last_name"],
                        "data_sources": person["data_sources"],
                    }
                    for person in persons
                ]
            },
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class GetPersonRequest(Serializer):
    person_id = serializers.CharField()

    def validate_person_id(self, value: str) -> str:
        if value.startswith(get_prefix("Person") + "_") and is_object_id(value, "uuid"):
            return value
        else:
            raise serializers.ValidationError("Invalid Person ID")


class PersonRecordSerializer(Serializer):
    id = serializers.CharField()
    person_id = serializers.CharField()
    created = serializers.DateTimeField()
    person_updated = serializers.DateTimeField()
    matched_or_reviewed = serializers.BooleanField()
    data_source = serializers.CharField()
    source_person_id = serializers.CharField()
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    sex = serializers.CharField()
    race = serializers.CharField()
    birth_date = serializers.CharField()
    death_date = serializers.CharField()
    social_security_number = serializers.CharField()
    address = serializers.CharField()
    city = serializers.CharField()
    state = serializers.CharField()
    zip_code = serializers.CharField()
    county = serializers.CharField()
    phone = serializers.CharField()


class PersonDetailSerializer(Serializer):
    id = serializers.CharField()
    created = serializers.DateTimeField()
    version = serializers.IntegerField()
    records = PersonRecordSerializer(many=True)


class GetPersonResponse(Serializer):
    person = PersonDetailSerializer()


@extend_schema(
    summary="Retrieve person by ID",
    request=GetPersonRequest,
    responses={200: GetPersonResponse},
)
@api_view(["GET"])
def get_person(request: Request, id: int) -> Response:
    """Get Person by ID."""
    serializer = GetPersonRequest(data={**request.query_params, "person_id": id})

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        try:
            person = empi.get_person(uuid=get_uuid(data["person_id"]))
        except Person.DoesNotExist:
            message = "Resource not found"

            return Response(error_data(message), status=status.HTTP_404_NOT_FOUND)

        return Response(
            {
                "person": {
                    "id": get_object_id(str(person["uuid"]), "Person"),
                    "created": person["created"],
                    "version": person["version"],
                    "records": [
                        {
                            **select_keys(record, record.keys() - {"person_uuid"}),
                            "id": get_object_id(record["id"], "PersonRecord"),
                            "person_id": get_object_id(record["person_uuid"], "Person"),
                        }
                        for record in person["records"]
                    ],
                }
            },
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
