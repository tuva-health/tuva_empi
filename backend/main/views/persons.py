from rest_framework import serializers, status
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import Person
from main.services.mpi_engine.mpi_engine_service import (
    MPIEngineService,
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


@api_view(["GET"])
@parser_classes([JSONParser])
def get_persons(request: Request) -> Response:
    """Get/search for persons."""
    serializer = GetPersonsRequest(data=request.query_params)

    if serializer.is_valid(raise_exception=True):
        data = {**serializer.validated_data}
        mpi_engine = MPIEngineService()

        if data.get("person_id"):
            data["person_id"] = remove_prefix(data.get("person_id", ""))

        persons = mpi_engine.get_persons(**data)

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


@api_view(["GET"])
@parser_classes([JSONParser])
def get_person(request: Request, id: int) -> Response:
    """Get Person by ID."""
    serializer = GetPersonRequest(data={**request.query_params, "person_id": id})

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        mpi_engine = MPIEngineService()

        try:
            person = mpi_engine.get_person(uuid=get_uuid(data["person_id"]))
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
