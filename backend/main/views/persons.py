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
from main.views.pagination import PaginationMixin
from main.views.serializer import Serializer


class GetPersonsRequest(Serializer):
    first_name = serializers.CharField(required=False)
    last_name = serializers.CharField(required=False)
    birth_date = serializers.CharField(required=False)
    person_id = serializers.CharField(required=False)
    source_person_id = serializers.CharField(required=False)
    data_source = serializers.CharField(required=False)
    page = serializers.IntegerField(required=False, default=1, min_value=1)
    page_size = serializers.IntegerField(
        required=False, default=50, min_value=1, max_value=1000
    )


class PersonSummarySerializer(Serializer):
    id = serializers.CharField()
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    data_sources = serializers.ListField(child=serializers.CharField())


class GetPersonsResponse(Serializer):
    persons = PersonSummarySerializer(many=True)


class GetPersonRequest(Serializer):
    person_id = serializers.CharField()

    def validate_person_id(self, value: str) -> str:
        if value.startswith(get_prefix("Person") + "_") and is_object_id(value, "uuid"):
            return value
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
    summary="Retrieve persons",
    request=GetPersonsRequest,
    responses={200: GetPersonsResponse},
)
@api_view(["GET"])
def get_persons(request: Request) -> Response:
    """Retrieve a paginated list of persons based on optional filters."""
    pagination = PaginationMixin()
    serializer = GetPersonsRequest(data=request.query_params)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    empi = EMPIService()

    filters = {k: v for k, v in data.items() if k not in {"page", "page_size"}}
    if "person_id" in filters:
        filters["person_id"] = remove_prefix(filters["person_id"])

    try:
        persons = empi.get_persons(**filters)
    except Exception:
        return Response(
            error_data("Unexpected internal error"),
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    page, page_size = pagination.get_pagination_params(data)

    results = [
        {
            "id": get_object_id(p["uuid"], "Person"),
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "data_sources": p["data_sources"],
        }
        for p in persons
    ]

    return pagination.create_paginated_response(
        results, page, page_size, response_key="persons"
    )


@extend_schema(
    summary="Retrieve person by ID",
    request=GetPersonRequest,
    responses={200: GetPersonResponse},
)
@api_view(["GET"])
def get_person(request: Request, id: str) -> Response:
    """Retrieve a specific person and their records by person ID."""
    serializer = GetPersonRequest(data={**request.query_params, "person_id": id})
    serializer.is_valid(raise_exception=True)

    empi = EMPIService()
    person_id = get_uuid(serializer.validated_data["person_id"])

    try:
        person = empi.get_person(uuid=person_id)
    except Person.DoesNotExist:
        return Response(
            error_data("Resource not found"), status=status.HTTP_404_NOT_FOUND
        )
    except Exception:
        return Response(
            error_data("Unexpected internal error"),
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    response_data = {
        "person": {
            "id": get_object_id(str(person["uuid"]), "Person"),
            "created": person["created"],
            "version": person["version"],
            "records": [
                {
                    **select_keys(record, set(record.keys()) - {"person_uuid"}),
                    "id": get_object_id(record["id"], "PersonRecord"),
                    "person_id": get_object_id(record["person_uuid"], "Person"),
                }
                for record in person["records"]
            ],
        }
    }

    return Response(response_data, status=status.HTTP_200_OK)
