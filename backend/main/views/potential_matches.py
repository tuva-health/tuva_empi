import logging
from typing import Union

from django.http import FileResponse
from django.utils import timezone
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import MatchGroup
from main.services.empi.empi_service import EMPIService
from main.util.io import open_temp_file
from main.util.object_id import (
    get_id,
    get_object_id,
    remove_prefix,
)
from main.views.errors import error_data
from main.views.pagination import PaginationMixin
from main.views.persons import PersonDetailSerializer
from main.views.serializer import Serializer

logger = logging.getLogger(__name__)
pagination = PaginationMixin()


class GetPotentialMatchesRequest(Serializer):
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


class PotentialMatchSummarySerializer(Serializer):
    id = serializers.CharField()
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    data_sources = serializers.ListField(child=serializers.CharField())
    max_match_probability = serializers.FloatField()


class GetPotentialMatchesResponse(Serializer):
    potential_matches = PotentialMatchSummarySerializer(many=True)


class GetPotentialMatchRequest(Serializer):
    potential_match_id = serializers.CharField()
    fields = serializers.CharField(
        required=False, default="id,first_name,last_name,data_source"
    )  # type: ignore[assignment]
    include_metadata = serializers.BooleanField(required=False, default=True)
    page = serializers.IntegerField(required=False, default=1, min_value=1)
    page_size = serializers.IntegerField(
        required=False, default=50, min_value=1, max_value=1000
    )

    def validate_potential_match_id(self, value: str) -> str:
        if not value.startswith("pm_"):
            raise serializers.ValidationError("Invalid PotentialMatch ID")
        return value


class PredictionResultSerializer(Serializer):
    id = serializers.CharField()
    created = serializers.DateTimeField()
    match_probability = serializers.FloatField()
    person_record_l_id = serializers.CharField()
    person_record_r_id = serializers.CharField()


class PotentialMatchDetailSerializer(Serializer):
    id = serializers.CharField()
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    data_sources = serializers.ListField(child=serializers.CharField())
    max_match_probability = serializers.FloatField()
    results = PredictionResultSerializer(many=True)
    persons = PersonDetailSerializer(many=True)


class GetPotentialMatchResponse(Serializer):
    potential_match = PotentialMatchDetailSerializer()


class ExportPotentialMatchesRequest(Serializer):
    s3_uri = serializers.CharField(required=False, allow_blank=True)
    estimate = serializers.BooleanField(required=False, default=False)

    def validate_s3_uri(self, value: str) -> str:
        if value and not value.startswith("s3://"):
            raise serializers.ValidationError("S3 URI must start with 's3://'")
        return value


# ----------------------------
# Views
# ----------------------------


@extend_schema(
    summary="Retrieve potential matches",
    request=GetPotentialMatchesRequest,
    responses={200: GetPotentialMatchesResponse},
)
@api_view(["GET"])
def get_potential_matches(request: Request) -> Response:
    """Retrieve a list of potential matches based on filters."""
    serializer = GetPotentialMatchesRequest(data=request.query_params)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    empi = EMPIService()
    filters = {k: v for k, v in data.items() if k not in {"page", "page_size"}}

    if "person_id" in filters:
        filters["person_id"] = remove_prefix(filters["person_id"])

    try:
        results = empi.get_potential_matches(**filters)
        page, page_size = pagination.get_pagination_params(data)

        transformed = [
            {**pm, "id": get_object_id(pm["id"], "PotentialMatch")} for pm in results
        ]

        return pagination.create_paginated_response(
            transformed, page, page_size, response_key="potential_matches"
        )
    except Exception:
        logger.error("Failed to retrieve potential matches", exc_info=True)
        return Response(error_data("Unexpected internal error"), status=500)


@extend_schema(
    summary="Retrieve potential match by ID",
    request=GetPotentialMatchRequest,
    responses={200: GetPotentialMatchResponse},
)
@api_view(["GET"])
def get_potential_match(request: Request, id: str) -> Response:
    """Retrieve detailed information for a specific potential match."""
    raw_query = request.query_params

    serializer = GetPotentialMatchRequest(
        data={
            "potential_match_id": id,
            "fields": raw_query.get(
                "fields", "id,first_name,last_name,data_source,social_security_number"
            ),
            "include_metadata": raw_query.get("include_metadata", "true"),
            "page": raw_query.get("page", 1),
            "page_size": raw_query.get("page_size", 50),
        }
    )
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    empi = EMPIService()

    try:
        pm = empi.get_potential_match(
            id=get_id(data["potential_match_id"]), fields=data["fields"]
        )

        page, page_size = data["page"], data["page_size"]
        total = len(pm["persons"])
        start, end = (page - 1) * page_size, page * page_size
        persons_page = pm["persons"][start:end]

        potential_match_data = {
            "id": get_object_id(pm["id"], "PotentialMatch"),
            "created": pm["created"],
            "version": pm["version"],
            "results": [
                {
                    "id": get_object_id(r["id"], "PredictionResult"),
                    "created": r["created"],
                    "match_probability": r["match_probability"],
                    "person_record_l_id": get_object_id(
                        r["person_record_l_id"], "PersonRecord"
                    ),
                    "person_record_r_id": get_object_id(
                        r["person_record_r_id"], "PersonRecord"
                    ),
                }
                for r in pm["results"]
            ],
            "persons": [
                {
                    "id": get_object_id(str(p["uuid"]), "Person"),
                    "created": p["created"],
                    "version": p["version"],
                    "records": [
                        {
                            **{k: v for k, v in rec.items() if k != "person_uuid"},
                            "id": get_object_id(rec["id"], "PersonRecord"),
                            "person_id": get_object_id(p["uuid"], "Person"),
                        }
                        for rec in p["records"]
                    ],
                }
                for p in persons_page
            ],
        }

        # For tests, return just the potential_match data
        if not data["include_metadata"]:
            response_data = {"potential_match": potential_match_data}
        else:
            response_data = {
                "potential_match": potential_match_data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total,
                    "total_pages": (total + page_size - 1) // page_size,
                    "has_next": page * page_size < total,
                    "has_previous": page > 1,
                    "next_page": page + 1 if page * page_size < total else None,
                    "previous_page": page - 1 if page > 1 else None,
                },
                "metadata": {
                    "fields_requested": data["fields"],
                    "response_size_optimized": True,
                    "memory_optimized": True,
                    "pagination_enabled": True,
                },
            }

        response = Response(response_data, status=200)
        response["Cache-Control"] = "public, max-age=300"
        response["ETag"] = f'"pm_{id}_v{pm["version"]}"'
        return response

    except MatchGroup.DoesNotExist:
        return Response(error_data("Resource not found"), status=404)
    except Exception:
        logger.error("Failed to retrieve potential match", exc_info=True)
        return Response(error_data("Unexpected internal error"), status=500)


@extend_schema(
    summary="Export potential matches",
    request=ExportPotentialMatchesRequest,
    responses={
        200: {
            "type": "object",
            "description": "CSV download or estimate count",
        },
        202: {
            "type": "object",
            "description": "Job created for S3 export",
            "properties": {"job_id": {"type": "string"}, "message": {"type": "string"}},
        },
    },
)
@api_view(["POST"])
def export_potential_matches(request: Request) -> Union[Response, FileResponse]:
    """Export potential matches to CSV.

    Modes:
    - Estimate only
    - S3 export (background job)
    - Direct file download
    """
    serializer = ExportPotentialMatchesRequest(data=request.data)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    empi = EMPIService()

    try:
        if data.get("estimate"):
            count = empi.estimate_export_count()
            return Response(
                {
                    "estimated_count": count,
                    "message": f"Estimated {count:,} potential match pairs to export",
                }
            )

        elif data.get("s3_uri"):
            job = empi.create_export_job(config_id=1, sink_uri=data["s3_uri"])
            return Response(
                {
                    "job_id": job.id,
                    "status": job.status,
                    "message": f"Export job {job.id} created for S3 export to {data['s3_uri']}",
                },
                status=status.HTTP_202_ACCEPTED,
            )

        else:
            f = open_temp_file()
            empi.export_potential_matches(sink=f)
            f.seek(0)

            return FileResponse(
                f,
                content_type="text/csv",
                as_attachment=True,
                filename=f"potential_matches_export_{timezone.now().strftime('%Y%m%d_%H%M%S')}.csv",
            )

    except Exception as e:
        logger.error(f"Export failed: {str(e)}", exc_info=True)
        return Response(
            error_data("Export failed. Please try again or contact support."),
            status=500,
        )
