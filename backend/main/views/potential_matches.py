from typing import Any, Union

from django.http import FileResponse
from django.utils import timezone
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.models import MatchGroup
from main.services.empi.empi_service import (
    EMPIService,
)
from main.util.dict import select_keys
from main.util.io import open_temp_file
from main.util.object_id import (
    get_id,
    get_object_id,
    get_prefix,
    is_object_id,
    remove_prefix,
)
from main.views.errors import error_data
from main.views.persons import PersonDetailSerializer
from main.views.serializer import Serializer


class GetPotentialMatchesRequest(Serializer):
    first_name = serializers.CharField(required=False)
    last_name = serializers.CharField(required=False)
    birth_date = serializers.CharField(required=False)
    person_id = serializers.CharField(required=False)
    source_person_id = serializers.CharField(required=False)
    data_source = serializers.CharField(required=False)


class PotentialMatchSummarySerializer(Serializer):
    id = serializers.CharField()
    first_name = serializers.CharField()
    last_name = serializers.CharField()
    data_sources = serializers.ListField(child=serializers.CharField())
    max_match_probability = serializers.FloatField()


class GetPotentialMatchesResponse(Serializer):
    potential_matches = PotentialMatchSummarySerializer(many=True)


@extend_schema(
    summary="Retrieve potential matches",
    request=GetPotentialMatchesRequest,
    responses={200: GetPotentialMatchesResponse},
)
@api_view(["GET"])
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


@extend_schema(
    summary="Retrieve potential match by ID",
    request=GetPotentialMatchRequest,
    responses={200: GetPotentialMatchResponse},
)
@api_view(["GET"])
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


class ExportPotentialMatchesRequest(serializers.Serializer[dict[str, Any]]):
    """Request serializer for exporting potential matches."""

    s3_uri = serializers.CharField(required=False, allow_blank=True)
    estimate = serializers.BooleanField(required=False, default=False)

    def validate_s3_uri(self, value: str) -> str:
        """Validate S3 URI format."""
        if value and not value.startswith("s3://"):
            raise serializers.ValidationError("S3 URI must start with 's3://'")
        return value


@extend_schema(
    summary="Export potential matches",
    request=ExportPotentialMatchesRequest,
    responses={
        200: {
            "type": "object",
            "description": "CSV file download for direct export or estimate count",
            "properties": {},
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
    """Export potential matches to CSV format.

    This endpoint supports three modes:
    1. Estimate only (estimate=true) - returns estimated record count
    2. S3 export (s3_uri provided) - creates background job for S3 export
    3. Direct file download (no s3_uri) - returns CSV file directly

    Priority order:
    1. estimate=true (returns estimated count)
    2. s3_uri provided (creates background job)
    3. Default (direct file download)
    """
    serializer = ExportPotentialMatchesRequest(data=request.data)
    if not serializer.is_valid():
        return Response(
            {"error": {"message": "Validation failed", "details": serializer.errors}},
            status=status.HTTP_400_BAD_REQUEST,
        )

    s3_uri = serializer.validated_data.get("s3_uri", "").strip()
    estimate_only = serializer.validated_data.get("estimate", False)

    empi_service = EMPIService()

    try:
        # Mode 1: Estimate only
        if estimate_only:
            estimated_count = empi_service.estimate_export_count()
            return Response(
                {
                    "estimated_count": estimated_count,
                    "message": f"Estimated {estimated_count:,} potential match pairs to export",
                }
            )

        # Mode 2: S3 export (background job)
        elif s3_uri:
            # Create background job for S3 export
            job = empi_service.create_export_job(
                config_id=1,  # Default config ID
                sink_uri=s3_uri,
            )
            return Response(
                {
                    "job_id": job.id,
                    "status": job.status,
                    "message": f"Export job {job.id} created for S3 export to {s3_uri}",
                },
                status=status.HTTP_202_ACCEPTED,
            )

        # Mode 3: Direct file download
        else:
            # Use the existing utility for temporary file handling
            f = open_temp_file()
            empi_service.export_potential_matches(sink=f)
            f.seek(0)

            return FileResponse(
                f,
                content_type="text/csv",
                as_attachment=True,
                filename=f"potential_matches_export_{timezone.now().strftime('%Y%m%d_%H%M%S')}.csv",
            )

    except Exception as e:
        # Log the full error for debugging but don't expose it to the client
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Export failed: {str(e)}", exc_info=True)

        return Response(
            {
                "error": {
                    "message": "Export failed. Please try again or contact support if the issue persists.",
                }
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
