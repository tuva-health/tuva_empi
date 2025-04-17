import re
from urllib.parse import urlparse

from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.services.empi.empi_service import (
    EMPIService,
    InvalidPersonRecordFileFormat,
)
from main.util.object_id import get_id, get_object_id, get_prefix, is_object_id
from main.util.s3 import ObjectDoesNotExist, UploadError
from main.views.errors import validation_error_data
from main.views.serializer import Serializer


class S3URIValidatorMixin:
    """Mixin for validating S3 URIs."""

    def validate_s3_uri(self, value: str) -> str:
        s3_uri_parsed = urlparse(value, allow_fragments=False)
        s3_bucket = s3_uri_parsed.netloc
        s3_key = s3_uri_parsed.path.lstrip("/")

        # "Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-)."
        # - https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
        if (
            value.startswith("s3://")
            and s3_uri_parsed.scheme == "s3"
            and re.fullmatch(r"[a-z0-9.-]+", s3_bucket)
            and s3_key
        ):
            return value
        else:
            raise serializers.ValidationError("Invalid S3 URI")


class ImportPersonRecordsRequest(S3URIValidatorMixin, Serializer):
    s3_uri = serializers.CharField()
    config_id = serializers.CharField()

    def validate_config_id(self, value: str) -> str:
        if value.startswith(get_prefix("Config") + "_") and is_object_id(value, "int"):
            return value
        else:
            raise serializers.ValidationError("Invalid Config ID")


class ImportPersonRecordsResponse(Serializer):
    job_id = serializers.CharField()


@extend_schema(
    summary="Import person records",
    request=ImportPersonRecordsRequest,
    responses={200: ImportPersonRecordsResponse},
)
@api_view(["POST"])
def import_person_records(request: Request) -> Response:
    """Import person records from an S3 object."""
    serializer = ImportPersonRecordsRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        try:
            # FIXME: Check if config exists and return 400 error if not
            job_id = empi.import_person_records(
                data["s3_uri"], get_id(data["config_id"])
            )
        except (ObjectDoesNotExist, InvalidPersonRecordFileFormat) as e:
            return Response(
                validation_error_data(details=[str(e)]),
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            {"job_id": get_object_id(job_id, "Job")}, status=status.HTTP_200_OK
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ExportPersonRecordsRequest(S3URIValidatorMixin, Serializer):
    s3_uri = serializers.CharField()


@extend_schema(
    summary="Export person records",
    request=ExportPersonRecordsRequest,
    responses={
        200: {
            "type": "object",
            "description": "Empty object",
            "properties": {},
        }
    },
)
@api_view(["POST"])
def export_person_records(request: Request) -> Response:
    """Export person records to S3 in CSV format."""
    serializer = ExportPersonRecordsRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        try:
            empi.export_person_records(data["s3_uri"])
        except UploadError as e:
            return Response(
                validation_error_data(details=[str(e)]),
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response({}, status=status.HTTP_200_OK)

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
