import re
from typing import Any, Mapping
from urllib.parse import urlparse

from django.http import FileResponse
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser, MultiPartParser
from rest_framework.request import Request
from rest_framework.response import Response

from main.services.empi.empi_service import (
    EMPIService,
    InvalidPersonRecordFileFormat,
)
from main.util.io import open_temp_file
from main.util.object_id import get_id, get_object_id, get_prefix, is_object_id
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
    s3_uri = serializers.CharField(required=False, allow_blank=False)
    file = serializers.FileField(required=False)
    config_id = serializers.CharField()

    def validate_config_id(self, value: str) -> str:
        if value.startswith(get_prefix("Config") + "_") and is_object_id(value, "int"):
            return value
        else:
            raise serializers.ValidationError("Invalid Config ID")

    def validate(self, data: Mapping[str, Any]) -> Mapping[str, Any]:
        if not data.get("s3_uri") and not data.get("file"):
            raise serializers.ValidationError("Must provide either 's3_uri' or 'file'.")
        if data.get("s3_uri") and data.get("file"):
            raise serializers.ValidationError(
                "Provide only one of 's3_uri' or 'file', not both."
            )
        return data


class ImportPersonRecordsResponse(Serializer):
    job_id = serializers.CharField()


@extend_schema(
    summary="Import person records",
    request=ImportPersonRecordsRequest,
    responses={200: ImportPersonRecordsResponse},
)
@api_view(["POST"])
@parser_classes([MultiPartParser, JSONParser])
def import_person_records(request: Request) -> Response:
    """Import person records from an S3 object."""
    serializer = ImportPersonRecordsRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        try:
            # FIXME: Check if config exists and return 400 error if not
            source = data.get("s3_uri", data.get("file"))
            config_id = get_id(data["config_id"])
            job_id = empi.import_person_records(source, config_id)
        except (FileNotFoundError, InvalidPersonRecordFileFormat) as e:
            return Response(
                validation_error_data(details=[str(e)]),
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            {"job_id": get_object_id(job_id, "Job")}, status=status.HTTP_200_OK
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ExportPersonRecordsRequest(S3URIValidatorMixin, Serializer):
    s3_uri = serializers.CharField(required=False, allow_blank=False)


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
def export_person_records(request: Request) -> Response | FileResponse:
    """Export person records to S3 in CSV format."""
    serializer = ExportPersonRecordsRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data
        empi = EMPIService()

        if s3_uri := data.get("s3_uri"):
            try:
                empi.export_person_records(s3_uri)

                return Response({}, status=status.HTTP_200_OK)
            # See: https://github.com/fsspec/s3fs/blob/main/s3fs/errors.py#L74-L79
            except FileNotFoundError as e:
                return Response(
                    validation_error_data(details=[str(e)]),
                    status=status.HTTP_400_BAD_REQUEST,
                )
        else:
            # TODO: It might be cleaner (and more performant) to have another method (e.g.
            # get_person_records) that returns a generator so that we can use StreamingHttpResponse.
            f = open_temp_file()
            empi.export_person_records(f)
            f.seek(0)

            return FileResponse(
                f,
                as_attachment=True,
                filename="person-record-export.csv",
                content_type="text/csv",
            )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
