import logging
import uuid
from typing import Any, NotRequired, Optional, TypedDict

from django.http import JsonResponse
from rest_framework import serializers, status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_exception_handler

logger = logging.getLogger(__name__)


class ErrorDataItem(TypedDict):
    field: NotRequired[str]
    message: str


class ErrorDataError(TypedDict):
    message: str
    details: NotRequired[list[ErrorDataItem]]


class ErrorData(TypedDict):
    error: ErrorDataError


def error_data(
    message: str, details: list[str] = [], field_details: dict[str, list[str]] = {}
) -> ErrorData:
    error_details: list[ErrorDataItem] = [{"message": e} for e in details]

    for field, error_messages in field_details.items():
        if field != "non_field_errors":
            for msg in error_messages:
                error_details.append({"field": field, "message": msg})
        else:
            for msg in error_messages:
                error_details.append({"message": msg})

    error: ErrorDataError = {"message": message}

    if error_details:
        error["details"] = error_details

    return {"error": error}


def validation_error_data(
    details: list[str] = [], field_details: dict[str, list[str]] = {}
) -> ErrorData:
    return error_data("Validation failed", details=details, field_details=field_details)


def exception_handler(exc: Exception, context: dict[str, Any]) -> Optional[Response]:
    response = drf_exception_handler(exc, context)

    if response is not None:
        if isinstance(exc, serializers.ValidationError):
            field_details: dict[str, list[str]] = {}

            def process_messages(field_prefix: str, messages: Any) -> None:
                # Handle primitive types
                if isinstance(messages, (str, int, float, bool)):
                    field_details[field_prefix] = [str(messages)]
                # Handle nested types
                elif isinstance(messages, (dict, list)):
                    items = (
                        list(enumerate(messages))
                        if isinstance(messages, list)
                        else list(messages.items())
                    )
                    for key, value in items:
                        if isinstance(value, (dict, list)):
                            # If the value is a nested type, we need to add the key to the prefix
                            new_prefix = (
                                f"{field_prefix}.{key}" if field_prefix else str(key)
                            )
                        else:
                            # If the value is a primitive type, we can just add it to the field details
                            new_prefix = field_prefix

                        # Recursively process the nested type
                        process_messages(new_prefix, value)

            for field, messages in response.data.items():
                process_messages(field, messages)
            response.data = validation_error_data(field_details=field_details)
        else:
            response.data = error_data(response.data["detail"])

    return response


def server_error(request: Request, *args: Any, **kwargs: Any) -> JsonResponse:
    id = uuid.uuid4()
    message = f"Unexpected internal error - id={id}"

    logger.error(message)

    return JsonResponse(
        error_data(message), status=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


def not_found(request: Request, *args: Any, **kwargs: Any) -> JsonResponse:
    message = "Resource not found"

    return JsonResponse(error_data(message), status=status.HTTP_404_NOT_FOUND)
