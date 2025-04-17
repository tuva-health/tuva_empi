from drf_spectacular.utils import extend_schema
from rest_framework import status
from rest_framework.decorators import (
    api_view,
    authentication_classes,
    permission_classes,
)
from rest_framework.request import Request
from rest_framework.response import Response


@extend_schema(
    summary="Retrieve health check",
    responses={
        200: {
            "type": "object",
            "description": "Empty object",
            "properties": {},
        }
    },
)
@api_view(["GET"])
@authentication_classes([])
@permission_classes([])
def health_check(request: Request) -> Response:
    """Check the health of the API."""
    return Response({}, status=status.HTTP_200_OK)
