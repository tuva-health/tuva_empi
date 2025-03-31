from rest_framework import status
from rest_framework.decorators import (
    api_view,
    authentication_classes,
    permission_classes,
)
from rest_framework.request import Request
from rest_framework.response import Response


@api_view(["GET"])
@authentication_classes([])
@permission_classes([])
def health_check(request: Request) -> Response:
    """Check the health of the API."""
    return Response({}, status=status.HTTP_200_OK)
