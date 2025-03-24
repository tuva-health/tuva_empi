from rest_framework import status
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser
from rest_framework.request import Request
from rest_framework.response import Response


@api_view(["GET"])
@parser_classes([JSONParser])
def health_check(request: Request) -> Response:
    """Check the health of the API."""
    return Response({}, status=status.HTTP_200_OK)
