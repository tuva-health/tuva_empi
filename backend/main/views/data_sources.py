from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.services.empi.empi_service import (
    EMPIService,
)
from main.views.serializer import Serializer


class GetDataSourcesRequest(Serializer):
    pass


@api_view(["GET"])
def get_data_sources(request: Request) -> Response:
    """Get data sources."""
    serializer = GetDataSourcesRequest(data=request.query_params)

    if serializer.is_valid(raise_exception=True):
        empi = EMPIService()
        data_sources = empi.get_data_sources()

        return Response(
            {"data_sources": data_sources},
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
