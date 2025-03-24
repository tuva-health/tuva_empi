from rest_framework import status
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import JSONParser
from rest_framework.request import Request
from rest_framework.response import Response

from main.services.mpi_engine.mpi_engine_service import (
    MPIEngineService,
)
from main.views.serializer import Serializer


class GetDataSourcesRequest(Serializer):
    pass


@api_view(["GET"])
@parser_classes([JSONParser])
def get_data_sources(request: Request) -> Response:
    """Get data sources."""
    serializer = GetDataSourcesRequest(data=request.query_params)

    if serializer.is_valid(raise_exception=True):
        mpi_engine = MPIEngineService()
        data_sources = mpi_engine.get_data_sources()

        return Response(
            {"data_sources": data_sources},
            status=status.HTTP_200_OK,
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
