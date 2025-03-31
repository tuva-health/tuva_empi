from rest_framework import serializers, status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from main.services.empi.empi_service import EMPIService
from main.util.object_id import get_object_id
from main.views.serializer import Serializer
from main.views.validators.splink_settings import SplinkSettingsSerializer


class CreateConfigRequest(Serializer):
    splink_settings = SplinkSettingsSerializer()
    potential_match_threshold = serializers.FloatField(min_value=0, max_value=1)
    auto_match_threshold = serializers.FloatField(min_value=0, max_value=1)


@api_view(["POST"])
def create_config(request: Request) -> Response:
    """Create Config object."""
    serializer = CreateConfigRequest(data=request.data)

    if serializer.is_valid(raise_exception=True):
        data = serializer.validated_data

        config = EMPIService().create_config(data)

        return Response(
            {"config_id": get_object_id(config.id, "Config")}, status=status.HTTP_200_OK
        )

    return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
