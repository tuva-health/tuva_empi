from typing import Any, Mapping

from rest_framework import serializers


class Serializer(serializers.Serializer[Any]):
    def to_internal_value(self, data: Any) -> Any:
        """Check for unexpected fields before normal processing."""
        if isinstance(data, Mapping):
            unexpected_fields = set(data.keys()) - set(self.fields.keys())

            if unexpected_fields:
                raise serializers.ValidationError(
                    {field: "Unexpected field" for field in unexpected_fields}
                )

        return super().to_internal_value(data)
