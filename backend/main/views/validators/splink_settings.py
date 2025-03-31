from typing import Any, Dict, List

from rest_framework import serializers

from main.views.serializer import Serializer


class ComparisonLevelSerializer(Serializer):
    sql_condition = serializers.CharField(required=True)
    label_for_charts = serializers.CharField(required=True)
    is_null_level = serializers.BooleanField(default=False)
    m_probability = serializers.FloatField(min_value=0, max_value=1, required=False)
    u_probability = serializers.FloatField(min_value=0, max_value=1, required=False)
    tf_adjustment_column = serializers.CharField(required=False)
    tf_adjustment_weight = serializers.FloatField(required=False)

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not data.get("is_null_level", False):
            if "m_probability" not in data or "u_probability" not in data:
                raise serializers.ValidationError(
                    "m_probability and u_probability required when is_null_level is false"
                )
        return data


class ComparisonSerializer(Serializer):
    output_column_name = serializers.CharField(required=True)
    comparison_description = serializers.CharField(required=True)
    comparison_levels = ComparisonLevelSerializer(many=True, required=True)

    def validate_comparison_levels(
        self, value: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not value:
            raise serializers.ValidationError(
                "At least one comparison level is required"
            )
        return value


class BlockingRuleSerializer(Serializer):
    blocking_rule = serializers.CharField(required=True)


class SplinkSettingsSerializer(Serializer):
    probability_two_random_records_match = serializers.FloatField(
        min_value=0, max_value=1, required=True
    )
    em_convergence = serializers.FloatField(min_value=0, required=True)
    max_iterations = serializers.IntegerField(min_value=1, required=True)
    blocking_rules_to_generate_predictions = BlockingRuleSerializer(
        many=True, required=True
    )
    comparisons = ComparisonSerializer(many=True, required=True, allow_empty=False)

    def validate_blocking_rules_to_generate_predictions(
        self, value: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        if not value:
            raise serializers.ValidationError("At least one blocking rule is required")
        return value
