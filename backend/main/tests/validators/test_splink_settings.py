from typing import Any, Dict
from unittest import TestCase

from rest_framework.exceptions import ValidationError

from main.validators.splink_settings import (
    ComparisonLevelSerializer,
    ComparisonSerializer,
    SplinkSettingsSerializer,
)


class TestSplinkSettings(TestCase):
    def setUp(self) -> None:
        self.valid_splink_settings: Dict[str, Any] = {
            "probability_two_random_records_match": 0.001,
            "em_convergence": 0.001,
            "max_iterations": 10,
            "blocking_rules_to_generate_predictions": [
                {"blocking_rule": '(l."id" = r."id")'}
            ],
            "comparisons": [
                {
                    "output_column_name": "test",
                    "comparison_description": "test",
                    "comparison_levels": [
                        {
                            "sql_condition": "test",
                            "label_for_charts": "test",
                            "is_null_level": False,
                            "m_probability": 0.1,
                            "u_probability": 0.1,
                        }
                    ],
                }
            ],
        }

    def assertValidationError(self, serializer: Any, field: str, message: str) -> None:
        try:
            serializer.is_valid(raise_exception=True)
            self.fail(
                f"Expected ValidationError for field '{field}' with message '{message}'"
            )
        except ValidationError as e:
            error_detail = e.detail
            if isinstance(error_detail, dict):
                error_message = str(error_detail.get(field, error_detail))
            else:
                error_message = str(error_detail)
            self.assertIn(
                message,
                error_message,
                f"Expected message '{message}' in error for field '{field}'",
            )

    def test_valid_splink_settings(self) -> None:
        serializer = SplinkSettingsSerializer(data=self.valid_splink_settings)
        self.assertTrue(serializer.is_valid())

    def test_max_iterations_not_integer(self) -> None:
        data = self.valid_splink_settings.copy()
        data["max_iterations"] = "abc"
        serializer = SplinkSettingsSerializer(data=data)
        self.assertValidationError(
            serializer, "max_iterations", "A valid integer is required"
        )

    def test_comparison_level_without_probabilities(self) -> None:
        data = {
            "sql_condition": "test condition",
            "label_for_charts": "test label",
            "is_null_level": False,
        }

        serializer = ComparisonLevelSerializer(data=data)
        self.assertValidationError(
            serializer,
            "non_field_errors",
            "m_probability and u_probability required when is_null_level is false",
        )

    def test_comparison_without_levels(self) -> None:
        data = {
            "output_column_name": "test",
            "comparison_description": "test description",
            "comparison_levels": [],
        }

        serializer = ComparisonSerializer(data=data)
        self.assertValidationError(
            serializer, "comparison_levels", "At least one comparison level is required"
        )

    def test_settings_without_comparisons(self) -> None:
        data = {
            "probability_two_random_records_match": 0.001,
            "em_convergence": 0.001,
            "max_iterations": 10,
            "blocking_rules_to_generate_predictions": [
                {"blocking_rule": '(l."id" = r."id")'}
            ],
        }

        serializer = SplinkSettingsSerializer(data=data)
        self.assertValidationError(serializer, "comparisons", "This field is required")

    def test_settings_with_empty_comparisons(self) -> None:
        data = {
            "probability_two_random_records_match": 0.001,
            "em_convergence": 0.001,
            "max_iterations": 10,
            "blocking_rules_to_generate_predictions": [
                {"blocking_rule": '(l."id" = r."id")'}
            ],
            "comparisons": [],
        }

        serializer = SplinkSettingsSerializer(data=data)
        self.assertValidationError(
            serializer, "comparisons", "This list may not be empty."
        )

    def test_settings_without_blocking_rules(self) -> None:
        data = {
            "probability_two_random_records_match": 0.001,
            "em_convergence": 0.001,
            "max_iterations": 10,
            "blocking_rules_to_generate_predictions": [],
            "comparisons": [
                {
                    "output_column_name": "test",
                    "comparison_description": "test",
                    "comparison_levels": [
                        {
                            "sql_condition": "test",
                            "label_for_charts": "test",
                            "is_null_level": True,
                        }
                    ],
                }
            ],
        }

        serializer = SplinkSettingsSerializer(data=data)
        self.assertValidationError(
            serializer,
            "blocking_rules_to_generate_predictions",
            "At least one blocking rule is required",
        )
