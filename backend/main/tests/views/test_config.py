from typing import Any, Dict, List
from unittest.mock import patch

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient, APITestCase

from main.models import User, UserRole


class ConfigViewTests(APITestCase):
    def setUp(self) -> None:
        self.client = APIClient()
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
        self.valid_config_payload: Dict[str, Any] = {
            "splink_settings": self.valid_splink_settings,
            "potential_match_threshold": 0.8,
            "auto_match_threshold": 0.95,
        }
        self.url = reverse("create_config")

        user = User.objects.create(idp_user_id="1", role=UserRole.member.value)
        auth_patcher = patch(
            "main.views.auth.jwt.JwtAuthentication.authenticate",
            return_value=(user, None),
        )
        auth_patcher.start()
        self.addCleanup(auth_patcher.stop)

    def assertValidationError(self, response: Any) -> List[Dict[str, Any]]:
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        error_data = response.json().get("error", {})
        self.assertEqual("Validation failed", error_data.get("message"))
        details: List[Dict[str, Any]] = error_data.get("details", [])
        return details

    def assertErrorDetail(
        self, error_details: list[Dict[str, Any]], field: str | None, message: str
    ) -> None:
        matching_errors = [
            detail
            for detail in error_details
            if (field is None or detail.get("field", "").startswith(field))
            and message in detail.get("message", "")
        ]
        self.assertTrue(
            matching_errors,
            f"Expected error with message '{message}' for field '{field}'",
        )

    def test_create_config_success(self) -> None:
        response = self.client.post(self.url, self.valid_config_payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("config_id", response.data)

    def test_create_config_invalid_thresholds(self) -> None:
        payload = self.valid_config_payload.copy()
        payload["potential_match_threshold"] = 1.5

        response = self.client.post(self.url, payload, format="json")
        error_details = self.assertValidationError(response)
        self.assertErrorDetail(
            error_details,
            "potential_match_threshold",
            "Ensure this value is less than or equal to 1.",
        )

    def test_create_config_no_blocking_rules(self) -> None:
        payload = self.valid_config_payload.copy()
        payload["splink_settings"] = self.valid_splink_settings.copy()
        payload["splink_settings"]["blocking_rules_to_generate_predictions"] = []

        response = self.client.post(self.url, payload, format="json")
        error_details = self.assertValidationError(response)
        self.assertErrorDetail(
            error_details,
            "splink_settings.blocking_rules_to_generate_predictions",
            "At least one blocking rule is required",
        )

    def test_create_config_invalid_blocking_rule_type(self) -> None:
        payload = self.valid_config_payload.copy()
        payload["splink_settings"] = self.valid_splink_settings.copy()
        payload["splink_settings"]["blocking_rules_to_generate_predictions"] = [
            {"blocking_rule": []}
        ]

        response = self.client.post(self.url, payload, format="json")
        error_details = self.assertValidationError(response)
        self.assertErrorDetail(
            error_details,
            "splink_settings.blocking_rules_to_generate_predictions",
            "Not a valid string.",
        )
