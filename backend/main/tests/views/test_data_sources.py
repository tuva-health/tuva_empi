from typing import Any
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse

from main.models import User, UserRole


class DataSourcesTestCase(TestCase):
    def setUp(self) -> None:
        user = User.objects.create(idp_user_id="1", role=UserRole.member.value)
        auth_patcher = patch(
            "main.views.auth.jwt.JwtAuthentication.authenticate",
            return_value=(user, None),
        )
        auth_patcher.start()
        self.addCleanup(auth_patcher.stop)

    @patch("main.views.data_sources.EMPIService")
    def test_get_data_sources_ok(self, mock_empi: Any) -> None:
        """Tests get_data_sources returns data sources."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_data_sources.return_value = [
            {"name": "test1"},
            {"name": "test2"},
        ]

        url = reverse("get_data_sources")

        response = self.client.get(
            url,
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(), {"data_sources": [{"name": "test1"}, {"name": "test2"}]}
        )
