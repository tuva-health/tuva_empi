from typing import Any
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse


class DataSourcesTestCase(TestCase):
    @patch("main.views.data_sources.MPIEngineService")
    def test_get_data_sources_ok(self, mock_mpi_engine: Any) -> None:
        """Tests get_data_sources returns data sources."""
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_data_sources.return_value = [
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
