from typing import Any
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.urls import reverse

from main.models import User, UserRole
from main.services.empi.empi_service import InvalidPersonRecordFileFormat


class PersonRecordsTestCase(TestCase):
    def setUp(self) -> None:
        user = User.objects.create(idp_user_id="1", role=UserRole.member.value)
        auth_patcher = patch(
            "main.views.auth.jwt.JwtAuthentication.authenticate",
            return_value=(user, None),
        )
        auth_patcher.start()
        self.addCleanup(auth_patcher.stop)

    @patch("main.views.person_records.EMPIService")
    def test_import_validation_ok(self, mock_empi: Any) -> None:
        """Tests import_person_records request validation succeeds."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {"job_id": "job_1"})

    @patch("main.views.person_records.EMPIService")
    def test_import_validation_invalid_content_type(self, mock_empi: Any) -> None:
        """Tests import_person_records rejects content types other than application/json."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/unknown",
        )
        self.assertEqual(response.status_code, 415)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Unsupported media type "application/unknown" in request.',
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_validation_invalid_request_method(self, mock_empi: Any) -> None:
        """Tests import_person_records rejects request methods besides POST."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        response = self.client.get(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Method "GET" not allowed.',
                }
            },
        )

        response = self.client.delete(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Method "DELETE" not allowed.',
                }
            },
        )

        response = self.client.patch(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Method "PATCH" not allowed.',
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_validation_invalid_json(self, mock_empi: Any) -> None:
        """Tests import_person_records rejects request methods with invalid JSON."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        response = self.client.post(
            url,
            "{",
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "JSON parse error - Expecting property name enclosed in "
                    "double quotes: line 1 column 2 (char 1)"
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_validation_missing_fields(self, mock_empi: Any) -> None:
        """Tests import_person_records rejects request methods with missing fields."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        response = self.client.post(
            url,
            "",
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "details": [
                        {"field": "config_id", "message": "This field is required."},
                    ],
                    "message": "Validation failed",
                }
            },
        )

        response = self.client.post(
            url,
            {"config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "details": [{"message": "Must provide either 's3_uri' or 'file'."}],
                    "message": "Validation failed",
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_invalid_config_id(self, mock_empi: Any) -> None:
        """Tests import_person_records config_id validation fails."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        # config_id is missing underscore and number
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "config_id", "message": "Invalid Config ID"}],
                }
            },
        )

        # config_id has 't' instead of number
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg_t"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "config_id", "message": "Invalid Config ID"}],
                }
            },
        )

        # config_id has 'job' prefix
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "job_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "config_id", "message": "Invalid Config ID"}],
                }
            },
        )

        # config_id (int) is missing prefix
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": 1},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "config_id", "message": "Invalid Config ID"}],
                }
            },
        )

        # config_id (str) is missing prefix
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "config_id", "message": "Invalid Config ID"}],
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_invalid_s3_uri(self, mock_empi: Any) -> None:
        """Tests import_person_records s3_uri validation fails."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.return_value = 1

        url = reverse("import_person_records")

        # s3_uri missing object
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

        # s3_uri bucket name has invalid character
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health?example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

        # s3_uri scheme is missing ':' character
        response = self.client.post(
            url,
            {"s3_uri": "s3//tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

        # s3_uri scheme is incorrect
        response = self.client.post(
            url,
            {"s3_uri": "s4://tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

        # s3_uri scheme is missing
        response = self.client.post(
            url,
            {"s3_uri": "//tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

        # s3_uri is HTTPS URL
        response = self.client.post(
            url,
            {"s3_uri": "https://example.com", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

    @override_settings(DEBUG=False)
    @patch("main.views.person_records.EMPIService")
    def test_import_unexpected_internal_error(self, mock_empi: Any) -> None:
        """Tests import_person_records handles internal errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.side_effect = ValueError("Unexpected error")

        url = reverse("import_person_records")

        self.client.raise_request_exception = False
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.client.raise_request_exception = True

        self.assertEqual(response.status_code, 500)
        self.assertTrue(
            response.json()["error"]["message"].startswith(
                "Unexpected internal error - id="
            )
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_invalid_file_format(self, mock_empi: Any) -> None:
        """Tests import_person_records handles invalid file format."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.side_effect = InvalidPersonRecordFileFormat(
            "Incorrectly formatted person records due to test"
        )

        url = reverse("import_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {"message": "Incorrectly formatted person records due to test"}
                    ],
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_import_s3_object_not_found(self, mock_empi: Any) -> None:
        """Tests import_person_records handles invalid file format."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.import_person_records.side_effect = FileNotFoundError(
            "S3 object does not exist"
        )

        url = reverse("import_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test", "config_id": "cfg_1"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"message": "S3 object does not exist"}],
                }
            },
        )


class ExportPersonRecordsTestCase(TestCase):
    def setUp(self) -> None:
        user = User.objects.create(idp_user_id="1", role=UserRole.member.value)
        auth_patcher = patch(
            "main.views.auth.jwt.JwtAuthentication.authenticate",
            return_value=(user, None),
        )
        auth_patcher.start()
        self.addCleanup(auth_patcher.stop)

    @patch("main.views.person_records.EMPIService")
    def test_export_validation_ok(self, mock_empi: Any) -> None:
        """Tests export_person_records request validation succeeds."""
        url = reverse("export_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {})

    @patch("main.views.person_records.EMPIService")
    def test_export_validation_invalid_content_type(self, mock_empi: Any) -> None:
        """Tests export_person_records rejects content types other than application/json."""
        url = reverse("export_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test"},
        )
        self.assertEqual(response.status_code, 415)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Unsupported media type "multipart/form-data; boundary=BoUnDaRyStRiNg" in request.',
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_export_validation_invalid_request_method(self, mock_empi: Any) -> None:
        """Tests export_person_records rejects request methods besides POST."""
        url = reverse("export_person_records")

        response = self.client.get(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Method "GET" not allowed.',
                }
            },
        )

        response = self.client.delete(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Method "DELETE" not allowed.',
                }
            },
        )

        response = self.client.patch(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Method "PATCH" not allowed.',
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_export_validation_invalid_json(self, mock_empi: Any) -> None:
        """Tests export_person_records rejects request methods with invalid JSON."""
        url = reverse("export_person_records")

        response = self.client.post(
            url,
            "{",
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "JSON parse error - Expecting property name enclosed in "
                    "double quotes: line 1 column 2 (char 1)"
                }
            },
        )

    @patch("main.views.person_records.EMPIService")
    def test_export_invalid_s3_uri(self, mock_empi: Any) -> None:
        """Tests export_person_records s3_uri validation fails."""
        url = reverse("export_person_records")

        # s3_uri missing object
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

        # s3_uri bucket name has invalid character
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health?example/test"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"field": "s3_uri", "message": "Invalid S3 URI"}],
                }
            },
        )

    @override_settings(DEBUG=False)
    @patch("main.views.person_records.EMPIService")
    def test_export_unexpected_internal_error(self, mock_empi: Any) -> None:
        """Tests export_person_records handles internal errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.export_person_records.side_effect = ValueError("Unexpected error")

        url = reverse("export_person_records")

        self.client.raise_request_exception = False
        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test"},
            content_type="application/json",
        )
        self.client.raise_request_exception = True

        self.assertEqual(response.status_code, 500)
        self.assertTrue(
            response.json()["error"]["message"].startswith(
                "Unexpected internal error - id="
            )
        )

    @patch("main.views.person_records.EMPIService")
    def test_export_s3_upload_error(self, mock_empi: Any) -> None:
        """Tests export_person_records handles S3 upload errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.export_person_records.side_effect = FileNotFoundError(
            "Failed to upload to S3"
        )

        url = reverse("export_person_records")

        response = self.client.post(
            url,
            {"s3_uri": "s3://tuva-health-example/test"},
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [{"message": "Failed to upload to S3"}],
                }
            },
        )
