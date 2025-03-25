import uuid
from typing import Any
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse


class MatchesTestCase(TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    #
    # create_match
    #

    @patch("main.views.matches.EMPIService")
    def test_create_match_ok(self, mock_empi: Any) -> None:
        """Tests create_match succeeds."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.match_person_records.return_value = None

        url = reverse("create_match")
        person_uuid = str(uuid.uuid4())
        request_data = {
            "potential_match_id": "pm_123",
            "potential_match_version": 1,
            "person_updates": [
                {
                    "id": "p_" + person_uuid,
                    "version": 2,
                    "new_person_record_ids": ["pr_789", "pr_101"],
                }
            ],
            "comments": [
                {
                    "person_record_id": "pr_789",
                    "comment": "Reviewed and approved.",
                }
            ],
        }

        response = self.client.post(url, request_data, content_type="application/json")

        mock_empi_obj.match_person_records.assert_called_once_with(
            potential_match_id=123,
            potential_match_version=1,
            person_updates=[
                {
                    "uuid": person_uuid,
                    "version": 2,
                    "new_person_record_ids": [789, 101],
                }
            ],
            comments=[
                {
                    "person_record_id": 789,
                    "comment": "Reviewed and approved.",
                }
            ],
        )

        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {})

    @patch("main.views.matches.EMPIService")
    def test_create_match_no_comments(self, mock_empi: Any) -> None:
        """Tests create_match succeeds without comments."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.match_person_records.return_value = None

        url = reverse("create_match")
        person_uuid = str(uuid.uuid4())
        request_data = {
            "potential_match_id": "pm_321",
            "potential_match_version": 3,
            "person_updates": [
                {
                    "id": "p_" + person_uuid,
                    "version": 1,
                    "new_person_record_ids": ["pr_777"],
                }
            ],
        }

        response = self.client.post(url, request_data, content_type="application/json")

        mock_empi_obj.match_person_records.assert_called_once_with(
            potential_match_id=321,
            potential_match_version=3,
            person_updates=[
                {
                    "uuid": person_uuid,
                    "version": 1,
                    "new_person_record_ids": [777],
                }
            ],
            comments=[],
        )

        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {})

    def test_create_match_invalid_request_method(self) -> None:
        """Tests create_match rejects request methods besides POST."""
        url = reverse("create_match")

        response = self.client.get(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(), {"error": {"message": 'Method "GET" not allowed.'}}
        )

        response = self.client.put(url, {})
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(), {"error": {"message": 'Method "PUT" not allowed.'}}
        )

        response = self.client.delete(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(), {"error": {"message": 'Method "DELETE" not allowed.'}}
        )

    def test_create_match_invalid_content_type(self) -> None:
        """Tests create_match rejects content types other than application/json."""
        url = reverse("create_match")

        response = self.client.post(url, {"potential_match_id": "pm_123"})
        self.assertEqual(response.status_code, 415)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": 'Unsupported media type "multipart/form-data; boundary=BoUnDaRyStRiNg" in request.',
                }
            },
        )

    def test_create_match_invalid_json(self) -> None:
        """Tests create_match rejects requests with invalid JSON format."""
        url = reverse("create_match")

        response = self.client.post(url, "{", content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "JSON parse error - Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
                }
            },
        )

    def test_create_match_missing_fields(self) -> None:
        """Tests create_match rejects requests with missing required fields."""
        url = reverse("create_match")

        request_data = {
            "potential_match_id": "pm_123",
            "potential_match_version": 1,
            "person_updates": [{}],
            "comments": [{}],
        }
        response = self.client.post(url, request_data, content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {
                            "field": "person_updates.0.new_person_record_ids",
                            "message": "This field is required.",
                        },
                        {
                            "field": "comments.0.person_record_id",
                            "message": "This field is required.",
                        },
                        {
                            "field": "comments.0.comment",
                            "message": "This field is required.",
                        },
                    ],
                }
            },
        )

        response = self.client.post(url, {}, content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {
                            "field": "potential_match_id",
                            "message": "This field is required.",
                        },
                        {
                            "field": "potential_match_version",
                            "message": "This field is required.",
                        },
                        {
                            "field": "person_updates",
                            "message": "This field is required.",
                        },
                    ],
                }
            },
        )

    def test_create_match_invalid_potential_match_id(self) -> None:
        """Tests create_match rejects requests with invalid potential_match_id."""
        url = reverse("create_match")

        self.client.raise_request_exception = False
        response = self.client.post(
            url,
            {
                "potential_match_id": "123",
                "potential_match_version": 1,
                "person_updates": [],
            },
            content_type="application/json",
        )
        self.client.raise_request_exception = True

        # FIXME: Should return 400
        self.assertEqual(response.status_code, 500)
        self.assertIn("error", response.json())
        self.assertTrue(
            response.json()["error"]["message"].startswith("Unexpected internal error")
        )

    @patch("main.views.matches.EMPIService")
    def test_create_match_internal_error(self, mock_empi: Any) -> None:
        """Tests create_match handles unexpected internal errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.match_person_records.side_effect = Exception("Unexpected error")

        url = reverse("create_match")

        request_data = {
            "potential_match_id": "pm_123",
            "potential_match_version": 1,
            "person_updates": [
                {
                    "id": "p_456",
                    "version": 2,
                    "new_person_record_ids": ["pr_789"],
                }
            ],
        }

        self.client.raise_request_exception = False
        response = self.client.post(url, request_data, content_type="application/json")
        self.client.raise_request_exception = True

        self.assertEqual(response.status_code, 500)
        self.assertIn("error", response.json())
        self.assertTrue(
            response.json()["error"]["message"].startswith("Unexpected internal error")
        )
