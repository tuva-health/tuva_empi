import uuid
from datetime import datetime
from typing import Any, Mapping
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse

from main.models import MatchGroup, User, UserRole
from main.services.empi.empi_service import (
    PersonDict,
    PersonRecordDict,
    PotentialMatchDict,
    PotentialMatchSummaryDict,
    PredictionResultDict,
)
from main.util.dict import select_keys


class PotentialMatchesTestCase(TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

        user = User.objects.create(idp_user_id="1", role=UserRole.member.value)
        auth_patcher = patch(
            "main.views.auth.jwt.JwtAuthentication.authenticate",
            return_value=(user, None),
        )
        auth_patcher.start()
        self.addCleanup(auth_patcher.stop)

    #
    # get_potential_matches
    #

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_matches_ok_all_params(self, mock_empi: Any) -> None:
        """Tests get_potential_matches succeeds (all query params)."""
        potential_matches = [
            {
                "id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "data_sources": ["ds1", "ds2"],
                "max_match_probability": 0.85,
            }
        ]
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_matches.return_value = potential_matches

        url = reverse("get_potential_matches")
        query_params = {
            "first_name": "John",
            "last_name": "Doe",
            "birth_date": "1990-01-01",
            "person_id": "p_123",
            "source_person_id": "source_123",
            "data_source": "test_source",
        }
        response = self.client.get(url, query_params)

        mock_empi_obj.get_potential_matches.assert_called_once_with(
            **{**query_params, "person_id": "123"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(),
            {"potential_matches": [{**potential_matches[0], "id": "pm_1"}]},
        )

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_matches_ok_no_params(self, mock_empi: Any) -> None:
        """Tests get_potential_matches succeeds (no query params)."""
        potential_matches = [
            {
                "id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "data_sources": ["ds1", "ds2"],
                "max_match_probability": 0.75,
            }
        ]
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_matches.return_value = potential_matches

        url = reverse("get_potential_matches")
        query_params: Mapping[str, str] = {}
        response = self.client.get(url, query_params)

        mock_empi_obj.get_potential_matches.assert_called_once_with(**query_params)
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(),
            {"potential_matches": [{**potential_matches[0], "id": "pm_1"}]},
        )

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_matches_ok_no_results(self, mock_empi: Any) -> None:
        """Tests get_potential_matches succeeds (no potential matches)."""
        potential_matches: list[PotentialMatchSummaryDict] = []
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_matches.return_value = potential_matches

        url = reverse("get_potential_matches")
        response = self.client.get(url, {})

        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {"potential_matches": potential_matches})

    def test_get_potential_matches_invalid_request_method(self) -> None:
        """Tests get_potential_matches rejects request methods besides GET."""
        url = reverse("get_potential_matches")

        response = self.client.post(url, {})
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": 'Method "POST" not allowed.'}},
        )

        response = self.client.put(url, {})
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": 'Method "PUT" not allowed.'}},
        )

        response = self.client.delete(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": 'Method "DELETE" not allowed.'}},
        )

    def test_get_potential_matches_invalid_query_params(self) -> None:
        """Tests get_potential_matches rejects invalid query parameters."""
        url = reverse("get_potential_matches")
        response = self.client.get(url, {"invalid_param": "test"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_matches_internal_error(self, mock_empi: Any) -> None:
        """Tests get_potential_matches handles unexpected internal errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_matches.side_effect = Exception("Unexpected error")

        url = reverse("get_potential_matches")
        self.client.raise_request_exception = False
        response = self.client.get(url, {})
        self.client.raise_request_exception = True

        self.assertEqual(response.status_code, 500)
        self.assertIn("error", response.json())
        self.assertTrue(
            response.json()["error"]["message"].startswith("Unexpected internal error")
        )

    #
    # get_potential_match
    #

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_match_ok(self, mock_empi: Any) -> None:
        """Tests get_potential_match succeeds."""
        person_id = uuid.uuid4()
        person_record: PersonRecordDict = {
            "id": 1,
            "created": datetime.now(),
            "person_uuid": str(person_id),
            "person_updated": datetime.now(),
            "matched_or_reviewed": False,
            "data_source": "ds1",
            "source_person_id": "spid_1",
            "first_name": "test-fn",
            "last_name": "test-ln",
            "sex": "f",
            "race": "x",
            "birth_date": "now",
            "death_date": "later",
            "social_security_number": "1111",
            "address": "111 Address Way",
            "city": "Test City",
            "state": "AA",
            "zip_code": "11111",
            "county": "Test County",
            "phone": "111-1111",
        }
        person: PersonDict = {
            "uuid": str(person_id),
            "created": datetime.now(),
            "version": 1,
            "records": [person_record],
        }
        predict_result: PredictionResultDict = {
            "id": 1,
            "created": datetime.now(),
            "match_probability": 0.95,
            "person_record_l_id": 1,
            "person_record_r_id": 2,
        }
        potential_match: PotentialMatchDict = {
            "id": 1,
            "created": datetime.now(),
            "version": 1,
            "persons": [person],
            "results": [predict_result],
        }
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_match.return_value = potential_match

        match_id = "pm_123"
        url = reverse("get_potential_match", args=[match_id])

        response = self.client.get(url)

        mock_empi_obj.get_potential_match.assert_called_once_with(id=123)
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(),
            {
                "potential_match": {
                    **potential_match,
                    "id": "pm_1",
                    "created": potential_match["created"].isoformat(),
                    "persons": [
                        {
                            "id": "p_" + str(person["uuid"]),
                            "created": person["created"].isoformat(),
                            "version": 1,
                            "records": [
                                {
                                    **select_keys(
                                        person_record,
                                        person_record.keys() - {"person_uuid"},
                                    ),
                                    "id": "pr_1",
                                    "created": person_record["created"].isoformat(),
                                    "person_id": "p_" + str(person["uuid"]),
                                    "person_updated": person_record[
                                        "person_updated"
                                    ].isoformat(),
                                }
                            ],
                        }
                    ],
                    "results": [
                        {
                            **predict_result,
                            "id": "prre_1",
                            "created": predict_result["created"].isoformat(),
                            "person_record_l_id": "pr_1",
                            "person_record_r_id": "pr_2",
                        }
                    ],
                }
            },
        )

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_match_not_found(self, mock_empi: Any) -> None:
        """Tests get_potential_match returns 404 when potential match does not exist."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_match.side_effect = MatchGroup.DoesNotExist()

        match_id = "pm_456"
        url = reverse("get_potential_match", args=[match_id])

        response = self.client.get(url)

        mock_empi_obj.get_potential_match.assert_called_once_with(id=456)
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": "Resource not found"}},
        )

    def test_get_potential_match_invalid_id(self) -> None:
        """Tests get_potential_match rejects request with invalid match ID."""
        match_id = "789"
        url = reverse("get_potential_match", args=[match_id])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {
                            "field": "potential_match_id",
                            "message": "Invalid PotentialMatch ID",
                        }
                    ],
                }
            },
        )

        match_id = "x_789"
        url = reverse("get_potential_match", args=[match_id])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {
                            "field": "potential_match_id",
                            "message": "Invalid PotentialMatch ID",
                        }
                    ],
                }
            },
        )

    def test_get_potential_match_invalid_request_method(self) -> None:
        """Tests get_potential_match rejects request methods besides GET."""
        match_id = "pm_789"
        url = reverse("get_potential_match", args=[match_id])

        response = self.client.post(url, {})
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": 'Method "POST" not allowed.'}},
        )

        response = self.client.put(url, {})
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": 'Method "PUT" not allowed.'}},
        )

        response = self.client.delete(url)
        self.assertEqual(response.status_code, 405)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": 'Method "DELETE" not allowed.'}},
        )

    @patch("main.views.potential_matches.EMPIService")
    def test_get_potential_match_internal_error(self, mock_empi: Any) -> None:
        """Tests get_potential_match handles unexpected internal errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.get_potential_match.side_effect = Exception("Unexpected error")

        match_id = "pm_321"
        url = reverse("get_potential_match", args=[match_id])

        self.client.raise_request_exception = False
        response = self.client.get(url)
        self.client.raise_request_exception = True

        mock_empi_obj.get_potential_match.assert_called_once_with(id=321)
        self.assertEqual(response.status_code, 500)
        self.assertIn("error", response.json())
        self.assertTrue(
            response.json()["error"]["message"].startswith("Unexpected internal error")
        )

    #
    # export_potential_matches
    #

    @patch("main.views.potential_matches.EMPIService")
    def test_export_potential_matches_ok_s3_uri(self, mock_empi: Any) -> None:
        """Tests export_potential_matches succeeds with S3 URI."""
        mock_empi_obj = mock_empi.return_value
        mock_job = type('Job', (), {'id': 123, 'status': 'new'})()
        mock_empi_obj.create_export_job.return_value = mock_job

        url = reverse("export_potential_matches")
        data = {
            "s3_uri": "s3://bucket/path/file.csv",
        }
        response = self.client.post(url, data, content_type="application/json")

        # The create_export_job should be called, but we don't need to verify the exact config_id
        # since it's a default value in the view
        mock_empi_obj.create_export_job.assert_called_once()
        call_args = mock_empi_obj.create_export_job.call_args
        self.assertEqual(call_args[1]['sink_uri'], "s3://bucket/path/file.csv")

        self.assertEqual(response.status_code, 202)
        self.assertDictEqual(response.json(), {
            "job_id": 123,
            "status": "new",
            "message": "Export job 123 created for S3 export to s3://bucket/path/file.csv"
        })



    @patch("main.views.potential_matches.EMPIService")
    def test_export_potential_matches_ok_estimate(self, mock_empi: Any) -> None:
        """Tests export_potential_matches succeeds with estimate only."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.estimate_export_count.return_value = 147389

        url = reverse("export_potential_matches")
        data = {"estimate": True}
        response = self.client.post(url, data, content_type="application/json")

        mock_empi_obj.estimate_export_count.assert_called_once()
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {
            "estimated_count": 147389,
            "message": "Estimated 147,389 potential match pairs to export"
        })

    @patch("main.views.potential_matches.EMPIService")
    def test_export_potential_matches_s3_error(self, mock_empi: Any) -> None:
        """Tests export_potential_matches handles S3 errors."""
        mock_empi_obj = mock_empi.return_value
        mock_empi_obj.create_export_job.side_effect = Exception("S3 error")

        url = reverse("export_potential_matches")
        data = {"s3_uri": "s3://bucket/path/file.csv"}
        response = self.client.post(url, data, content_type="application/json")

        self.assertEqual(response.status_code, 500)
        self.assertIn("Export failed", response.json()["error"]["message"])
        self.assertIn("S3 error", response.json()["error"]["details"][0]["message"])

    def test_export_potential_matches_invalid_s3_uri(self) -> None:
        """Tests export_potential_matches validates S3 URI format."""
        url = reverse("export_potential_matches")
        data = {"s3_uri": "invalid-uri"}
        response = self.client.post(url, data, content_type="application/json")

        self.assertEqual(response.status_code, 400)
        self.assertIn("Validation failed", response.json()["error"]["message"])
