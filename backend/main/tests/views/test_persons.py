import uuid
from datetime import datetime
from typing import Any, Mapping
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse

from main.models import Person
from main.services.mpi_engine.mpi_engine_service import (
    PersonDict,
    PersonRecordDict,
    PersonSummaryDict,
)
from main.util.dict import select_keys


class PersonsTestCase(TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    #
    # get_persons
    #

    @patch("main.views.persons.MPIEngineService")
    def test_get_persons_ok_all_params(self, mock_mpi_engine: Any) -> None:
        """Tests get_persons succeeds (all query params)."""
        persons: list[PersonSummaryDict] = [
            {
                "uuid": str(uuid.uuid4()),
                "first_name": "John",
                "last_name": "Doe",
                "data_sources": ["ds1", "ds2"],
            }
        ]
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_persons.return_value = persons

        url = reverse("get_persons")
        query_params = {
            "first_name": "John",
            "last_name": "Doe",
            "birth_date": "1990-01-01",
            "person_id": "p_123",
            "source_person_id": "source_123",
            "data_source": "test_source",
        }
        response = self.client.get(url, query_params)

        mock_mpi_engine_obj.get_persons.assert_called_once_with(
            **{**query_params, "person_id": "123"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(),
            {
                "persons": [
                    {
                        **select_keys(
                            persons[0], {"first_name", "last_name", "data_sources"}
                        ),
                        "id": "p_" + persons[0]["uuid"],
                    }
                ]
            },
        )

    @patch("main.views.persons.MPIEngineService")
    def test_get_persons_ok_no_params(self, mock_mpi_engine: Any) -> None:
        """Tests get_persons succeeds (no query params)."""
        persons: list[PersonSummaryDict] = [
            {
                "uuid": str(uuid.uuid4()),
                "first_name": "John",
                "last_name": "Doe",
                "data_sources": ["ds1", "ds2"],
            }
        ]
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_persons.return_value = persons

        url = reverse("get_persons")
        query_params: Mapping[str, str] = {}
        response = self.client.get(url, query_params)

        mock_mpi_engine_obj.get_persons.assert_called_once_with(**query_params)
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(),
            {
                "persons": [
                    {
                        **select_keys(
                            persons[0], {"first_name", "last_name", "data_sources"}
                        ),
                        "id": "p_" + persons[0]["uuid"],
                    }
                ]
            },
        )

    @patch("main.views.persons.MPIEngineService")
    def test_get_persons_ok_no_results(self, mock_mpi_engine: Any) -> None:
        """Tests get_persons succeeds (no persons)."""
        persons: list[PersonSummaryDict] = []
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_persons.return_value = persons

        url = reverse("get_persons")
        response = self.client.get(url, {})

        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(), {"persons": persons})

    def test_get_persons_invalid_request_method(self) -> None:
        """Tests get_persons rejects request methods besides GET."""
        url = reverse("get_persons")

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

    def test_get_persons_invalid_query_params(self) -> None:
        """Tests get_persons rejects invalid query parameters."""
        url = reverse("get_persons")
        response = self.client.get(url, {"invalid_param": "test"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())

    @patch("main.views.persons.MPIEngineService")
    def test_get_persons_internal_error(self, mock_mpi_engine: Any) -> None:
        """Tests get_persons handles unexpected internal errors."""
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_persons.side_effect = Exception("Unexpected error")

        url = reverse("get_persons")
        self.client.raise_request_exception = False
        response = self.client.get(url, {})
        self.client.raise_request_exception = True

        self.assertEqual(response.status_code, 500)
        self.assertIn("error", response.json())
        self.assertTrue(
            response.json()["error"]["message"].startswith("Unexpected internal error")
        )

    #
    # get_person
    #

    @patch("main.views.persons.MPIEngineService")
    def test_get_person_ok(self, mock_mpi_engine: Any) -> None:
        """Tests get_person succeeds."""
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
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_person.return_value = person

        url = reverse("get_person", args=["p_" + str(person_id)])

        response = self.client.get(url)

        mock_mpi_engine_obj.get_person.assert_called_once_with(uuid=str(person_id))
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(
            response.json(),
            {
                "person": {
                    "id": "p_" + str(person_id),
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
                            "person_id": "p_" + str(person_id),
                            "person_updated": person_record[
                                "person_updated"
                            ].isoformat(),
                        }
                    ],
                }
            },
        )

    @patch("main.views.persons.MPIEngineService")
    def test_get_person_not_found(self, mock_mpi_engine: Any) -> None:
        """Tests get_person returns 404 when person does not exist."""
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_person.side_effect = Person.DoesNotExist()

        person_id = uuid.uuid4()
        url = reverse("get_person", args=["p_" + str(person_id)])

        response = self.client.get(url)

        mock_mpi_engine_obj.get_person.assert_called_once_with(uuid=str(person_id))
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(
            response.json(),
            {"error": {"message": "Resource not found"}},
        )

    def test_get_person_invalid_id(self) -> None:
        """Tests get_person rejects request with invalid match ID."""
        person_id_int = 789
        url = reverse("get_person", args=[str(person_id_int)])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {
                            "field": "person_id",
                            "message": "Invalid Person ID",
                        }
                    ],
                }
            },
        )

        person_id = uuid.uuid4()
        url = reverse("get_person", args=["x_" + str(person_id)])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(
            response.json(),
            {
                "error": {
                    "message": "Validation failed",
                    "details": [
                        {
                            "field": "person_id",
                            "message": "Invalid Person ID",
                        }
                    ],
                }
            },
        )

    def test_get_person_invalid_request_method(self) -> None:
        """Tests get_person rejects request methods besides GET."""
        person_id = uuid.uuid4()
        url = reverse("get_person", args=["p_" + str(person_id)])

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

    @patch("main.views.persons.MPIEngineService")
    def test_get_person_internal_error(self, mock_mpi_engine: Any) -> None:
        """Tests get_person handles unexpected internal errors."""
        mock_mpi_engine_obj = mock_mpi_engine.return_value
        mock_mpi_engine_obj.get_person.side_effect = Exception("Unexpected error")

        person_id = uuid.uuid4()
        url = reverse("get_person", args=["p_" + str(person_id)])

        self.client.raise_request_exception = False
        response = self.client.get(url)
        self.client.raise_request_exception = True

        mock_mpi_engine_obj.get_person.assert_called_once_with(uuid=str(person_id))
        self.assertEqual(response.status_code, 500)
        self.assertIn("error", response.json())
        self.assertTrue(
            response.json()["error"]["message"].startswith("Unexpected internal error")
        )
