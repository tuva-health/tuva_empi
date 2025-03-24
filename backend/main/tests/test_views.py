from django.test import TestCase


class ViewsTestCase(TestCase):
    def test_not_found(self) -> None:
        """Tests 404 handler."""
        url = "dne"

        response = self.client.post(url)
        self.assertEqual(response.status_code, 404)
        self.assertDictEqual(
            response.json(), {"error": {"message": "Resource not found"}}
        )
