from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import psycopg
from django.db import DatabaseError
from django.db.backends.utils import CursorWrapper

from main.util.record_preprocessor import (
    create_transformation_functions,
    remove_invalid_and_dedupe,
    transform_all_columns,
)


class TestTransformationFunctions(TestCase):
    """Test the PostgreSQL transformation function creation."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.mock_cursor = Mock(spec=CursorWrapper)

    def test_create_transformation_functions_success(self) -> None:
        """Test successful creation of all transformation functions."""
        result = create_transformation_functions(self.mock_cursor)

        # Assert: should return True and execute SQL 12 times (one for each function)
        self.assertTrue(result)
        self.assertEqual(self.mock_cursor.execute.call_count, 12)

        # Verify that each function creation SQL was called
        calls = self.mock_cursor.execute.call_args_list

        # Check that normalize_first_name function was created
        first_call = str(calls[0][0][0])  # First call's first argument
        self.assertIn("CREATE OR REPLACE FUNCTION normalize_first_name", first_call)

    @patch("main.util.record_preprocessor.logger")
    def test_create_transformation_functions_programming_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of SQL syntax errors during function creation."""
        # Arrange: mock execute to raise ProgrammingError
        self.mock_cursor.execute.side_effect = psycopg.ProgrammingError(
            "syntax error at or near 'FUNCTION'"
        )

        # Act
        result = create_transformation_functions(self.mock_cursor)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert (
            error_message
            == "Failed to create PostgreSQL transformation functions - SQL syntax error. Check function definitions and SQL syntax: syntax error at or near 'FUNCTION'"
        )

    @patch("main.util.record_preprocessor.logger")
    def test_create_transformation_functions_operational_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of database connection errors."""
        # Arrange: mock execute to raise OperationalError
        self.mock_cursor.execute.side_effect = psycopg.OperationalError(
            "connection to server lost"
        )

        # Act
        result = create_transformation_functions(self.mock_cursor)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert (
            error_message
            == "Failed to create PostgreSQL transformation functions - database connection or operational error. Check database connectivity and permissions: connection to server lost"
        )

    @patch("main.util.record_preprocessor.logger")
    def test_create_transformation_functions_database_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of general database errors."""
        # Arrange: mock execute to raise DatabaseError
        self.mock_cursor.execute.side_effect = DatabaseError("insufficient privileges")

        # Act
        result = create_transformation_functions(self.mock_cursor)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert (
            error_message
            == "Failed to create PostgreSQL transformation functions - database error. This may indicate insufficient privileges to create functions: insufficient privileges"
        )

    @patch("main.util.record_preprocessor.logger")
    def test_create_transformation_functions_unexpected_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of unexpected errors."""
        # Arrange: mock execute to raise unexpected exception
        self.mock_cursor.execute.side_effect = ValueError("unexpected error")

        # Act
        result = create_transformation_functions(self.mock_cursor)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert (
            error_message
            == "Failed to create PostgreSQL transformation functions - unexpected error during function creation: unexpected error"
        )


class TestTransformAllColumns(TestCase):
    """Test the main transformation function."""

    def setUp(self) -> None:
        self.mock_cursor = Mock(spec=CursorWrapper)
        self.valid_table_name = "test_table_123"
        self.invalid_table_name = "test-table!"

    def test_transform_all_columns_success(self) -> None:
        """Test successful transformation of all columns."""
        # Arrange
        self.mock_cursor.execute.return_value = None

        # Act
        result = transform_all_columns(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertTrue(result["success"])
        self.mock_cursor.execute.assert_called_once()

        # Verify the SQL contains all expected transformations
        executed_sql = str(self.mock_cursor.execute.call_args[0][0])

        expected_transformations = [
            "first_name = normalize_first_name(first_name)",
            "last_name = normalize_last_name(last_name)",
            "sex = normalize_sex(sex)",
            "race = normalize_race(race)",
            "birth_date = normalize_birth_date(birth_date)",
            "death_date = normalize_death_date(death_date)",
            "social_security_number = normalize_ssn(social_security_number)",
            "address = normalize_address(address)",
            "city = normalize_city(city)",
            "state = normalize_state(state)",
            "zip_code = normalize_zip(zip_code)",
            "phone = normalize_phone(phone)",
        ]

        for transformation in expected_transformations:
            self.assertIn(transformation, executed_sql)

    def test_transform_all_columns_valid_table_names(self) -> None:
        """Test that valid table names are accepted."""
        valid_names = [
            "users",
            "user_data",
            "_private_table",
            "table123",
            "Table_With_Mixed_Case_123",
        ]

        self.mock_cursor.execute.return_value = None

        for table_name in valid_names:
            with self.subTest(table_name=table_name):
                result = transform_all_columns(self.mock_cursor, table_name)
                self.assertTrue(result, f"Valid table name '{table_name}' was rejected")

    @patch("main.util.record_preprocessor.logger")
    def test_transform_all_columns_invalid_table_names(
        self, mock_logger: MagicMock
    ) -> None:
        """Test that invalid table names are rejected."""
        invalid_names = [
            "test-table",  # Contains hyphen
            "123table",  # Starts with number
            "table!",  # Contains special character
            "table with spaces",  # Contains spaces
            "",  # Empty string
            "table;DROP TABLE x;",  # SQL injection attempt
            "table'name",  # Contains quote
        ]

        for table_name in invalid_names:
            with self.subTest(table_name=table_name):
                result = transform_all_columns(self.mock_cursor, table_name)
                self.assertFalse(
                    result["success"], f"Invalid table name '{table_name}' was accepted"
                )

        # Should never call execute for invalid names
        self.mock_cursor.execute.assert_not_called()

    @patch("main.util.record_preprocessor.logger")
    def test_transform_all_columns_programming_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of SQL syntax errors during transformation."""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.ProgrammingError(
            "function normalize_first_name(text) does not exist"
        )

        # Act
        result = transform_all_columns(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertFalse(result["success"])
        self.mock_cursor.execute.assert_called_once()
        mock_logger.error.assert_called_once()

        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("SQL syntax error", error_message)
        self.assertIn(self.valid_table_name, error_message)

    @patch("main.util.record_preprocessor.logger")
    def test_transform_all_columns_operational_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of database connection errors."""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg.OperationalError(
            "server closed the connection unexpectedly"
        )

        # Act
        result = transform_all_columns(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()

        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("database connection or operational error", error_message)
        self.assertIn("Check database connectivity", error_message)

    @patch("main.util.record_preprocessor.logger")
    def test_transform_all_columns_database_error(self, mock_logger: MagicMock) -> None:
        """Test handling of general database errors."""
        # Arrange
        self.mock_cursor.execute.side_effect = DatabaseError(
            "table 'test_table_123' doesn't exist"
        )

        # Act
        result = transform_all_columns(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()

        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("database error", error_message)
        self.assertIn(
            "transformation functions exist and table is accessible", error_message
        )

    @patch("main.util.record_preprocessor.logger")
    def test_transform_all_columns_unexpected_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of unexpected errors."""
        # Arrange
        self.mock_cursor.execute.side_effect = RuntimeError("unexpected runtime error")

        # Act
        result = transform_all_columns(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()

        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("unexpected error during column transformation", error_message)


class TestRemoveInvalidAndDedupe(TestCase):
    """Test the cleanup and deduplication function."""

    def setUp(self) -> None:
        self.mock_cursor = Mock(spec=CursorWrapper)
        self.valid_table_name = "test_table_123"

    def test_remove_invalid_and_dedupe_success(self) -> None:
        """Test successful cleanup and deduplication."""
        from unittest.mock import MagicMock

        self.mock_cursor = MagicMock()
        self.mock_cursor.execute.return_value = None
        self.mock_cursor.fetchone.side_effect = [
            [1000],  # Initial count
            [975],  # Final count
        ]
        # Act
        result = remove_invalid_and_dedupe(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertTrue(result["success"])
        # 3 calls for deletion and dedupe, and 2 are for the counts
        self.assertEqual(self.mock_cursor.execute.call_count, 5)

    @patch("main.util.record_preprocessor.logger")
    def test_remove_invalid_and_dedupe_invalid_table_names(
        self, mock_logger: MagicMock
    ) -> None:
        """Test that invalid table names are rejected."""
        invalid_names = [
            "test-table",
            "123table",
            "table!",
            "table with spaces",
            "",
            "a" * 64,  # Too long
            "table;DROP TABLE x;",
            "table'name",
        ]

        for table_name in invalid_names:
            with self.subTest(table_name=table_name):
                result = remove_invalid_and_dedupe(self.mock_cursor, table_name)
                self.assertFalse(
                    result["success"], f"Invalid table name '{table_name}' was accepted"
                )

        # Should never execute for invalid names
        self.mock_cursor.execute.assert_not_called()

        # Should log error for each invalid name
        self.assertEqual(mock_logger.error.call_count, len(invalid_names))

    @patch("main.util.record_preprocessor.logger")
    def test_remove_invalid_and_dedupe_database_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of general database errors."""
        # Arrange
        self.mock_cursor.execute.side_effect = DatabaseError(
            "insufficient permissions for DELETE operations"
        )

        # Act
        result = remove_invalid_and_dedupe(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()

        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("database error", error_message)
        self.assertIn("insufficient permissions for DELETE operations", error_message)

    @patch("main.util.record_preprocessor.logger")
    def test_remove_invalid_and_dedupe_unexpected_error(
        self, mock_logger: MagicMock
    ) -> None:
        """Test handling of unexpected errors."""
        # Arrange
        self.mock_cursor.execute.side_effect = ValueError("unexpected error")

        # Act
        result = remove_invalid_and_dedupe(self.mock_cursor, self.valid_table_name)

        # Assert
        self.assertFalse(result["success"])
        mock_logger.error.assert_called_once()

        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("unexpected error during record deletion", error_message)
