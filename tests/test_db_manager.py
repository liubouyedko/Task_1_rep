import os
import sys
import unittest
import unittest.mock
from unittest.mock import MagicMock, patch

import psycopg2

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from db_manager import DatabaseManager


# ---------------------------------------------------------------------
class TestDatabaseManager(unittest.TestCase):
    """
    Test suite for the `DatabaseManager` class.

    This suite tests various functionalities of the `DatabaseManager` class, including
    its constructor, connection handling, SQL file execution, database creation, and table creation.
    """

    # -----------------------------------------------------------------
    @patch("dotenv.load_dotenv")
    @patch("os.getenv")
    def test_constructor_database_manager(self, mock_getenv, mock_load_dotenv):
        """
        Test the constructor of the `DatabaseManager` class to ensure that it correctly
        initializes its attributes with values from environment variables.
        """
        mock_getenv.side_effect = lambda x: {
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
            "DB_HOST": "test_host",
            "DB_PORT": "5432",
        }.get(x)

        instance = DatabaseManager()

        self.assertEqual(instance.dbname, "test_db")
        self.assertEqual(instance.user, "test_user")
        self.assertEqual(instance.password, "test_password")
        self.assertEqual(instance.host, "test_host")
        self.assertEqual(instance.port, "5432")

    # -----------------------------------------------------------------
    @patch("db_manager.psycopg2.connect")
    @patch("db_manager.os.getenv")
    def test_create_new_connection(self, mock_getenv, mock_connect):
        """
        Test the `create_connection` method of the `DatabaseManager` class to ensure that
        it successfully creates a new database connection using the correct credentials.
        """
        mock_getenv.side_effect = lambda key: {
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
        }.get(key)

        manager = DatabaseManager()
        mock_connection = MagicMock(name="psycopg2_connection_mock")
        mock_connect.return_value = mock_connection
        connection = manager.create_connection()

        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port="5432",
        )

        self.assertEqual(connection, mock_connection)

    # -----------------------------------------------------------------
    @patch(
        "db_manager.open",
        new_callable=unittest.mock.mock_open,
        read_data="SELECT * FROM test_table;",
    )
    @patch("psycopg2.extensions.connection")
    def test_execute_sql_file(self, mock_connection, mock_open):
        """
        Test `execute_sql_file` to ensure it correctly reads SQL from a file and executes it.
        """
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        exporter = DatabaseManager()

        result_connection = exporter.execute_sql_file(
            "./sql_queries/db_schema.sql", mock_connection
        )
        mock_open.assert_called_once_with("./sql_queries/db_schema.sql", "r")
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table;")
        mock_cursor.close.assert_called_once()

        self.assertEqual(result_connection, mock_connection)

    # -----------------------------------------------------------------
    @patch(
        "builtins.open", new_callable=unittest.mock.mock_open, read_data="INVALID SQL;"
    )
    @patch("psycopg2.extensions.connection")
    def test_execute_sql_file_with_error(self, mock_connection, mock_open):
        """
        Test `execute_sql_file` to ensure it raises a `psycopg2.Error`
        when SQL execution fails.
        """
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.execute.side_effect = psycopg2.Error("SQL error")
        instance = DatabaseManager()

        with self.assertRaises(psycopg2.Error):
            instance.execute_sql_file("test.sql", mock_connection)

    # -----------------------------------------------------------------
    @patch("db_manager.psycopg2.connect")
    @patch("db_manager.os.getenv")
    def test_create_database(self, mock_getenv, mock_connect):
        """
        Test the `create_database` method of the `DatabaseManager` class to ensure that
        it successfully creates a database using the provided credentials.
        """
        mock_getenv.side_effect = lambda key: {
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
        }.get(key)

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        manager = DatabaseManager()
        manager.create_database(mock_connection)

        mock_cursor.execute.assert_called_once_with("CREATE DATABASE test_db")
        mock_cursor.close.assert_called_once()

    # -----------------------------------------------------------------
    @patch("db_manager.psycopg2.connect")
    @patch("db_manager.DatabaseManager.execute_sql_file", return_value=None)
    @patch("db_manager.os.getenv")
    def test_create_tables(self, mock_getenv, mock_execute_sql_file, mock_connect):
        """
        Test the `create_tables` method of the `DatabaseManager` class to ensure that
        it correctly creates tables by executing SQL commands from a file.
        """
        mock_getenv.side_effect = lambda key: {
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
        }.get(key)

        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value

        manager = DatabaseManager()
        result = manager.create_tables(mock_connection)

        self.assertTrue(result)

        mock_execute_sql_file.assert_called_once_with(
            "./sql_queries/db_schema.sql", mock_connection
        )
        mock_cursor.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
