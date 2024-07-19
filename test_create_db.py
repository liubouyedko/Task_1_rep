import unittest
from unittest.mock import patch, MagicMock
import unittest.mock
import json
from create_db import DatabaseManager, DataLoader


# ---------------------------------------------------------------------
class TestDatabaseManager(unittest.TestCase):
    # -----------------------------------------------------------------
    # Test Constructor (DatabaseManager -> __init__())
    @patch("dotenv.load_dotenv")
    @patch("os.getenv")
    def test_constructor_database_manager(self, mock_getenv, mock_load_dotenv):

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
    # Test Creating Connection (DatabaseManager -> create_connection())
    @patch("create_db.psycopg2.connect")
    @patch("create_db.os.getenv")
    def test_create_new_connection(self, mock_getenv, mock_connect):

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
    # Test Executing SQL File (DatabaseManager -> execute_sql_file())
    @patch("create_db.load_dotenv")
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_sql_file(self, mock_open, mock_load_dotenv):

        mock_file = MagicMock()
        mock_file.read.return_value = "SELECT * FROM test_table;"
        mock_open.return_value.__enter__.return_value = mock_file

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        manager = DatabaseManager()
        returned_connection = manager.execute_sql_file("test.sql", mock_connection)

        mock_open.assert_called_once_with("test.sql", "r")
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table;")
        mock_cursor.close.assert_called_once()

        self.assertEqual(returned_connection, mock_connection)

    # -----------------------------------------------------------------
    # Test Creating Database (DatabaseManager -> create_database())
    @patch("create_db.psycopg2.connect")
    @patch("create_db.os.getenv")
    def test_create_database(self, mock_getenv, mock_connect):
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
        manager.create_database()

        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port="5432",
        )

        mock_cursor.execute.assert_called_once_with("CREATE DATABASE test_db")
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    # -----------------------------------------------------------------
    # Test Creating Tables (DatabaseManager -> create_tables())
    @patch("create_db.psycopg2.connect")
    @patch("create_db.DatabaseManager.execute_sql_file")
    @patch("create_db.os.getenv")
    def test_create_tables(self, mock_getenv, mock_execute_sql_file, mock_connect):
        mock_getenv.side_effect = lambda key: {
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
        }.get(key)

        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value

        mock_execute_sql_file.return_value = None
        manager = DatabaseManager()
        result = manager.create_tables()

        self.assertTrue(result)
        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="test_user",
            password="test_password",
            host="localhost",
            port="5432",
        )

        mock_execute_sql_file.assert_called_once_with(
            manager, "db_schema.sql", mock_connection
        )
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()


# --------------------------------------------------------------------
class TestDataLoader(unittest.TestCase):
    # -----------------------------------------------------------------
    # Setting Up
    def setUp(self):
        self.db_manager = MagicMock()

    # -----------------------------------------------------------------
    # Some Example Student Data for Testing
    def fix_data_students(self):
        return [
            {
                "birthday": "1996-05-13T00:00:00.000000",
                "id": 1,
                "name": "Alice",
                "room": 101,
                "sex": "F",
            },
            {
                "birthday": "1997-03-21T00:00:00.000000",
                "id": 2,
                "name": "Bob",
                "room": 102,
                "sex": "M",
            },
        ]

    # -----------------------------------------------------------------
    # Some Example Room Data for Testing
    def fix_data_rooms(self):
        return [
            {
                "id": 1,
                "name": "Room #001",
            },
            {
                "id": 1,
                "name": "Room #001",
            },
        ]

    # -----------------------------------------------------------------
    # Test Loading Data from students.json File (DataLoader -> load_data_from_json())
    @patch("builtins.open", new_callable=MagicMock)
    def test_load_data_from_json_student(self, mock_open):
        connection_mock = MagicMock()
        cursor_mock = MagicMock()
        connection_mock.cursor.return_value.__enter__.return_value = cursor_mock

        json_data = self.fix_data_students()
        table_name = "student"

        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(
            json_data
        )

        data_loader = DataLoader(self.db_manager)
        data_loader.load_data_from_json(connection_mock, "test_path.json", table_name)

        insert_query = (
            f"INSERT INTO {table_name} (birthday, id, name, room, sex) "
            f"VALUES (%s, %s, %s, %s, %s) "
            f"ON CONFLICT (id) DO NOTHING"
        )

        expected_records = [
            (
                record.get("birthday", None),
                record.get("id", None),
                record.get("name", None),
                record.get("room", None),
                record.get("sex", None),
            )
            for record in json_data
        ]

        cursor_mock.executemany.assert_called_once_with(insert_query, expected_records)
        connection_mock.commit.assert_called_once()

    # -----------------------------------------------------------------
    # Test Loading Data from rooms.json File (DataLoader -> load_data_from_json())
    @patch("builtins.open", new_callable=MagicMock)
    def test_load_data_from_json_room(self, mock_open):
        connection_mock = MagicMock()
        cursor_mock = MagicMock()
        connection_mock.cursor.return_value.__enter__.return_value = cursor_mock

        json_data = self.fix_data_rooms()
        table_name = "room"

        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(
            json_data
        )

        data_loader = DataLoader(self.db_manager)
        data_loader.load_data_from_json(connection_mock, "test_path.json", table_name)

        insert_query = (
            f"INSERT INTO {table_name} (id, name) "
            f"VALUES (%s, %s) "
            f"ON CONFLICT (id) DO NOTHING"
        )

        expected_records = [
            (record.get("id", None), record.get("name", None)) for record in json_data
        ]

        cursor_mock.executemany.assert_called_once_with(insert_query, expected_records)
        connection_mock.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
