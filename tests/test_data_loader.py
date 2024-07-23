import json
import os
import sys
import unittest
import unittest.mock
from unittest.mock import MagicMock, patch

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from data_loader import DataLoader


# --------------------------------------------------------------------
class TestDataLoader(unittest.TestCase):
    """
    Test suite for the `DataLoader` class.

    This suite tests the functionality of the `DataLoader` class, including data loading
    from JSON files into database tables.
    """

    # -----------------------------------------------------------------
    def setUp(self):
        """
        Set up a `DataLoader` instance with a mock `DatabaseManager` for testing.
        """
        self.db_manager = MagicMock()

    # -----------------------------------------------------------------
    def fix_data_students(self):
        """
        Provide example student data for testing.

        Returns:
            List[dict]: A list of dictionaries representing student data.
        """
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
    def fix_data_rooms(self):
        """
        Provide example room data for testing.

        Returns:
            List[dict]: A list of dictionaries representing room data.
        """
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
    @patch("builtins.open", new_callable=MagicMock)
    def test_load_data_from_json_student(self, mock_open):
        """
        Test the `load_data_from_json` method of the `DataLoader` class for loading student data.

        Verifies that the method correctly reads student data from a JSON file and inserts
        it into the database.
        """
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
    @patch("builtins.open", new_callable=MagicMock)
    def test_load_data_from_json_room(self, mock_open):
        """
        Test the `load_data_from_json` method of the `DataLoader` class for loading room data.

        Verifies that the method correctly reads room data from a JSON file and inserts
        it into the database.
        """
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
