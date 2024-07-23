import logging
import os
import sys
import unittest
import unittest.mock
from unittest.mock import MagicMock, call, mock_open, patch

import psycopg2

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)

from execute_queries import DataExporter


# ---------------------------------------------------------------------
class TestDataExporter(unittest.TestCase):
    """
    Test suite for the `DataExporter` class.

    This suite tests various functionalities of the `DataExporter` class,
    including SQL file execution, index creation, data export to JSON and XML,
    and result export based on format.
    """

    # -----------------------------------------------------------------
    def setUp(self):
        """
        Set up a `DataExporter` instance with a mock database connection
        for testing.

        Creates a mock connection and cursor, and initializes a `DataExporter`
        instance with the mock connection.
        """
        self.connection_mock = MagicMock()
        self.cursor_mock = MagicMock()
        self.connection_mock.cursor.return_value = self.cursor_mock
        self.exporter = DataExporter(self.connection_mock)

    # -----------------------------------------------------------------
    def test_init(self):
        """
        Test the constructor of the `DataExporter` class.

        Verifies that the `DataExporter` instance is correctly initialized
        with the provided database connection.
        """
        exporter = DataExporter(self.connection_mock)
        self.assertEqual(exporter.connection, self.connection_mock)

    # -----------------------------------------------------------------
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_sql_file(self, mock_open):
        """
        Test the `execute_sql_file` method of the `DataExporter` class.

        Verifies that SQL commands in the provided file are executed correctly
        and the results are returned in the specified format (dictionary).
        """
        sql_content = "SELECT * FROM test_table1; SELECT * FROM test_table2;"
        mock_open.return_value.__enter__.return_value.read.return_value = sql_content
        self.cursor_mock.description = [("col1",), ("col2",)]

        expected_result_table1 = [
            ("row1_col1", "row1_col2"),
            ("row2_col1", "row2_col2"),
        ]
        expected_result_table2 = [
            ("row1_col1", "row1_col2"),
            ("row2_col1", "row2_col2"),
        ]
        self.cursor_mock.fetchall.side_effect = [
            expected_result_table1,
            expected_result_table2,
        ]

        expected_dict_result = [
            [
                {"col1": "row1_col1", "col2": "row1_col2"},
                {"col1": "row2_col1", "col2": "row2_col2"},
            ],
            [
                {"col1": "row1_col1", "col2": "row1_col2"},
                {"col1": "row2_col1", "col2": "row2_col2"},
            ],
        ]

        results = self.exporter.execute_sql_file("test_path.sql", "dict")

        self.assertEqual(results, expected_dict_result)
        self.cursor_mock.execute.assert_has_calls(
            [
                unittest.mock.call("SELECT * FROM test_table1"),
                unittest.mock.call("SELECT * FROM test_table2"),
            ]
        )
        self.cursor_mock.close.assert_called_once()

    # -----------------------------------------------------------------
    @patch("builtins.open", new_callable=MagicMock)
    def test_create_indexes_from_sql_file(self, mock_open):
        """
        Test the `create_indexes_from_sql_file` method of the `DataExporter` class.

        Verifies that index creation queries from the SQL file are executed
        correctly and the connection is committed. Also tests error handling.
        """
        sql_content = """
        CREATE INDEX IF NOT EXISTS idx_student_id ON student(id);
        CREATE INDEX IF NOT EXISTS idx_room_name ON room(name);
        """
        mock_open.return_value.__enter__.return_value.read.return_value = sql_content

        obj = DataExporter(self.connection_mock)
        obj.create_indexes_from_sql_file("test_file.sql")

        expected_queries = [
            "CREATE INDEX IF NOT EXISTS idx_student_id ON student(id)",
            "CREATE INDEX IF NOT EXISTS idx_room_name ON room(name)",
        ]
        actual_execute_calls = [
            call[0][0] for call in self.cursor_mock.execute.call_args_list
        ]
        self.assertEqual(actual_execute_calls, expected_queries)
        self.connection_mock.commit.assert_called()

        self.cursor_mock.execute.side_effect = psycopg2.Error("Test error")
        with self.assertRaises(psycopg2.Error):
            obj.create_indexes_from_sql_file("test_file.sql")
        self.assertTrue(logging.getLogger().error)

    # -----------------------------------------------------------------
    @patch("builtins.open", new_callable=mock_open)
    def test_export_to_json(self, mock_open):
        """
        Test the `export_to_json` method of the `DataExporter` class.

        Verifies that data is correctly exported to a JSON file and
        the written data matches the expected format.
        """
        data = [{"key": "value"}]
        output_files = ["test.json"]

        exporter = DataExporter(None)
        exporter.export_to_json(data, output_files)
        mock_open.assert_called_once_with("test.json", "w", encoding="utf-8")

        expected_calls = [
            call("{"),
            call("\n    "),
            call('"key"'),
            call(": "),
            call('"value"'),
            call("\n"),
            call("}"),
        ]
        mock_open().write.assert_has_calls(expected_calls, any_order=False)

    # -----------------------------------------------------------------
    @patch("builtins.open", new_callable=mock_open)
    def test_export_to_xml(self, mock_open):
        """
        Test the `export_to_xml` method of the `DataExporter` class.

        Verifies that data is correctly exported to an XML file and
        the written XML matches the expected format.
        """
        data = [
            (["id", "name", "student_count"], [(1, "Room #1", 10), (2, "Room #2", 5)])
        ]
        output_files = ["output_file.xml"]

        exporter = DataExporter(None)
        exporter.export_to_xml(data, output_files)

        mock_open.assert_called_once_with("output_file.xml", "w", encoding="utf-8")
        handle = mock_open()
        written_data = handle.write.call_args[0][0]

        expected_xml = """<?xml version="1.0" encoding="utf-8"?>
<data>
 <row>
  <id>
   1
  </id>
  <name>
   Room #1
  </name>
  <student_count>
   10
  </student_count>
 </row>
 <row>
  <id>
   2
  </id>
  <name>
   Room #2
  </name>
  <student_count>
   5
  </student_count>
 </row>
</data>"""
        self.assertEqual(written_data.strip(), expected_xml.strip())

    # -----------------------------------------------------------------
    @patch("execute_queries.DataExporter.export_to_json")
    @patch("execute_queries.DataExporter.export_to_xml")
    @patch("execute_queries.DataExporter.execute_sql_file")
    def test_export_result(
        self, mock_execute_sql_file, mock_export_to_xml, mock_export_to_json
    ):
        """
        Test the `export_result` method of the `DataExporter` class.

        Verifies that the result of executing an SQL file is exported in the
        requested format (JSON or XML) to the specified output files.
        """
        sql_file = "test_file.sql"
        expected_result = [{"key": "value"}]
        mock_execute_sql_file.return_value = expected_result

        format_j = "json"
        output_files_json = [
            "output_1.json",
            "output_2.json",
            "output_3.json",
            "output_4.json",
        ]
        self.exporter.export_result(format_j, sql_file)

        mock_execute_sql_file.assert_called_once_with(sql_file, "dict")
        mock_export_to_json.assert_called_once_with(expected_result, output_files_json)

        format_x = "xml"
        output_files_xml = [
            "output_1.xml",
            "output_2.xml",
            "output_3.xml",
            "output_4.xml",
        ]
        self.exporter.export_result(format_x, sql_file)
        mock_export_to_xml.assert_called_once_with(expected_result, output_files_xml)

    # ----------------------------------------------------------------
    def test_convert_to_xml(self):
        """
        Test the `convert_to_xml` method of the `DataExporter` class.

        Verifies that `convert_to_xml` correctly converts query results into
        the expected XML format. Compares the generated XML with the expected
        XML output for given column names and rows.
        """
        columns = ["id", "name", "student_count"]
        rows = [(1, "Room #1", 10), (2, "Room #2", 5)]
        results = [(columns, rows)]

        expected_xml = """<Results><Query_1><Row><id>1</id><name>Room #1</name><student_count>10</student_count></Row><Row><id>2</id><name>Room #2</name><student_count>5</student_count></Row></Query_1></Results>"""
        result = self.exporter.convert_to_xml(results)

        self.assertEqual(result.strip(), expected_xml.strip())


if __name__ == "__main__":
    unittest.main()
