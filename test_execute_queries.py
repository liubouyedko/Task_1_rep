import unittest
import psycopg2
import logging
import unittest.mock
from unittest.mock import patch, MagicMock, mock_open, call
import xml.etree.ElementTree as ET
from execute_queries import DataExporter


# ---------------------------------------------------------------------
class TestDataExporter(unittest.TestCase):
    # -----------------------------------------------------------------
    # Setting Up
    def setUp(self):
        self.connection_mock = MagicMock()
        self.cursor_mock = MagicMock()
        self.connection_mock.cursor.return_value = self.cursor_mock
        self.exporter = DataExporter(self.connection_mock)

    # -----------------------------------------------------------------
    # Test Constructor (DataExporter -> __init__())
    def test_init(self):
        exporter = DataExporter(self.connection_mock)
        self.assertEqual(exporter.connection, self.connection_mock)

    # -----------------------------------------------------------------
    # Test Executing SQL File (DataExporter -> execute_sql_file())
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_sql_file(self, mock_open):
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
    # Test Creating Indexes from SQL File (DataExporter -> create_indexes_from_sql_file())
    @patch("builtins.open", new_callable=MagicMock)
    def test_create_indexes_from_sql_file(self, mock_open):
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
    # Test Executing SQL File if Error (DataExporter -> execute_sql_file())
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_sql_file_with_error(self, mock_open):
        sql_content = "SELECT * FROM test_table; INVALID SQL QUERY;"
        mock_open.return_value.__enter__.return_value.read.return_value = sql_content

    # -----------------------------------------------------------------
    # Test Exporting Data to JSON (DataExporter -> export_to_json())
    @patch("builtins.open", new_callable=mock_open)
    def test_export_to_json(self, mock_open):
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
    # Test Exporting Data to XML (DataExporter -> export_to_xml())
    @patch("builtins.open", new_callable=mock_open)
    def test_export_to_xml(self, mock_open):
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
    # Test Exporting Result (DataExporter -> export_result())
    @patch("execute_queries.DataExporter.export_to_json")
    @patch("execute_queries.DataExporter.export_to_xml")
    @patch("execute_queries.DataExporter.execute_sql_file")
    def test_export_result(
        self, mock_execute_sql_file, mock_export_to_xml, mock_export_to_json
    ):
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
    # Test Convering to XML (DataExporter -> convert_to_xml())
    def test_convert_to_xml(self):
        columns = ["id", "name", "student_count"]
        rows = [(1, "Room #1", 10), (2, "Room #2", 5)]
        results = [(columns, rows)]

        expected_xml = """<Results><Query_1><Row><id>1</id><name>Room #1</name><student_count>10</student_count></Row><Row><id>2</id><name>Room #2</name><student_count>5</student_count></Row></Query_1></Results>"""
        result = self.exporter.convert_to_xml(results)

        self.assertEqual(result.strip(), expected_xml.strip())


if __name__ == "__main__":
    unittest.main()
