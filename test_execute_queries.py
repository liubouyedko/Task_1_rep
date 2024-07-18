import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import unittest.mock
from execute_queries import DataExporter


# ---------------------------------------------------------------------
class TestDataExporter(unittest.TestCase):
    # -----------------------------------------------------------------
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
    # Test Executing sql File (DataExporter -> execute_sql_file())
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_sql_file(self, mock_open):
        sql_content = "SELECT * FROM test_table1; SELECT * FROM test_table2;"
        mock_open.return_value.__enter__.return_value.read.return_value = sql_content

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

        results = self.exporter.execute_sql_file("test_path.sql")

        expected_queries = ["SELECT * FROM test_table1", "SELECT * FROM test_table2"]
        actual_queries = [
            call[0][0] for call in self.cursor_mock.execute.call_args_list
        ]
        self.assertEqual(actual_queries, expected_queries)
        self.cursor_mock.close.assert_called_once()

    # -----------------------------------------------------------------
    # Test Executing sql File if Error (DataExporter -> execute_sql_file())
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_sql_file_with_error(self, mock_open):
        sql_content = "SELECT * FROM test_table; INVALID SQL QUERY;"
        mock_open.return_value.__enter__.return_value.read.return_value = sql_content

    # -----------------------------------------------------------------
    # Test Exporting Data to json (DataExporter -> export_to_json())
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
    # Test Exporting Data to xml (DataExporter -> export_to_xml())
    @patch("builtins.open", new_callable=mock_open)
    def test_export_to_xml(self, mock_open):
        data = [{"key": "value"}]
        output_files = ["test.xml"]

        exporter = DataExporter(None)
        exporter.export_to_xml(data, output_files)
        mock_open.assert_called_once_with("test.xml", "w", encoding="utf-8")

        expected_call = [
            call(
                '<?xml version="1.0" encoding="utf-8"?>\n<data>\n <row>\n  <col_0>\n   k\n  </col_0>\n  <col_1>\n   e\n  </col_1>\n  <col_2>\n   y\n  </col_2>\n </row>\n</data>\n'
            )
        ]
        mock_open.return_value.write.assert_has_calls(expected_call, any_order=False)

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

        mock_execute_sql_file.assert_called_once_with(sql_file)
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


if __name__ == "__main__":
    unittest.main()
