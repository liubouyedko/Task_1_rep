import json
import logging
import xml.etree.ElementTree as ET
from decimal import Decimal
from typing import Any, List, Tuple, Union

import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv

dotenv_path = ".env"
load_dotenv(dotenv_path=dotenv_path)


# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


class DataExporter:
    """
    A class responsible for exporting data from a PostgreSQL database to JSON and XML formats.

    Attributes:
        connection (psycopg2.extensions.connection): The database connection object used to execute SQL queries.

    Methods:
        execute_sql_file(sql_file: str, output_format: str = "dict") -> Union[List[List[dict]], List[Tuple[List[str], List[Tuple[Any, ...]]]]]:
            Executes SQL queries from a file and returns the results in the specified format.

        create_indexes_from_sql_file(sql_file: str) -> None:
            Creates database indexes based on the SQL commands in the provided file.

        export_to_json(data: List[Any], output_files: List[Any]) -> None:
            Exports the given data to JSON format and writes it to specified output files.

        export_to_xml(data: List[Tuple[List[str], List[Tuple[Any, ...]]]], output_files: List[Any]) -> None:
            Exports the given data to XML format and writes it to specified output files.

        export_result(format: str, sql_file: str) -> None:
            Executes SQL queries from a file and exports the results to JSON or XML format based on the specified format.

        convert_to_xml(results: List[Tuple[List[str], List[Tuple[Any, ...]]]]) -> str:
            Converts the provided query results into an XML string.
    """

    def __init__(self, connection):
        """
        Initializes the DataExporter with a database connection.

        Args:
            connection (psycopg2.extensions.connection): The database connection object.
        """
        self.connection = connection

    def execute_sql_file(
        self, sql_file: str, output_format: str = "dict"
    ) -> Union[List[List[dict]], List[Tuple[List[str], List[Tuple[Any, ...]]]]]:
        """
        Executes SQL queries from a file and returns the results in the specified format.

        This method reads SQL commands from the given file, executes them against the database, and formats the results
        based on the provided output format. The results can be returned as a list of dictionaries or a list of tuples
        depending on the output format.

        Args:
            sql_file (str): The path to the SQL file containing the queries to execute.
            output_format (str, optional): The format for the results. Can be "dict" for a list of dictionaries or "xml"
                for a list of tuples containing column names and rows. Defaults to "dict".

        Returns:
            Union[List[List[dict]], List[Tuple[List[str], List[Tuple[Any, ...]]]]]: The results of the executed SQL queries,
                formatted according to the output format.
        """
        with open(sql_file, "r") as file:
            sql = file.read()

        cursor = self.connection.cursor()
        queries = sql.split(";")
        results = []

        for query in queries:
            query = query.strip()
            if query:
                try:
                    cursor.execute(query)
                    column_names = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    if output_format == "dict":
                        results.append([dict(zip(column_names, row)) for row in rows])
                    elif output_format == "xml":
                        results.append((column_names, rows))
                except psycopg2.Error as e:
                    logging.error(f"Error executing query: {query}")
                    logging.error(f"Database error: {e}")
                    raise

        cursor.close()
        return results

    def create_indexes_from_sql_file(self, sql_file: str) -> None:
        """
        Creates database indexes based on the SQL commands in the provided file.

        This method reads SQL commands from the specified file and executes them to create indexes in the database.
        The connection to the database is used to execute these commands.

        Args:
            sql_file (str): The path to the SQL file containing the commands to create indexes.
        """
        with open(sql_file, "r") as file:
            sql = file.read()

        cursor = self.connection.cursor()
        queries = sql.split(";")

        for query in queries:
            query = query.strip()
            if query:
                try:
                    cursor.execute(query)
                    self.connection.commit()
                    logging.info("Index created")
                except psycopg2.Error as e:
                    logging.error(f"Error executing query: {query}")
                    logging.error(f"Database error: {e}")
                    raise
                except Exception as e:
                    logging.error(f"Unexpected error executing query: {query}")
                    logging.error(f"Error details: {e}")
                    raise
        cursor.close()

    def export_to_json(self, data: List[Any], output_files: List[Any]) -> None:
        """
        Exports the given data to JSON format and writes it to specified output files.

        This method serializes the provided data into JSON format and writes it to the files specified in
        `output_files`. The data is serialized using a custom serializer for `Decimal` objects.

        Args:
            data (List[Any]): The data to be exported to JSON format.
            output_files (List[Any]): The list of file paths where the JSON data should be written.
        """

        def default_serializer(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(
                f"Object of type {obj.__class__.__name__} is not JSON serializable"
            )

        for result, output_file in zip(data, output_files):
            with open(output_file, "w", encoding="utf-8") as file:
                json.dump(
                    result,
                    file,
                    ensure_ascii=False,
                    indent=4,
                    default=default_serializer,
                )

    def export_to_xml(
        self,
        data: List[Tuple[List[str], List[Tuple[Any, ...]]]],
        output_files: List[Any],
    ) -> None:
        """
        Exports the given data to XML format and writes it to specified output files.

        This method converts the provided data into XML format using BeautifulSoup and writes the XML to
        the files specified in `output_files`.

        Args:
            data (List[Tuple[List[str], List[Tuple[Any, ...]]]]): The data to be exported to XML format. Each item
                in the list is a tuple containing column names and rows.
            output_files (List[Any]): The list of file paths where the XML data should be written.
        """
        for (columns, rows), output_file in zip(data, output_files):
            root = BeautifulSoup(features="xml")
            xml_data = root.new_tag("data")

            for row in rows:
                row_tag = root.new_tag("row")
                for col, value in zip(columns, row):
                    col_tag = root.new_tag(col)
                    col_tag.string = str(value)
                    row_tag.append(col_tag)
                xml_data.append(row_tag)

            root.append(xml_data)

            with open(output_file, "w", encoding="utf-8") as file:
                file.write(str(root.prettify()))

    def export_result(self, format: str, sql_file: str) -> None:
        """
        Executes SQL queries from a file and exports the results to JSON or XML format based on the specified format.

        This method calls `execute_sql_file` to execute the SQL queries from the provided file, then exports the
        results to the specified format (either JSON or XML) using the appropriate export method.

        Args:
            format (str): The format to export the results to. Can be "json" or "xml".
            sql_file (str): The path to the SQL file containing the queries to execute.
        """
        if format == "json":
            output_files_json = [
                "output_1.json",
                "output_2.json",
                "output_3.json",
                "output_4.json",
            ]
            results = self.execute_sql_file(sql_file, "dict")
            self.export_to_json(results, output_files_json)

        elif format == "xml":
            output_files_xml = [
                "output_1.xml",
                "output_2.xml",
                "output_3.xml",
                "output_4.xml",
            ]
            results = self.execute_sql_file(sql_file, "xml")
            self.export_to_xml(results, output_files_xml)

    def convert_to_xml(
        self, results: List[Tuple[List[str], List[Tuple[Any, ...]]]]
    ) -> str:
        """
        Converts the provided query results into an XML string.

        This method converts the query results into an XML representation using the ElementTree library.

        Args:
            results (List[Tuple[List[str], List[Tuple[Any, ...]]]]): The query results to be converted to XML. Each
                item in the list is a tuple containing column names and rows.

        Returns:
            str: The XML representation of the query results.
        """
        root = ET.Element("Results")
        for i, (columns, rows) in enumerate(results):
            query_element = ET.SubElement(root, f"Query_{i+1}")
            for row in rows:
                row_element = ET.SubElement(query_element, "Row")
                for col, val in zip(columns, row):
                    col_element = ET.SubElement(row_element, col)
                    col_element.text = str(val)
        return ET.tostring(root, encoding="utf-8").decode("utf-8")
