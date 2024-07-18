import json
import logging
from decimal import Decimal
from typing import Any, List, Tuple, Union
import xml.etree.ElementTree as ET

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
    def __init__(self, connection):
        self.connection = connection

    def execute_sql_file(
        self, sql_file: str, output_format: str = "dict"
    ) -> Union[List[List[dict]], List[Tuple[List[str], List[Tuple[Any, ...]]]]]:
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
        if format == "json":
            output_files_json = [
                "output_1.json",
                "output_2.json",
                "output_3.json",
                "output_4.json",
            ]
            results = self.execute_sql_file(sql_file, "dict")
            print(type(results))  # list
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
        root = ET.Element("Results")
        for i, (columns, rows) in enumerate(results):
            query_element = ET.SubElement(root, f"Query_{i+1}")
            for row in rows:
                row_element = ET.SubElement(query_element, "Row")
                for col, val in zip(columns, row):
                    col_element = ET.SubElement(row_element, col)
                    col_element.text = str(val)
        return ET.tostring(root, encoding="utf-8").decode("utf-8")
