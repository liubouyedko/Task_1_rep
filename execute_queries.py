import json
import logging
from decimal import Decimal
from typing import Any, List, Tuple

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

    def execute_sql_file(self, sql_file: str) -> List[List[Tuple[Any, ...]]]:
        with open(sql_file, "r") as file:
            sql = file.read()

        cursor = self.connection.cursor()
        queries = sql.split(";")  # Разделение запросов по комментариям
        results = []

        for query in queries:
            query = query.strip()
            if query:  # Проверка, что запрос не пустой
                try:
                    cursor.execute(query)
                    results.append(cursor.fetchall())
                except psycopg2.Error as e:
                    logging.error(f"Error executing query: {query}")
                    logging.error(f"Database error: {e}")
                    raise

        cursor.close()
        return results

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

    def export_to_xml(self, data: List[Any], output_files: List[Any]) -> None:
        for table, output_file in zip(data, output_files):
            root = BeautifulSoup(features="xml")
            xml_data = root.new_tag("data")

            for row in table:
                row_tag = root.new_tag("row")
                for col, value in enumerate(row):
                    col_tag = root.new_tag(f"col_{col}")
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
            results = self.execute_sql_file(sql_file)
            self.export_to_json(results, output_files_json)

        elif format == "xml":
            output_files_xml = [
                "output_1.xml",
                "output_2.xml",
                "output_3.xml",
                "output_4.xml",
            ]
            results = self.execute_sql_file(sql_file)
            self.export_to_xml(results, output_files_xml)
