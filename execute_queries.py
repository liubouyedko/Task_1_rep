import psycopg2
import json
import logging
from decimal import Decimal
from bs4 import BeautifulSoup


# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def execute_sql_file(sql_file, connection):
    with open(sql_file, "r") as file:
        sql = file.read()

    cursor = connection.cursor()
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


def export_to_json(data, output_files):
    def default_serializer(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(
            f"Object of type {obj.__class__.__name__} is not JSON serializable"
        )

    for result, output_file in zip(data, output_files):
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(
                result, file, ensure_ascii=False, indent=4, default=default_serializer
            )


def export_to_xml(data, output_files):
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


def export_result(format, sql_file, connection):
    if format == "json":
        output_files_json = [
            "output_1.json",
            "output_2.json",
            "output_3.json",
            "output_4.json",
        ]
        results = execute_sql_file(sql_file, connection)
        export_to_json(results, output_files_json)

    elif format == "xml":
        output_files_xml = [
            "output_1.xml",
            "output_2.xml",
            "output_3.xml",
            "output_4.xml",
        ]
        results = execute_sql_file(sql_file, connection)
        export_to_xml(results, output_files_xml)
