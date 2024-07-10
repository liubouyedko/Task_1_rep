import psycopg2
import json
import logging
from decimal import Decimal


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
