import json
import logging
from decimal import Decimal
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.row import Row
from dotenv import load_dotenv
import os


dotenv_path = "config.env"
load_dotenv(dotenv_path=dotenv_path)


# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")

DATABASE_URL = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def execute_sql_file(sql_file):
    session = Session()
    results = []
    try:
        with open(sql_file, "r") as file:
            sql = file.read()
        queries = sql.split(";")
        for query in queries:
            query = query.strip()
            if query:
                result = session.execute(text(query)).fetchall()
                results.append(result)
                logging.info("SQL query executed successfully")
    except SQLAlchemyError as e:
        logging.error(f"Error executing query: {query}")
        logging.error(f"Database error: {e}")
        raise
    finally:
        session.close()
    return results


def default_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Convert Decimal to float
    if isinstance(obj, Row):
        return dict(obj.items())  # Convert Row to dictionary
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def export_to_json(data, output_files):
    serialized_data = []
    for result, output_file in zip(data, output_files):
        serialized_result = []
        for row in result:
            serialized_row = default_serializer(row)
            serialized_result.append(serialized_row)
        serialized_data.append(serialized_result)
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(serialized_result, file, ensure_ascii=False, indent=4)


def export_to_xml(data, output_files):
    for table, output_file in zip(data, output_files):
        root = BeautifulSoup(features="xml")
        xml_data = root.new_tag("data")

        for row in table:
            row_tag = root.new_tag("row")
            for col, value in row.items():
                col_tag = root.new_tag(str(col))
                col_tag.string = str(value)
                row_tag.append(col_tag)
            xml_data.append(row_tag)

        root.append(xml_data)

        with open(output_file, "w", encoding="utf-8") as file:
            file.write(str(root.prettify()))


def export_result(format, sql_file):
    if format.lower() == "json":
        output_files_json = [
            "output_1.json",
            "output_2.json",
            "output_3.json",
            "output_4.json",
        ]
        results = execute_sql_file(sql_file)
        results_as_dicts = []
        for res in results:
            result_dicts = [dict(row.items()) for row in res]
            results_as_dicts.append(result_dicts)
        export_to_json(results_as_dicts, output_files_json)
        logging.info(f"Data exported to '{format}' format successfully")

    elif format.lower() == "xml":
        output_files_xml = [
            "output_1.xml",
            "output_2.xml",
            "output_3.xml",
            "output_4.xml",
        ]
        results = execute_sql_file(sql_file)
        export_to_xml(results, output_files_xml)
        logging.info(f"Data exported to '{format}' format successfully")

    else:
        logging.error("Unknown file format")
        print("Unknown file format")
