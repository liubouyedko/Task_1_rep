import logging
import os

# import sys

from dotenv import load_dotenv

from create_db import DatabaseManager, DataLoader
from execute_queries import DataExporter

dotenv_path = ".env"
load_dotenv(dotenv_path=dotenv_path)

# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    json_file_path_students = os.getenv("JSON_FILE_PATH_STUDENTS")
    json_file_path_rooms = os.getenv("JSON_FILE_PATH_ROOMS")
    format = os.getenv("OUTPUT_FILE_FORMAT")

    print(host, port)

    db_manager = DatabaseManager(dbname, user, password, host, port)
    data_loader = DataLoader(db_manager)

    db_manager.create_database()
    db_manager.create_tables()
    connection = db_manager.create_connection()

    sql_file = os.getenv("SQL_FILE")

    # if len(sys.argv) < 2:
    #     print("Enter the path to your json file (students.json):")
    #     json_file_path_students = input()
    # else:

    # if len(sys.argv) < 3:
    #     print("Enter the path to your json file (rooms.json):")
    #     json_file_path_rooms = input()
    # else:

    # json_file_path_students = "students.json"
    # json_file_path_rooms = "rooms.json"

    data_loader.load_data_from_json(connection, json_file_path_rooms, "room")
    data_loader.load_data_from_json(connection, json_file_path_students, "student")

    data_exporter = DataExporter(connection)
    data_exporter.export_result(format, sql_file)

    connection.close()


if __name__ == "__main__":
    main()
