import logging
import sys
import os
from dotenv import load_dotenv
from create_db import (
    create_database,
    create_tables,
    create_connection,
    load_data_from_json,
)
from execute_queries import export_result
from sqlalchemy import create_engine

dotenv_path = "config.env"
load_dotenv(dotenv_path=dotenv_path)

# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def main():
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_string = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{dbname}"
    db = create_engine(db_string)

    # with db.connect() as connection:
    #     connection.execute()

    logging.basicConfig(level=logging.INFO)
    create_database()
    create_tables()
    connection = create_connection()

    sql_file = os.getenv("SQL_FILE")

    if len(sys.argv) < 2:
        print("Enter the path to your json file (students.json):")
        json_file_path_students = input()
    else:
        json_file_path_students = sys.argv[1]

    if len(sys.argv) < 3:
        print("Enter the path to your json file (rooms.json):")
        json_file_path_rooms = input()
    else:
        json_file_path_rooms = sys.argv[2]

    # # json_file_path_rooms = input("Enter the path to your json file (rooms.json): ")

    load_data_from_json(connection, json_file_path_rooms, "room")
    load_data_from_json(connection, json_file_path_students, "student")

    format = input("Enter a file format for data export(json/xml): ")

    export_result(format, sql_file)

    connection.close()
    db.dispose()


if __name__ == "__main__":
    main()
