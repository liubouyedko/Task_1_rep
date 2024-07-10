import logging
import json
from create_db import (
    create_database,
    create_tables,
    create_connection,
    load_data_from_json,
)
from execute_queries import export_result

# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def main():
    dbname = "db_students"
    user = "postgres"
    password = "qwerty123"
    host = "127.0.0.1"
    port = "5432"

    logging.basicConfig(level=logging.INFO)
    create_database(user, password, host, port, dbname)
    create_tables(user, password, host, port, dbname)
    connection = create_connection(dbname, user, password, host, port)

    sql_file = "select_queries.sql"

    json_file_path_students = input(
        "Enter the path to your json file (students.json): "
    )
    json_file_path_rooms = input("Enter the path to your json file (rooms.json): ")

    load_data_from_json(connection, json_file_path_rooms, "room")
    load_data_from_json(connection, json_file_path_students, "student")

    format = input("Enter a file format for data export(json/xml): ")

    export_result(format, sql_file, connection)

    connection.close()


if __name__ == "__main__":
    main()
