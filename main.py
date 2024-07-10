import logging
from create_db import (
    create_database,
    create_tables,
    create_connection,
    load_data_from_json,
)
from execute_queries import execute_sql_file, export_to_json


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

    # сделать считывание названия файлов из консоли
    json_file_path_students = "students.json"
    json_file_path_rooms = "rooms.json"

    load_data_from_json(connection, json_file_path_rooms, "room")
    load_data_from_json(connection, json_file_path_students, "student")

    # Выполнение SQL-запросов и экспорт результатов
    try:
        sql_file = "select_queries.sql"
        output_files = [
            "output_1.json",
            "output_2.json",
            "output_3.json",
            "output_4.json",
        ]
        results = execute_sql_file(sql_file, connection)
        export_to_json(results, output_files)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
