import json
import logging
from db_setup import create_connection


# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def load_data_from_json(connection, json_file_path, table_name):
    if connection is None:
        logging.error(
            f"Failed to load data into {table_name}: No connection to the database."
        )
        return

    try:
        with open(json_file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

            if table_name == "student":
                insert_query = (
                    f"INSERT INTO {table_name} (birthday, id, name, room, sex) "
                    f"VALUES (%s, %s, %s, %s, %s) "
                    f"ON CONFLICT (id) DO NOTHING"
                )

                with connection.cursor() as cursor:
                    for record in data:
                        cursor.execute(
                            insert_query,
                            (
                                record.get("birthday", None),
                                record.get("id", None),
                                record.get("name", None),
                                record.get("room", None),
                                record.get("sex", None),
                            ),
                        )
                    connection.commit()

            elif table_name == "room":
                insert_query = (
                    f"INSERT INTO {table_name} (id, name) "
                    f"VALUES (%s, %s) "
                    f"ON CONFLICT (id) DO NOTHING"
                )

                with connection.cursor() as cursor:
                    for record in data:
                        cursor.execute(
                            insert_query,
                            (
                                record.get("id", None),
                                record.get("name", None),
                            ),
                        )
                    connection.commit()

    except IOError as e:
        logging.exception(f"The error '{e}' occured during oppening {json_file_path}")


if __name__ == "__main__":

    connection = create_connection(
        "db_students", "postgres", "qwerty123", "127.0.0.1", "5432"
    )

    json_file_path_students = "students.json"
    json_file_path_rooms = "rooms.json"

    load_data_from_json(connection, json_file_path_students, "student")
    load_data_from_json(connection, json_file_path_rooms, "room")

    if connection:
        connection.close()
