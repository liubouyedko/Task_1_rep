import json
import logging
from typing import Optional

import psycopg2

from db_manager import DatabaseManager


class DataLoader:
    """
    A class to handle loading data into a PostgreSQL database from JSON files.

    The DataLoader class is responsible for reading data from JSON files and
    inserting it into specified tables within a PostgreSQL database. It supports
    inserting data into 'student' and 'room' tables, handling conflicts by ignoring
    duplicate entries based on the 'id' field.

    Attributes:
        db_manager (DatabaseManager): An instance of the DatabaseManager class to manage database connections.

    Methods:
        load_data_from_json(connection: Optional[psycopg2.extensions.connection], json_file_path: str, table_name: str):
            Loads data from a specified JSON file into the given table in the PostgreSQL database.
    """

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db_manager = db_manager

    def load_data_from_json(
        self,
        connection: Optional[psycopg2.extensions.connection],
        json_file_path: str,
        table_name: str,
    ) -> None:
        """
        Loads data from a specified JSON file into the given table in the PostgreSQL database.

        This method reads data from a JSON file and inserts it into the specified table. It supports
        inserting data into 'student' and 'room' tables. If a connection to the database is not provided,
        it logs an error. The method handles any IOErrors that occur during file reading and logs the
        exception.

        Args:
            connection (psycopg2.extensions.connection): The database connection object.
            json_file_path (str): The path to the JSON file containing the data.
            table_name (str): The name of the table to insert the data into.

        Raises:
            IOError: If there is an error opening the JSON file.
        """
        if connection is None:
            logging.error(
                f"Failed to load data into {table_name}: No connection to the DB."
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

                    records = [
                        (
                            record.get("birthday", None),
                            record.get("id", None),
                            record.get("name", None),
                            record.get("room", None),
                            record.get("sex", None),
                        )
                        for record in data
                    ]

                    with connection.cursor() as cursor:
                        cursor.executemany(insert_query, records)
                        connection.commit()

                elif table_name == "room":
                    insert_query = (
                        f"INSERT INTO {table_name} (id, name) "
                        f"VALUES (%s, %s) "
                        f"ON CONFLICT (id) DO NOTHING"
                    )

                    records = [
                        (record.get("id", None), record.get("name", None))
                        for record in data
                    ]

                    with connection.cursor() as cursor:
                        cursor.executemany(insert_query, records)
                        connection.commit()

        except IOError as e:
            logging.exception(f"'{e}' occurred during opening {json_file_path}")
