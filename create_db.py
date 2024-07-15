import json
import logging
import os
import sqlite3

import psycopg2
from dotenv import load_dotenv
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

connection = None


dotenv_path = ".env"
load_dotenv(dotenv_path=dotenv_path)


# Logging config
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


def execute_sql_file(filename, connection) -> None:
    with open(filename, "r") as file:
        sql = file.read()
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()
    return connection


def create_database() -> None:
    conn = psycopg2.connect(
        dbname="postgres", user=user, password=password, host=host, port=port
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    try:
        cursor.execute(f"CREATE DATABASE {dbname}")
        logging.info(f"Database '{dbname}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        logging.info(f"Database '{dbname}' already exists.")

    cursor.close()
    conn.close()


def create_tables() -> None:
    conn = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )
    cursor = conn.cursor()

    try:
        execute_sql_file("db_schema.sql", conn)
        conn.commit()
        logging.info("Tables created successfully.")
    except psycopg2.errors.UndefinedTable as e:
        conn.rollback()
        logging.error(f"Error creating tables: {e}")

    cursor.close()
    conn.close()


# creating connection if not exists
def create_connection() -> sqlite3.Connection:
    global connection
    if connection is None or connection.closed:
        try:
            connection = psycopg2.connect(
                database=dbname,
                user=user,
                password=password,
                host=host,
                port=port,
            )
            logging.info(f"Connection to PostgreSQL DB {dbname} successful")
        except (OperationalError, InterfaceError) as e:
            logging.exception(f"Error connecting to {dbname}: '{e}'")
    else:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
        except (OperationalError, InterfaceError):
            try:
                connection = psycopg2.connect(
                    database=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port,
                )
                logging.info(f"Reconnection to PostgreSQL {dbname} successful")
            except (OperationalError, InterfaceError) as e:
                logging.exception(
                    f"The error '{e}' occurred during reconnecting to {dbname}"
                )
    return connection


def load_data_from_json(connection, json_file_path, table_name) -> None:
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
        logging.exception(f"'{e}' occured during oppening {json_file_path}")
