import json
import logging
import os
import pandas as pd
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, connection as pg_connection


connection = None


# # Load environment variables
# dotenv_path = ".env"
# load_dotenv(dotenv_path=dotenv_path)


# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


# # Database configurations
# dbname = os.getenv("DB_NAME")
# user = os.getenv("DB_USER")
# password = os.getenv("DB_PASSWORD")
# host = os.getenv("DB_HOST")
# port = os.getenv("DB_PORT")


class DatabaseManager:
    # def __init__(self, dbname, user, password, host, port):
    #     self.dbname = dbname
    #     self.user = user
    #     self.password = password
    #     self.host = host
    #     self.port = port
    #     self.connection: Optional[pg_connection] = None

    def __init__(self) -> None:
        dotenv_path = ".env"
        load_dotenv(dotenv_path=dotenv_path)

        # Database configurations
        dbname = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")

        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection: Optional[pg_connection] = None

    # Creating connection if not exists
    def create_connection(self) -> psycopg2.extensions.connection:
        global connection
        if connection is None or connection.closed:
            try:
                connection = psycopg2.connect(
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                )
                logging.info(f"Connection to PostgreSQL DB {self.dbname} successful")
            except (OperationalError, InterfaceError) as e:
                logging.exception(f"Error connecting to {self.dbname}: '{e}'")
        else:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1;")
            except (OperationalError, InterfaceError):
                try:
                    connection = psycopg2.connect(
                        dbname=self.dbname,
                        user=self.user,
                        password=self.password,
                        host=self.host,
                        port=self.port,
                    )
                    logging.info(f"Reconnection to PostgreSQL {self.dbname} successful")
                except (OperationalError, InterfaceError) as e:
                    logging.exception(
                        f"The error '{e}' occurred during reconnecting to {self.dbname}"
                    )
        return connection

    def execute_sql_file(
        self, filename: str, connection
    ) -> psycopg2.extensions.connection:
        with open(filename, "r") as file:
            sql = file.read()
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()
        return connection

    def create_database(self) -> psycopg2.extensions.cursor:
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        try:
            cursor.execute(f"CREATE DATABASE {self.dbname}")
            logging.info(f"Database '{self.dbname}' created successfully.")
        except psycopg2.errors.DuplicateDatabase:
            logging.info(f"Database '{self.dbname}' already exists.")

        cursor.close()
        conn.close()
        return cursor

    def create_tables(self) -> bool:
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        cursor = conn.cursor()

        try:
            DatabaseManager.execute_sql_file(self, "db_schema.sql", conn)
            conn.commit()
            logging.info("Tables created successfully.")
            success = True
        except psycopg2.errors.UndefinedTable as e:
            success = False
            conn.rollback()
            logging.error(f"Error creating tables: {e}")

        cursor.close()
        conn.close()
        return success


class DataLoader:
    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db_manager = db_manager

    def load_data_from_json(self, connection, json_file_path, table_name):
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
