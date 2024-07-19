import json
import logging
import os
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import connection as pg_connection

connection = None

# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


class DatabaseManager:
    """
    A class to manage PostgreSQL database connections and operations.

    The DatabaseManager class handles the creation of database connections,
    execution of SQL files, creation of databases, and creation of tables.
    It loads database configuration details from environment variables defined
    in a .env file.

    Attributes:
        dbname (str): The name of the database.
        user (str): The username to connect to the database.
        password (str): The password to connect to the database.
        host (str): The host address of the database.
        port (str): The port number on which the database server is listening.
        connection (Optional[psycopg2.extensions.connection]): The database connection object.

    Methods:
        create_connection() -> psycopg2.extensions.connection:
            Creates and returns a connection to the PostgreSQL database.

        execute_sql_file(filename: str, connection: psycopg2.extensions.connection) -> psycopg2.extensions.connection:
            Executes a SQL file using the provided database connection.

        create_database() -> psycopg2.extensions.cursor:
            Creates the database specified in the dbname attribute if it doesn't already exist.

        create_tables() -> bool:
            Creates tables in the PostgreSQL database using a SQL schema file.
    """

    def __init__(self) -> None:
        """
        Initializes the DatabaseManager instance by loading database configuration
        from a .env file and setting up the necessary attributes.
        """
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

    def create_connection(self) -> psycopg2.extensions.connection:
        """
        Establishes and returns a connection to the PostgreSQL database.

        This method checks if a global connection object already exists and is open.
        If the connection is not established or is closed, it attempts to create a new connection.
        If the connection is active, it verifies the connection by executing a simple query.
        In case of connection failure, it logs the exception and tries to reconnect.

        Returns:
            psycopg2.extensions.connection: The connection object to the PostgreSQL database.

        Raises:
            OperationalError: If there is an issue connecting to the database.
            InterfaceError: If there is an interface-related error while connecting to the database.

        Logging:
            Logs a message indicating whether the connection was successful or if an error occurred.
        """
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
                logging.info(f"Connection to PostgreSQL {self.dbname} successful")
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
        self, filename: str, connection: psycopg2.extensions.connection
    ) -> psycopg2.extensions.connection:
        """
        Executes a SQL file using the provided database connection.

        Reads the SQL file and executes its contents using the provided
        database connection.

        Args:
            filename (str): The path to the SQL file.
            connection (psycopg2.extensions.connection): The database connection object.

        Returns:
            psycopg2.extensions.connection: The database connection object after executing the SQL file.
        """
        with open(filename, "r") as file:
            sql = file.read()
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()
        return connection

    def create_database(self) -> psycopg2.extensions.cursor:
        """
        Creates the database specified in the dbname attribute if it doesn't already exist.

        Connects to the PostgreSQL server and attempts to create a database with the
        name specified in the dbname attribute. If the database already exists, it logs
        an informational message.

        Returns:
            psycopg2.extensions.cursor: The cursor object used for database operations.

        Raises:
            psycopg2.errors.DuplicateDatabase: If the database already exists.
        """
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
        """
        Creates tables in the PostgreSQL database using a SQL schema file.

        This method connects to the PostgreSQL database specified in the instance's attributes and
        executes SQL commands from the "db_schema.sql" file to create the necessary tables. If the
        tables are created successfully, a commit is performed, and a log message is recorded.
        If there is an error, such as an undefined table, the transaction is rolled back, and an
        error message is logged.

        Returns:
            bool: True if the tables were created successfully, False if an error occurred.

        Raises:
            psycopg2.Error: If there is an error during the connection or table creation process.
        """
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
        """
        Initializes the DataLoader instance with a DatabaseManager object.

        Args:
            db_manager (DatabaseManager): An instance of the DatabaseManager class to manage database connections.
        """
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
