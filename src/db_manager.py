import logging
import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

connection = None

# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="./py_log.log",
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

    Methods:
        create_connection() -> psycopg2.extensions.connection:
            Creates and returns a connection to the PostgreSQL database.

        execute_sql_file(filename: str, connection: psycopg2.extensions.connection) -> psycopg2.extensions.connection:
            Executes a SQL file using the provided database connection.

        create_database(connection) -> psycopg2.extensions.cursor:
            Creates the database specified in the dbname attribute if it doesn't already exist.

        create_tables(connection) -> bool:
            Creates tables in the PostgreSQL database using a SQL schema file.
    """

    def __init__(self) -> None:
        dotenv_path = "./.env"
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
        try:
            cursor.execute(sql)
        except psycopg2.Error as e:
            logging.error(f"Error executing SQL file {filename}: {e}")
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            cursor.close()

        return connection

    def create_database(self, connection) -> psycopg2.extensions.cursor:
        """
        Creates the database specified in dbname if it doesn't exist.

        Args:
            connection (psycopg2.extensions.connection): The database connection object.

        Returns:
            psycopg2.extensions.cursor: The cursor object used for database operations.

        Logs:
            Success or existence of the database.
        """
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        try:
            cursor.execute(f"CREATE DATABASE {self.dbname}")
            logging.info(f"Database '{self.dbname}' created successfully.")
        except psycopg2.errors.DuplicateDatabase:
            logging.info(f"Database '{self.dbname}' already exists.")

        cursor.close()
        # connection.close()
        return cursor

    def create_tables(self, connection) -> bool:
        """
        Creates tables in the PostgreSQL database using the specified SQL schema file.

        Executes SQL commands from the "db_schema.sql" file to create the necessary tables.
        Commits the transaction if successful, otherwise rolls back on error.

        Args:
            connection (psycopg2.extensions.connection): The database connection object.

        Returns:
            bool: True if the tables were created successfully, False if an error occurred.
        """
        cursor = connection.cursor()

        try:
            self.execute_sql_file("./sql_queries/db_schema.sql", connection)
            connection.commit()
            logging.info("Tables created successfully.")
            success = True
        except psycopg2.errors.UndefinedTable as e:
            success = False
            connection.rollback()
            logging.error(f"Error creating tables: {e}")

        cursor.close()
        # connection.close()
        return success
