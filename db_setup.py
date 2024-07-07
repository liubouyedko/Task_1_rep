import psycopg2
import logging
from psycopg2 import OperationalError, InterfaceError, sql

# loggin config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


connection = None


# creating connection if not exists
def create_connection(db_name, db_user, db_password, db_host, db_port):
    global connection
    if connection is None or connection.closed:
        try:
            connection = psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
            )
            logging.info(f"Connection to PostgreSQL DB {db_name} successful")
        except (OperationalError, InterfaceError) as e:
            logging.exception(
                f"The error '{e}' occurred during connecting to {db_name}"
            )
    else:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
        except (OperationalError, InterfaceError):
            try:
                connection = psycopg2.connect(
                    database=db_name,
                    user=db_user,
                    password=db_password,
                    host=db_host,
                    port=db_port,
                )
                logging.info(f"Reconnection to PostgreSQL DB {db_name} successful")
            except (OperationalError, InterfaceError) as e:
                logging.exception(
                    f"The error '{e}' occurred during reconnecting to {db_name}"
                )
    return connection


def execute_query(connection, query):
    if connection is None:
        logging.error("Failed to execute query: No connection to the database.")
        return
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
    except OperationalError as e:
        logging.exception(f"The error '{e}' occurred")
    finally:
        cursor.close()


# creating DB if not exists
def create_database(admin_connection, db_name):
    if admin_connection is None:
        logging.error(
            "Failed to create database: No connection to the PostgreSQL server."
        )
        return
    admin_connection.autocommit = True
    cursor = admin_connection.cursor()
    try:
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (db_name,)
        )
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(sql.SQL(f"CREATE DATABASE {db_name}"))
            logging.info(f"Database {db_name} created successfully")
        else:
            logging.info(f"Database {db_name} already exists")
    except OperationalError as e:
        logging.exception(f"The error '{e}' occurred during database creation")
    finally:
        cursor.close()


# checking if table exists
def check_table_exists(connection, table_name):
    if connection is None:
        logging.error("Failed to check table: No connection to the database.")
        return False
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);",
            (table_name,),
        )
        exists = cursor.fetchone()[0]
    except Exception as e:
        logging.exception(f"Error checking if table '{table_name}' exists: {e}")
        exists = False
    finally:
        cursor.close()
    return exists


if __name__ == "__main__":

    admin_connection = create_connection(
        "postgres", "postgres", "qwerty123", "127.0.0.1", "5432"
    )

    create_database(admin_connection, "db_students")

    if admin_connection:
        admin_connection.close()

    connection = create_connection(
        "db_students", "postgres", "qwerty123", "127.0.0.1", "5432"
    )

    create_students_table = """
    CREATE TABLE IF NOT EXISTS Student (
        birthday TIMESTAMP, 
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        room INTEGER,
        sex CHAR(1)
    )
    """
    execute_query(connection, create_students_table)

    create_rooms_table = """
    CREATE TABLE IF NOT EXISTS Room (
        id INTEGER PRIMARY KEY,
        name VARCHAR(15)
    )
    """
    execute_query(connection, create_rooms_table)

    if connection:
        connection.close()
