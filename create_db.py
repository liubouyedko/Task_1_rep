import psycopg2
import logging
import json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import OperationalError, InterfaceError


connection = None


# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def execute_sql_file(filename, connection):
    with open(filename, "r") as file:
        sql = file.read()
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()


def create_database(user, password, host, port, dbname):
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


def create_tables(user, password, host, port, dbname):
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
        logging.exception(
            f"The error '{e}' occured during oppening JSON file: {json_file_path}"
        )
