import logging
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import sessionmaker


dotenv_path = "config.env"
load_dotenv(dotenv_path=dotenv_path)


# logging config
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


DATABASE_URL = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def execute_sql_file(filename, session):
    with open(filename, "r") as file:
        sql = file.read()

    with session.begin():
        session.execute(text(sql))


def create_database():
    engine_default = create_engine(
        f"postgresql+pg8000://{user}:{password}@{host}:{port}/postgres"
    )
    SessionDefault = sessionmaker(bind=engine_default)
    session_default = SessionDefault()

    try:
        query = f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{dbname}'"
        exists = session_default.execute(text(query)).scalar()

        if not exists:
            session_default.execute(text(f"CREATE DATABASE {dbname}"))
            logging.info(f"Database '{dbname}' created successfully.")
        else:
            logging.info(f"Database '{dbname}' already exists.")

    except SQLAlchemyError as e:
        logging.error(f"Error creating database '{dbname}': {e}")

    finally:
        session_default.close()


def create_tables():
    session = Session()

    try:
        execute_sql_file("db_schema.sql", session)
        logging.info("Tables created successfully")
    except SQLAlchemyError as e:
        logging.error(f"Error creating tables: {e}")

    finally:
        session.close()


def create_connection():
    try:
        session = Session()
        logging.info(f"Connected to database '{dbname}' successfully.")
        return session
    except SQLAlchemyError as e:
        logging.error(f"Error connecting to database '{dbname}': {e}")
        return None


def load_data_from_json(session, json_file_path, table_name):
    if session is None:
        logging.error(
            f"Failed to load data into {table_name}: No connection to the database"
        )
        return

    try:
        with open(json_file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

            if table_name == "student":
                insert_query = (
                    f"INSERT INTO {table_name} (birthday, id, name, room, sex) "
                    f"VALUES (:birthday, :id, :name, :room, :sex) "
                    f"ON CONFLICT (id) DO NOTHING"
                )

                for record in data:
                    session.execute(
                        text(insert_query),
                        {
                            "birthday": record.get("birthday"),
                            "id": record.get("id"),
                            "name": record.get("name"),
                            "room": record.get("room"),
                            "sex": record.get("sex"),
                        },
                    )

            elif table_name == "room":
                insert_query = (
                    f"INSERT INTO {table_name} (id, name) "
                    f"VALUES (:id, :name) "
                    f"ON CONFLICT (id) DO NOTHING"
                )

                for record in data:
                    session.execute(
                        text(insert_query),
                        {
                            "id": record.get("id"),
                            "name": record.get("name"),
                        },
                    )
            session.commit()
            logging.info(f"Data '{table_name}' loaded into database successfully")

    except IOError as e:
        logging.error(
            f"The error '{e}' occurred during opening JSON file: {json_file_path}"
        )
    except IntegrityError as e:
        logging.error(f"Data integrity error: {e}")
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")
