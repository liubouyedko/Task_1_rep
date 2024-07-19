import argparse
import logging
import os

from dotenv import load_dotenv

from create_db import DatabaseManager, DataLoader
from execute_queries import DataExporter

dotenv_path = ".env"
load_dotenv(dotenv_path=dotenv_path)

# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def main(students_file_path: str, rooms_file_path: str, output_format: str) -> None:
    print(f"Argument 1: {students_file_path}")
    print(f"Argument 2: {rooms_file_path}")
    print(f"Argument 3: {output_format}")

    db_manager = DatabaseManager()
    data_loader = DataLoader(db_manager)

    db_manager.create_database()
    db_manager.create_tables()
    connection = db_manager.create_connection()

    sql_file = os.getenv("SQL_FILE")

    data_loader.load_data_from_json(connection, rooms_file_path, "room")
    data_loader.load_data_from_json(connection, students_file_path, "student")

    data_exporter = DataExporter(connection)
    data_exporter.create_indexes_from_sql_file("create_indexes.sql")
    data_exporter.export_result(output_format, sql_file)

    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python script with arguments")

    parser.add_argument("students_file_path", type=str, help="Student file path")
    parser.add_argument("rooms_file_path", type=str, help="Rooms file path")
    parser.add_argument("output_format", type=str, help="Output file format")

    args = parser.parse_args()

    main(args.students_file_path, args.rooms_file_path, args.output_format)
