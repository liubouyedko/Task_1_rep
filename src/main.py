import argparse
import logging

from dotenv import load_dotenv

from data_loader import DataLoader
from db_manager import DatabaseManager
from execute_queries import DataExporter

dotenv_path = "./.env"
load_dotenv(dotenv_path=dotenv_path)

# Logging configurations
logging.basicConfig(
    level=logging.INFO,
    filename="./py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def main(students_file_path: str, rooms_file_path: str, output_format: str) -> None:
    """
    Main function that initializes the database, loads data from JSON files into the database,
    and exports the results to the specified format.

    This function performs the following steps:
    1. Initializes the `DatabaseManager` and `DataLoader` instances.
    2. Creates the database and tables.
    3. Loads data from the provided JSON files into the specified database tables.
    4. Creates indexes in the database based on a provided SQL file.
    5. Exports query results to JSON or XML format based on the specified output format.

    Args:
        students_file_path (str): The path to the JSON file containing student data.
        rooms_file_path (str): The path to the JSON file containing room data.
        output_format (str): The format for exporting results, either "json" or "xml".
    """
    db_manager = DatabaseManager()
    data_loader = DataLoader(db_manager)

    db_manager.create_database()
    db_manager.create_tables()
    connection = db_manager.create_connection()

    sql_file = "./sql_queries/select_queries.sql"

    data_loader.load_data_from_json(connection, rooms_file_path, "room")
    data_loader.load_data_from_json(connection, students_file_path, "student")

    data_exporter = DataExporter(connection)
    data_exporter.create_indexes_from_sql_file("./sql_queries/create_indexes.sql")
    data_exporter.export_result(output_format, sql_file)

    connection.close()


if __name__ == "__main__":
    """
    Entry point for the script. Parses command-line arguments and invokes the `main` function.

    Command-line arguments:
        - students_file_path: The path to the JSON file containing student data.
        - rooms_file_path: The path to the JSON file containing room data.
        - output_format: The format for exporting results, either "json" or "xml".
    """
    parser = argparse.ArgumentParser(description="Python script with arguments")

    parser.add_argument("students_file_path", type=str, help="Student file path")
    parser.add_argument("rooms_file_path", type=str, help="Rooms file path")
    parser.add_argument("output_format", type=str, help="Output file format")

    args = parser.parse_args()

    main(args.students_file_path, args.rooms_file_path, args.output_format)
