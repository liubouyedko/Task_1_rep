# Data Import and Export System

This project provides a robust solution for importing and exporting data between JSON files and a PostgreSQL database.
The system is designed to handle student and room data, perform queries, and output results in JSON or XML format.
It utilizes object-oriented principles and SQL for database interactions, without relying on ORM.

## Project Overview

The project involves the following tasks:

1. **Database Schema Creation**: Design and implement a database schema that reflects the data in the provided JSON files.
2. **Data Import**: Load data from `students.json` and `rooms.json` into the PostgreSQL database.
3. **Query Execution**: Execute various SQL queries to generate reports based on the data.
4. **Data Export**: Export the results of the queries to JSON or XML format.
5. **Optimization**: Generate and apply SQL queries to create indexes for optimized performance.

## Features

- **Database Management**: Create databases, tables, and manage connections using SQL.
- **Data Loading**: Import data from JSON files into the PostgreSQL database.
- **Data Querying**: Execute complex queries to retrieve and analyze data.
- **Data Exporting**: Export query results in JSON or XML formats.
- **Index Optimization**: Create indexes to optimize query performance.

## Prerequisites

- Docker and Docker Compose
- PostgreSQL
- Python 3.10 or higher

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/liubouyedko/Task_1_rep.git
   cd Task_1_rep
   ```

3. **Set Up Docker**:

   Ensure you have Docker and Docker Compose installed.
   The project includes a `Dockerfile` and `docker-compose.yml` for easy setup.

4. **Create the `.env` File**:

   Create a `.env` file in the root directory with your database credentials:
   ```
   DB_NAME=db_students
   DB_USER=user_name
   DB_PASSWORD=user_password
   DB_HOST=db
   DB_PORT=5432
   ```
   Replace `[user_name]` and `[user_password]` with the your data to connect to Postgres.


6. **Build and Run Docker Container**:

   ```bash
   docker-compose build
   docker-compose up -d
   ```

## Usage

To run the data import and export script, use the following command:
```bash
docker-compose run app python src/main.py [path_to_students_json] [path_to_rooms_json] [output_format]
```
Replace `[path_to_students_json]` and `[path_to_rooms_json]` with the paths to your JSON files, and `[output_format]` with either `json` or `xml`.

**Example:**
```bash
docker-compose run app python src/main.py students.json rooms.json json
```

## Project Structure
* **sql_queries**:
  - `create_indexes.sql`: SQL file for creating indexes.
  - `db_schema.sql`: SQL file with `CREATE TABLE` statements.
  - `select_queries.sql`: SQL file with select queries.
* **src**:
  - `data_loader.py`: Data loading.
  - `db_manager.py': Database connection and schema creation.
  - `execute_queries.py`: Query execution and data export.
  - `main.py`: Main script for executing functions.
* **tests**:
  - `test_data_loader.py`: Unit tests for `data_loader.py`.
  - `test_db_manager.py`: Unit tests for `db_manager.py`.
  - `test_execute_queries.py`: Unit tests for `execute_queries.py`.
- `.dockerignore`: Docker ignore file.
- `.env`: Environment variables for database credentials.
- `.gitignore`: Git ignore file.
- `.pre-commit-config.yaml`: Configuration for pre-commit hooks.
- `docker-compose.yml`: Docker Compose configuration.
- `Dockerfile`: Docker image build configuration.
- `requirements.txt`: Required Python packages.

You will also need to have two files, `rooms.json` and `students.json`, with the schema according to `db_schema.sql`.
