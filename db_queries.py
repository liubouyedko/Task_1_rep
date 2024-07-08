import logging
import json
from db_setup import create_connection, close_connection
from decimal import Decimal


# logging config
logging.basicConfig(
    level=logging.INFO,
    filename="py_log.log",
    filemode="w",
    format="%(process)d %(asctime)s %(levelname)s %(message)s",
)


def execute_query(query):
    connection = create_connection(
        "db_students", "postgres", "qwerty123", "127.0.0.1", "5432"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        logging.info("Query executed successfully")
        return result
    except Exception as e:
        logging.exception(f"The error '{e}' occured while executing the {query}")
        return None
    finally:
        cursor.close()
        close_connection()


def save_result_to_json(column_names, results, json_file_path):
    try:
        data = []
        for row in results:
            data.append(dict(zip(column_names, row)))

        # Преобразуем значения Decimal в сериализуемый формат
        data_serializable = json.loads(
            json.dumps(data, default=convert_decimal_to_serializable)
        )

        with open(json_file_path, "w") as f:
            json.dump(data_serializable, f, ensure_ascii=False, indent=4)

        print(f"Results saved to {json_file_path}")
    except Exception as e:
        print(f"An error occurred while saving to JSON: {e}")


def convert_decimal_to_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Преобразуем Decimal в float
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def get_column_names(table_name):
    query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    """
    result = execute_query(query)
    if result:
        return [row[0] for row in result]
    else:
        return []


if __name__ == "__main__":

    # 1
    # List of rooms and the number of students in each room
    sql_1 = """ 
        SELECT 
            room.id, 
            room.name, 
            COUNT(student.id)
        FROM room
        LEFT JOIN student
        ON room.id = student.room
        GROUP BY room.id
        ORDER BY room.id ASC;
    """
    column_names_1 = ["room_id", "room_name", "amount_of_students"]
    results_1 = execute_query(sql_1)

    save_result_to_json(
        column_names_1, results_1, "1_number_of_students_in_each_room.json"
    )

    # 2
    # 5 rooms with the smallest average age of students
    sql_2 = """
        SELECT 
            room.id,
            room.name,
            ROUND(AVG(date_part('year', age(current_date, student.birthday)))::numeric, 2) AS average_age
        FROM room
        LEFT JOIN student ON room.id = student.room
        GROUP BY room.id, room.name
        ORDER BY average_age
        LIMIT 5;
    """
    column_names_2 = ["room_id", "room_name", "average_age"]
    results_2 = execute_query(sql_2)
    save_result_to_json(column_names_2, results_2, "2_smallest_average_age.json")

    # 3
    # 5 rooms with the largest age difference among students
    sql_3 = """
        SELECT 
            room.id, 
            room.name, 
            ROUND(
                MAX(
                    EXTRACT(year FROM age(student.birthday)) +
                    EXTRACT(month FROM age(student.birthday)) / 12.0 +
                    EXTRACT(day FROM age(student.birthday)) / 365.25
                ) - 
                MIN(
                    EXTRACT(year FROM age(student.birthday)) +
                    EXTRACT(month FROM age(student.birthday)) / 12.0 +
                    EXTRACT(day FROM age(student.birthday)) / 365.25
                ), 2) AS age_difference
        FROM room
        LEFT JOIN student
        ON room.id = student.room
        GROUP BY room.id, room.name
        HAVING MAX(
                    EXTRACT(year FROM age(student.birthday)) +
                    EXTRACT(month FROM age(student.birthday)) / 12.0 +
                    EXTRACT(day FROM age(student.birthday)) / 365.25
                ) - 
                MIN(
                    EXTRACT(year FROM age(student.birthday)) +
                    EXTRACT(month FROM age(student.birthday)) / 12.0 +
                    EXTRACT(day FROM age(student.birthday)) / 365.25
                ) IS NOT NULL
        ORDER BY age_difference DESC
        LIMIT 5;
    """
    column_names_3 = ["room_id", "room_name", "age_difference"]
    results_3 = execute_query(sql_3)
    save_result_to_json(
        column_names_3,
        results_3,
        "3_largest_age_difference.json",
    )

    # 4
    # List of rooms where students of different genders live
    sql_4 = """
        SELECT room.id, room.name
        FROM room
        LEFT JOIN student
        ON room.id = student.room
        GROUP BY room.id
        HAVING COUNT(DISTINCT student.sex) > 1
        ORDER BY room.id ASC;
    """
    column_names_4 = ["room.id", "room.name"]
    results_4 = execute_query(sql_4)
    save_result_to_json(
        column_names_4, results_4, "4_rooms_with_different_genders.json"
    )
