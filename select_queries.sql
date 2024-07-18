SELECT
        room.id,
        room.name,
        COUNT(student.id)
FROM room
LEFT JOIN student
ON room.id = student.room
GROUP BY room.id
ORDER BY room.id ASC;


SELECT
        room.id,
        room.name,
        ROUND(AVG(date_part('year', age(current_date, student.birthday)))::numeric, 2) AS average_age
FROM room
LEFT JOIN student ON room.id = student.room
GROUP BY room.id, room.name
ORDER BY average_age
LIMIT 5;


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


SELECT room.id, room.name
FROM room
LEFT JOIN student
ON room.id = student.room
GROUP BY room.id
HAVING COUNT(DISTINCT student.sex) > 1
ORDER BY room.id ASC;