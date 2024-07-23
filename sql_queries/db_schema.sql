CREATE TABLE IF NOT EXISTS Room (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    );

CREATE TABLE IF NOT EXISTS Student (
        birthday TIMESTAMP NOT NULL,
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        room INTEGER NOT NULL,
        sex CHAR(1) NOT NULL,
        CONSTRAINT fk_room_id FOREIGN KEY (room) REFERENCES Room(id)
    );
