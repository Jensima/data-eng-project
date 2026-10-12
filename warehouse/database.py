import sqlite3
from pathlib import Path

# -------------------------------------------------------
# Database Location
# -------------------------------------------------------

DATABASE_NAME = "warehouse.db"
DATABASE_PATH = Path(__file__).parent / DATABASE_NAME
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


# -------------------------------------------------------
# Connection
# -------------------------------------------------------

def get_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# -------------------------------------------------------
# Initialize Database
# -------------------------------------------------------

def initialize_database():
    conn = get_connection()
    cursor = conn.cursor()
    with open(SCHEMA_PATH, "r") as f:
        cursor.executescript(f.read())

    conn.commit()
    conn.close()
    print("Database initialized successfully.")


# -------------------------------------------------------

if __name__ == "__main__":
    initialize_database()