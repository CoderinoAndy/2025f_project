import sqlite3
from contextlib import contextmanager
from pathlib import Path

@contextmanager
def db_session(db_name):
    conn = sqlite3.connect(db_name)
    try:
        # Must be BEFORE you run queries
        conn.execute("PRAGMA foreign_keys = ON;")

        # Give the connection to the 'with' block
        yield conn

        # Only commit if the 'with' block succeeds
        conn.commit()

    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise

    finally:
        conn.close()

def init_db(db_name):
    schema_path = Path(__file__).with_name("schema.sql")
    if not schema_path.exists():
        raise FileNotFoundError("schema.sql not found next to db.py")

    with db_session(db_name) as conn:
        conn.executescript(schema_path.read_text(encoding="utf-8"))

if __name__ == "__main__":
    init_db("instance/app.sqlite")
    print("Database initialized.")