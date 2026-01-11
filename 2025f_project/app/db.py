import sqlite3
from contextlib import contextmanager
from pathlib import Path

@contextmanager
def db_session(db_path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        yield conn
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise
    finally:
        conn.close()

def init_db(db_path="instance/app.sqlite"):
    schema_path = Path(__file__).resolve().parent / "sql" / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError("schema.sql not found in app/sql/")

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with db_session(db_path) as conn:
        conn.executescript(schema_path.read_text(encoding="utf-8"))

def get_table(table_name, db_path="instance/app.sqlite"):
    # Whitelist allowed tables to avoid SQL injection
    allowed = {"emails", "users", "settings"}  # <-- change to real table names
    if table_name not in allowed:
        raise ValueError("Invalid table name.")

    with db_session(db_path) as conn:
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        table_data = cursor.fetchall()
        return table_data
