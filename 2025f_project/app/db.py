import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_DEFAULT = "instance/app.sqlite"
ALLOWED_TABLES = {"emails", "accounts", "email_labels", "email_label_links"}
ALLOWED_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}

@contextmanager
def db_session(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
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

def _row_to_dict(row):
    data = dict(row)
    if "is_read" in data:
        data["is_read"] = bool(data["is_read"])
    if "priority" in data and data["priority"] is not None:
        data["priority"] = int(data["priority"])
    if "title" not in data and "subject" in data:
        data["title"] = data["subject"]
    # Keep legacy template field names
    if "received_at" in data and "date" not in data:
        data["date"] = data["received_at"]
    return data

def get_table(table_name, db_path=DB_DEFAULT):
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name.")
    with db_session(db_path) as conn:
        cur = conn.execute(f"SELECT * FROM {table_name}")
        return [dict(r) for r in cur.fetchall()]

def fetch_emails(email_type=None, db_path=DB_DEFAULT):
    clause = ""
    params = []
    if email_type:
        clause = "WHERE type = ?"
        params.append(email_type)
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            SELECT *
            FROM emails
            {clause}
            ORDER BY received_at DESC, id DESC
            """,
            params,
        )
        return [_row_to_dict(r) for r in cur.fetchall()]

def fetch_email_by_id(email_id, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        cur = conn.execute("SELECT * FROM emails WHERE id = ?", (email_id,))
        row = cur.fetchone()
        return _row_to_dict(row) if row else None

def mark_read(email_id, read=True, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute("UPDATE emails SET is_read = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (1 if read else 0, email_id))

def set_email_type(email_id, new_type, db_path=DB_DEFAULT):
    if new_type not in ALLOWED_TYPES:
        raise ValueError("Invalid email type.")
    with db_session(db_path) as conn:
        conn.execute(
            "UPDATE emails SET type = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_type, email_id),
        )

def toggle_read_state(email_id, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute(
            """
            UPDATE emails
            SET is_read = 1 - is_read,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (email_id,),
        )

def update_draft(email_id, draft_text, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute(
            """
            UPDATE emails
            SET draft = ?, draft_updated_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (draft_text, email_id),
        )

def delete_email(email_id, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute("DELETE FROM emails WHERE id = ?", (email_id,))
