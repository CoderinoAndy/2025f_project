import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_DEFAULT = "instance/app.sqlite"
LOCAL_USER_EMAIL = "you@example.com"
ALLOWED_TABLES = {"emails"}
ALLOWED_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}

EMAIL_SELECT_SQL = """
SELECT
    m.id,
    m.external_id,
    m.thread_id,
    m.title,
    m.sender,
    (
        SELECT group_concat(r.address, ', ')
        FROM (
            SELECT er.address
            FROM email_recipients er
            WHERE er.email_id = m.id AND er.recipient_type = 'to'
            ORDER BY er.id ASC
        ) AS r
    ) AS recipients,
    (
        SELECT group_concat(r.address, ', ')
        FROM (
            SELECT er.address
            FROM email_recipients er
            WHERE er.email_id = m.id AND er.recipient_type = 'cc'
            ORDER BY er.id ASC
        ) AS r
    ) AS cc,
    m.body,
    m.type,
    m.priority,
    m.is_read,
    m.received_at,
    m.summary,
    m.draft
FROM email_messages m
"""


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
    if "received_at" in data and "date" not in data:
        data["date"] = data["received_at"]
    return data


def _split_addresses(raw_value):
    if raw_value is None:
        return []
    text = str(raw_value).replace(";", ",")
    addresses = []
    seen = set()
    for chunk in text.split(","):
        address = chunk.strip()
        if not address:
            continue
        key = address.lower()
        if key in seen:
            continue
        seen.add(key)
        addresses.append(address)
    return addresses


def _insert_recipients(conn, email_id, recipient_type, raw_value):
    for address in _split_addresses(raw_value):
        conn.execute(
            """
            INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
            VALUES (?, ?, ?)
            """,
            (email_id, recipient_type, address),
        )


def get_table(table_name, db_path=DB_DEFAULT):
    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name.")
    if table_name == "emails":
        return fetch_emails(db_path=db_path)
    return []


def fetch_emails(email_type=None, db_path=DB_DEFAULT):
    clause = ""
    params = []
    if email_type:
        clause = "WHERE m.type = ?"
        params.append(email_type)
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            {EMAIL_SELECT_SQL}
            {clause}
            ORDER BY m.received_at DESC, m.id DESC
            """,
            params,
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def fetch_email_by_id(email_id, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            {EMAIL_SELECT_SQL}
            WHERE m.id = ?
            """,
            (email_id,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None


def fetch_thread_emails(thread_id, db_path=DB_DEFAULT):
    if not thread_id:
        return []
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            {EMAIL_SELECT_SQL}
            WHERE m.thread_id = ?
            ORDER BY m.received_at ASC, m.id ASC
            """,
            (thread_id,),
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def mark_read(email_id, read=True, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute(
            "UPDATE email_messages SET is_read = ? WHERE id = ?",
            (1 if read else 0, email_id),
        )


def set_email_type(email_id, new_type, db_path=DB_DEFAULT):
    if new_type not in ALLOWED_TYPES:
        raise ValueError("Invalid email type.")
    with db_session(db_path) as conn:
        conn.execute("UPDATE email_messages SET type = ? WHERE id = ?", (new_type, email_id))


def toggle_read_state(email_id, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute(
            """
            UPDATE email_messages
            SET is_read = 1 - is_read
            WHERE id = ?
            """,
            (email_id,),
        )


def update_draft(email_id, draft_text, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute("UPDATE email_messages SET draft = ? WHERE id = ?", (draft_text, email_id))


def create_reply_email(source_email_id, reply_text, recipients, cc, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        cur = conn.execute(
            """
            SELECT id, thread_id, title, priority
            FROM email_messages
            WHERE id = ?
            """,
            (source_email_id,),
        )
        source = cur.fetchone()
        if source is None:
            raise ValueError("Source email not found.")

        thread_id = source["thread_id"] or f"thread-{source_email_id}"
        title = source["title"] or "No subject"
        if not title.lower().startswith("re:"):
            title = f"Re: {title}"

        priority = int(source["priority"] or 1)
        priority = max(1, min(3, priority))

        cur = conn.execute(
            """
            INSERT INTO email_messages (
                external_id,
                thread_id,
                title,
                sender,
                body,
                type,
                priority,
                is_read,
                received_at
            )
            VALUES (NULL, ?, ?, ?, ?, 'read-only', ?, 1, CURRENT_TIMESTAMP)
            """,
            (thread_id, title, LOCAL_USER_EMAIL, reply_text, priority),
        )

        new_email_id = cur.lastrowid
        _insert_recipients(conn, new_email_id, "to", recipients)
        _insert_recipients(conn, new_email_id, "cc", cc)
        return new_email_id


def delete_email(email_id, db_path=DB_DEFAULT):
    with db_session(db_path) as conn:
        conn.execute("DELETE FROM email_messages WHERE id = ?", (email_id,))

