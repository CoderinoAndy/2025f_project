import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

DB_DEFAULT = "instance/app.sqlite"
LOCAL_USER_EMAIL = "you@example.com"
ALLOWED_TABLES = {"emails"}
ALLOWED_TYPES = {
    "response-needed",
    "read-only",
    "junk",
    "junk-uncertain",
    "sent",
    "draft",
}

EMAIL_SELECT_SQL = """
SELECT
    m.id,
    m.external_id,
    m.provider_draft_id,
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
    m.body_html,
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
        _apply_schema_migrations(conn)


def _apply_schema_migrations(conn):
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(email_messages)").fetchall()
    }
    definition_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='email_messages'"
    ).fetchone()
    definition_sql = (definition_row["sql"] or "").lower() if definition_row else ""
    needs_rebuild = (
        "body_html" not in columns
        or "provider_draft_id" not in columns
        or "'sent'" not in definition_sql
        or "'draft'" not in definition_sql
    )

    if needs_rebuild:
        _rebuild_email_tables(conn, columns)

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_email_messages_provider_draft_id
        ON email_messages(provider_draft_id)
        WHERE provider_draft_id IS NOT NULL
        """
    )


def _rebuild_email_tables(conn, existing_columns):
    body_html_expr = "body_html" if "body_html" in existing_columns else "NULL"
    provider_draft_expr = (
        "provider_draft_id" if "provider_draft_id" in existing_columns else "NULL"
    )

    conn.execute("PRAGMA foreign_keys = OFF;")
    conn.execute("ALTER TABLE email_messages RENAME TO email_messages_old;")

    conn.execute(
        """
        CREATE TABLE email_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          external_id TEXT UNIQUE,
          provider_draft_id TEXT UNIQUE,
          thread_id TEXT,
          title TEXT NOT NULL,
          sender TEXT NOT NULL,
          body TEXT,
          body_html TEXT,
          type TEXT NOT NULL DEFAULT 'read-only'
            CHECK (type IN ('response-needed','read-only','junk','junk-uncertain','sent','draft')),
          priority INTEGER NOT NULL DEFAULT 1
            CHECK (priority BETWEEN 1 AND 3),
          is_read INTEGER NOT NULL DEFAULT 0
            CHECK (is_read IN (0,1)),
          received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          summary TEXT,
          draft TEXT
        );
        """
    )

    conn.execute(
        f"""
        INSERT INTO email_messages (
            id, external_id, provider_draft_id, thread_id, title, sender, body, body_html,
            type, priority, is_read, received_at, summary, draft
        )
        SELECT
            id,
            external_id,
            {provider_draft_expr},
            thread_id,
            title,
            sender,
            body,
            {body_html_expr},
            CASE
                WHEN type IN ('response-needed','read-only','junk','junk-uncertain','sent','draft')
                    THEN type
                ELSE 'read-only'
            END,
            priority,
            is_read,
            received_at,
            summary,
            draft
        FROM email_messages_old
        """
    )

    conn.execute("ALTER TABLE email_recipients RENAME TO email_recipients_old;")
    conn.execute(
        """
        CREATE TABLE email_recipients (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email_id INTEGER NOT NULL,
          recipient_type TEXT NOT NULL
            CHECK (recipient_type IN ('to','cc')),
          address TEXT NOT NULL,
          FOREIGN KEY (email_id) REFERENCES email_messages(id) ON DELETE CASCADE,
          UNIQUE (email_id, recipient_type, address)
        );
        """
    )
    conn.execute(
        """
        INSERT INTO email_recipients (id, email_id, recipient_type, address)
        SELECT id, email_id, recipient_type, address
        FROM email_recipients_old
        """
    )

    conn.execute("DROP TABLE email_recipients_old;")
    conn.execute("DROP TABLE email_messages_old;")
    conn.execute("PRAGMA foreign_keys = ON;")


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


def fetch_emails(email_type=None, exclude_types=None, db_path=DB_DEFAULT):
    where_clauses = []
    params = []
    if email_type:
        where_clauses.append("m.type = ?")
        params.append(email_type)
    excluded = sorted(set(exclude_types or []))
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        where_clauses.append(f"m.type NOT IN ({placeholders})")
        params.extend(excluded)
    clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
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


def fetch_email_by_external_id(external_id, db_path=DB_DEFAULT):
    if not external_id:
        return None
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            {EMAIL_SELECT_SQL}
            WHERE m.external_id = ?
            """,
            (external_id,),
        )
        row = cur.fetchone()
        return _row_to_dict(row) if row else None


def fetch_email_by_provider_draft_id(provider_draft_id, db_path=DB_DEFAULT):
    if not provider_draft_id:
        return None
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            {EMAIL_SELECT_SQL}
            WHERE m.provider_draft_id = ?
            """,
            (provider_draft_id,),
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


def upsert_email_from_provider(email_data, db_path=DB_DEFAULT):
    external_id = (email_data.get("external_id") or "").strip()
    provider_draft_id = (email_data.get("provider_draft_id") or "").strip()
    if not external_id and not provider_draft_id:
        raise ValueError("external_id or provider_draft_id is required for provider sync.")

    message_type = email_data.get("type")
    if message_type not in ALLOWED_TYPES:
        message_type = "read-only"

    priority = int(email_data.get("priority") or 1)
    priority = max(1, min(3, priority))

    received_at = (email_data.get("received_at") or "").strip()
    if not received_at:
        received_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    normalized = {
        "thread_id": email_data.get("thread_id"),
        "provider_draft_id": provider_draft_id or None,
        "title": (email_data.get("title") or "(No subject)").strip(),
        "sender": (email_data.get("sender") or "unknown@unknown").strip(),
        "body": email_data.get("body") or "",
        "body_html": email_data.get("body_html"),
        "type": message_type,
        "priority": priority,
        "is_read": 1 if bool(email_data.get("is_read")) else 0,
        "received_at": received_at,
        "summary": email_data.get("summary"),
        "draft": email_data.get("draft"),
        "recipients": email_data.get("recipients"),
        "cc": email_data.get("cc"),
    }

    with db_session(db_path) as conn:
        cur = conn.execute(
            """
            SELECT id, summary, draft, body_html, provider_draft_id, type, priority
            FROM email_messages
            WHERE external_id = ?
            """,
            (external_id,),
        )
        existing = cur.fetchone()
        if existing is None and provider_draft_id:
            cur = conn.execute(
                """
                SELECT id, summary, draft, body_html, provider_draft_id, type, priority
                FROM email_messages
                WHERE provider_draft_id = ?
                """,
                (provider_draft_id,),
            )
            existing = cur.fetchone()

        if existing:
            summary_value = existing["summary"] if existing["summary"] else normalized["summary"]
            draft_value = existing["draft"] if existing["draft"] else normalized["draft"]
            body_html_value = normalized["body_html"]
            if body_html_value is None:
                body_html_value = existing["body_html"]
            existing_priority = existing["priority"]
            priority_value = (
                int(existing_priority)
                if existing_priority is not None
                else normalized["priority"]
            )
            priority_value = max(1, min(3, priority_value))
            type_value = normalized["type"]
            existing_type = (existing["type"] or "").strip()
            # Keep manual/local triage stable when Gmail only toggles read state
            # (UNREAD label), which would otherwise flip response-needed/read-only.
            if normalized["type"] in {"response-needed", "read-only"}:
                if existing_type in {"response-needed", "read-only", "junk-uncertain"}:
                    type_value = existing_type
            conn.execute(
                """
                UPDATE email_messages
                SET thread_id = ?,
                    title = ?,
                    sender = ?,
                    body = ?,
                    body_html = ?,
                    provider_draft_id = ?,
                    type = ?,
                    priority = ?,
                    is_read = ?,
                    received_at = ?,
                    summary = ?,
                    draft = ?
                WHERE id = ?
                """,
                (
                    normalized["thread_id"],
                    normalized["title"],
                    normalized["sender"],
                    normalized["body"],
                    body_html_value,
                    normalized["provider_draft_id"] or existing["provider_draft_id"],
                    type_value,
                    priority_value,
                    normalized["is_read"],
                    normalized["received_at"],
                    summary_value,
                    draft_value,
                    existing["id"],
                ),
            )
            email_id = existing["id"]
        else:
            cur = conn.execute(
                """
                INSERT INTO email_messages (
                    external_id,
                    provider_draft_id,
                    thread_id,
                    title,
                    sender,
                    body,
                    body_html,
                    type,
                    priority,
                    is_read,
                    received_at,
                    summary,
                    draft
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    external_id,
                    normalized["provider_draft_id"],
                    normalized["thread_id"],
                    normalized["title"],
                    normalized["sender"],
                    normalized["body"],
                    normalized["body_html"],
                    normalized["type"],
                    normalized["priority"],
                    normalized["is_read"],
                    normalized["received_at"],
                    normalized["summary"],
                    normalized["draft"],
                ),
            )
            email_id = cur.lastrowid

        conn.execute("DELETE FROM email_recipients WHERE email_id = ?", (email_id,))
        _insert_recipients(conn, email_id, "to", normalized["recipients"])
        _insert_recipients(conn, email_id, "cc", normalized["cc"])

    return email_id


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
                provider_draft_id,
                thread_id,
                title,
                sender,
                body,
                type,
                priority,
                is_read,
                received_at
            )
            VALUES (NULL, NULL, ?, ?, ?, ?, 'sent', ?, 1, CURRENT_TIMESTAMP)
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


def save_local_draft(
    title,
    body,
    recipients,
    cc,
    email_id=None,
    provider_draft_id=None,
    thread_id=None,
    sender=LOCAL_USER_EMAIL,
    db_path=DB_DEFAULT,
):
    clean_title = (title or "(No subject)").strip()
    clean_body = body or ""
    with db_session(db_path) as conn:
        draft_id = None
        if email_id:
            cur = conn.execute(
                "SELECT id FROM email_messages WHERE id = ?",
                (email_id,),
            )
            row = cur.fetchone()
            draft_id = row["id"] if row else None
        if not draft_id and provider_draft_id:
            cur = conn.execute(
                """
                SELECT id
                FROM email_messages
                WHERE provider_draft_id = ?
                """,
                (provider_draft_id,),
            )
            row = cur.fetchone()
            draft_id = row["id"] if row else None

        if draft_id:
            conn.execute(
                """
                UPDATE email_messages
                SET provider_draft_id = ?,
                    thread_id = ?,
                    title = ?,
                    sender = ?,
                    body = ?,
                    type = 'draft',
                    is_read = 1,
                    received_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    provider_draft_id,
                    thread_id,
                    clean_title,
                    sender,
                    clean_body,
                    draft_id,
                ),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO email_messages (
                    external_id,
                    provider_draft_id,
                    thread_id,
                    title,
                    sender,
                    body,
                    type,
                    priority,
                    is_read,
                    received_at
                )
                VALUES (NULL, ?, ?, ?, ?, ?, 'draft', 1, 1, CURRENT_TIMESTAMP)
                """,
                (
                    provider_draft_id,
                    thread_id,
                    clean_title,
                    sender,
                    clean_body,
                ),
            )
            draft_id = cur.lastrowid

        conn.execute("DELETE FROM email_recipients WHERE email_id = ?", (draft_id,))
        _insert_recipients(conn, draft_id, "to", recipients)
        _insert_recipients(conn, draft_id, "cc", cc)
        return draft_id


def update_email_ai_fields(
    email_id,
    summary=None,
    email_type=None,
    priority=None,
    db_path=DB_DEFAULT,
):
    assignments = []
    params = []

    if summary is not None:
        assignments.append("summary = ?")
        params.append(summary)

    if email_type is not None:
        if email_type not in ALLOWED_TYPES:
            raise ValueError("Invalid email type.")
        assignments.append("type = ?")
        params.append(email_type)

    if priority is not None:
        safe_priority = max(1, min(3, int(priority)))
        assignments.append("priority = ?")
        params.append(safe_priority)

    if not assignments:
        return

    params.append(email_id)
    sql = f"UPDATE email_messages SET {', '.join(assignments)} WHERE id = ?"
    with db_session(db_path) as conn:
        conn.execute(sql, params)


def create_local_sent_email(
    title,
    body,
    recipients,
    cc,
    sender=LOCAL_USER_EMAIL,
    thread_id=None,
    db_path=DB_DEFAULT,
):
    clean_title = (title or "(No subject)").strip()
    with db_session(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO email_messages (
                external_id,
                provider_draft_id,
                thread_id,
                title,
                sender,
                body,
                type,
                priority,
                is_read,
                received_at
            )
            VALUES (NULL, NULL, ?, ?, ?, ?, 'sent', 1, 1, CURRENT_TIMESTAMP)
            """,
            (thread_id, clean_title, sender, body or ""),
        )
        email_id = cur.lastrowid
        _insert_recipients(conn, email_id, "to", recipients)
        _insert_recipients(conn, email_id, "cc", cc)
        return email_id

