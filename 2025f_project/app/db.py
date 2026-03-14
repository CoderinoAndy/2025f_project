# Model layer.
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from .debug_logger import log_event, log_exception
from .email_content import (
    contains_common_mojibake,
    repair_body_text,
    repair_header_text,
    repair_html_content,
    normalize_outgoing_text,
)

# Central DB path and enum-like values shared by validation and query code.
APP_ROOT = Path(__file__).resolve().parent.parent
DB_DEFAULT = str(APP_ROOT / "instance" / "app.sqlite")
LOCAL_USER_EMAIL = "you@example.com"
SQLITE_BUSY_TIMEOUT_MS = 30000
ALLOWED_TYPES = {
    "response-needed",
    "read-only",
    "junk",
    "junk-uncertain",
    "sent",
    "draft",
}
AI_CATEGORIES = {
    "urgent",
    "informational",
    "junk",
}
USER_DISPLAY_NAME_SETTING_KEY = "user_display_name"
SETTING_VALUE_MAX_CHARS = 80

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
    m.draft,
    m.is_archived,
    m.ai_category,
    m.ai_needs_response,
    m.ai_confidence
FROM email_messages m
"""

MAILBOX_LIST_SELECT_SQL = """
SELECT
    m.id,
    m.title,
    m.type,
    m.priority,
    m.is_read,
    m.received_at,
    m.is_archived
FROM email_messages m
"""

MAILBOX_SORT_SQL = {
    "date_desc": "m.received_at DESC, m.id DESC",
    "date_asc": "m.received_at ASC, m.id ASC",
    "priority_desc": "m.priority DESC, m.received_at DESC, m.id DESC",
    "priority_asc": "m.priority ASC, m.received_at DESC, m.id DESC",
    "unread_first": "m.is_read ASC, m.received_at DESC, m.id DESC",
    "read_first": "m.is_read DESC, m.received_at DESC, m.id DESC",
}


@contextmanager
def db_session(db_path):
    """Database session.
    """
    # One connection per context call keeps transaction boundaries explicit.
    conn = sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS};")
        conn.execute("PRAGMA foreign_keys = ON;")
        yield conn
        # Commit once per session so callers can perform multi-statement updates atomically.
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        log_exception(
            action_type="database",
            action="db_session",
            error=e,
            component="sqlite",
            details="SQLite operation failed.",
            db_path=db_path,
        )
        raise
    finally:
        conn.close()


def init_db(db_path=DB_DEFAULT):
    """Initialize database.
    """
    # Shared helper for this file.
    schema_path = Path(__file__).resolve().parent / "sql" / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError("schema.sql not found in app/sql/")

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with db_session(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        _apply_schema_migrations(conn)
    log_event(
        action_type="database",
        action="init_schema",
        status="ok",
        component="sqlite",
        db_path=db_path,
    )


def _apply_schema_migrations(conn):
    """Apply schema migrations.
    """
    # Snapshot current schema so we can decide whether rebuild/alter is needed.
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
        or "ai_image_context" in columns
        or "ai_image_context_status" in columns
        or "ai_image_context_updated_at" in columns
    )

    # Rebuild when old schemas are missing columns/check constraints that ALTER TABLE cannot fix safely.
    if needs_rebuild:
        _rebuild_email_tables(conn, columns)

    columns_after = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(email_messages)").fetchall()
    }
    if "ai_category" not in columns_after:
        conn.execute(
            """
            ALTER TABLE email_messages
            ADD COLUMN ai_category TEXT
              CHECK (ai_category IN ('urgent','informational','junk'))
            """
        )
    if "ai_needs_response" not in columns_after:
        conn.execute(
            """
            ALTER TABLE email_messages
            ADD COLUMN ai_needs_response INTEGER
              CHECK (ai_needs_response IN (0,1))
            """
        )
    if "ai_confidence" not in columns_after:
        conn.execute(
            """
            ALTER TABLE email_messages
            ADD COLUMN ai_confidence REAL
            """
        )
    if "is_archived" not in columns_after:
        conn.execute(
            """
            ALTER TABLE email_messages
            ADD COLUMN is_archived INTEGER NOT NULL DEFAULT 0
              CHECK (is_archived IN (0,1))
            """
        )
    _ensure_settings_table(conn)

    # Keep read-heavy mailbox queries fast with explicit supporting indexes.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_email_messages_provider_draft_id
        ON email_messages(provider_draft_id)
        WHERE provider_draft_id IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_email_messages_archived_received
        ON email_messages(is_archived, received_at DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_email_messages_type_archived_received
        ON email_messages(type, is_archived, received_at DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_email_messages_thread_received
        ON email_messages(thread_id, received_at ASC, id ASC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_email_recipients_email_type_order
        ON email_recipients(email_id, recipient_type, id)
        """
    )


def _ensure_settings_table(conn):
    """Create the lightweight app settings table when missing."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _normalize_setting_value(value, max_chars=SETTING_VALUE_MAX_CHARS):
    """Normalize short app-setting text values before storage."""
    if value is None:
        return None
    cleaned = " ".join(str(value).split()).strip()
    if not cleaned:
        return None
    return cleaned[:max_chars]


def _rebuild_email_tables(conn, existing_columns):
    """Rebuild email tables.
    """
    # Preserve as much legacy data as possible by selecting old columns when they exist.
    body_html_expr = "body_html" if "body_html" in existing_columns else "NULL"
    provider_draft_expr = (
        "provider_draft_id" if "provider_draft_id" in existing_columns else "NULL"
    )
    ai_category_expr = "ai_category" if "ai_category" in existing_columns else "NULL"
    ai_needs_response_expr = (
        "ai_needs_response" if "ai_needs_response" in existing_columns else "NULL"
    )
    ai_confidence_expr = (
        "ai_confidence" if "ai_confidence" in existing_columns else "NULL"
    )
    is_archived_expr = "is_archived" if "is_archived" in existing_columns else "0"

    # Temporarily disable FK checks while tables are renamed/recreated in place.
    conn.execute("PRAGMA foreign_keys = OFF;")
    conn.execute("ALTER TABLE email_messages RENAME TO email_messages_old;")

    # Recreate table with current constraints/defaults in one deterministic schema.
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
          draft TEXT,
          is_archived INTEGER NOT NULL DEFAULT 0
            CHECK (is_archived IN (0,1)),
          ai_category TEXT
            CHECK (ai_category IN ('urgent','informational','junk')),
          ai_needs_response INTEGER
            CHECK (ai_needs_response IN (0,1)),
          ai_confidence REAL
        );
        """
    )

    # Copy legacy rows into the new schema while coercing enum-like values safely.
    conn.execute(
        f"""
        INSERT INTO email_messages (
            id, external_id, provider_draft_id, thread_id, title, sender, body, body_html,
            type, priority, is_read, received_at, summary, draft, is_archived,
            ai_category, ai_needs_response, ai_confidence
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
            draft,
            {is_archived_expr},
            {ai_category_expr},
            {ai_needs_response_expr},
            {ai_confidence_expr}
        FROM email_messages_old
        """
    )

    # Rebuild recipients table so FK/cascade and uniqueness constraints are re-applied.
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
    """Row recipient dict.
    """
    data = dict(row)
    repaired_body_html = None
    if data.get("body_html") is not None:
        repaired_body_html = repair_html_content(data.get("body_html") or "")
        if contains_common_mojibake(repaired_body_html):
            repaired_body_html = None
        data["body_html"] = repaired_body_html or None
    if "body" in data:
        data["body"] = repair_body_text(data.get("body") or "", repaired_body_html)
    if "title" in data and data["title"] is not None:
        data["title"] = repair_header_text(data["title"])
    if "sender" in data and data["sender"] is not None:
        data["sender"] = repair_header_text(data["sender"])
    if "recipients" in data and data["recipients"] is not None:
        data["recipients"] = repair_header_text(data["recipients"])
    if "cc" in data and data["cc"] is not None:
        data["cc"] = repair_header_text(data["cc"])
    if "draft" in data and data["draft"] is not None:
        data["draft"] = normalize_outgoing_text(data["draft"])
    # Normalize SQLite scalar types (0/1/REAL) into Python values used by templates/API code.
    if "is_read" in data:
        data["is_read"] = bool(data["is_read"])
    if "is_archived" in data:
        data["is_archived"] = bool(data["is_archived"])
    if "ai_needs_response" in data and data["ai_needs_response"] is not None:
        data["ai_needs_response"] = bool(data["ai_needs_response"])
    if "priority" in data and data["priority"] is not None:
        data["priority"] = int(data["priority"])
    if "ai_confidence" in data and data["ai_confidence"] is not None:
        data["ai_confidence"] = float(data["ai_confidence"])
    if "summary" in data and data["summary"] is not None:
        data["summary"] = " ".join(repair_body_text(data["summary"], None).split()).strip()
    if "title" not in data and "subject" in data:
        data["title"] = data["subject"]
    if "received_at" in data and "date" not in data:
        data["date"] = data["received_at"]
    return data


def _split_addresses(raw_value):
    """Split addresses.
    """
    # Shared helper for this file.
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


def _escape_like_pattern(raw_value):
    """Escape SQL LIKE wildcards so mailbox search behaves like plain text search."""
    text = str(raw_value or "")
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _build_mailbox_filter_clause(
    *,
    email_type=None,
    exclude_types=None,
    include_archived=False,
    archived_only=False,
    search_query=None,
):
    """Build shared WHERE clauses for mailbox list/count queries."""
    where_clauses = []
    params = []

    if archived_only:
        where_clauses.append("m.is_archived = 1")
    elif not include_archived:
        where_clauses.append("m.is_archived = 0")

    if email_type:
        where_clauses.append("m.type = ?")
        params.append(email_type)

    excluded = sorted(set(exclude_types or []))
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        where_clauses.append(f"m.type NOT IN ({placeholders})")
        params.extend(excluded)

    normalized_query = " ".join(str(search_query or "").split()).strip()
    if normalized_query:
        like_value = f"%{_escape_like_pattern(normalized_query.lower())}%"
        where_clauses.append(
            """
            (
                lower(m.title) LIKE ? ESCAPE '\\'
                OR lower(m.sender) LIKE ? ESCAPE '\\'
                OR lower(coalesce(m.body, '')) LIKE ? ESCAPE '\\'
                OR EXISTS (
                    SELECT 1
                    FROM email_recipients er
                    WHERE er.email_id = m.id
                      AND lower(er.address) LIKE ? ESCAPE '\\'
                )
            )
            """
        )
        params.extend([like_value, like_value, like_value, like_value])

    clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return clause, params


def _resolve_mailbox_sort_sql(sort_code):
    """Resolve a mailbox sort code to a safe SQL ORDER BY fragment."""
    return MAILBOX_SORT_SQL.get(str(sort_code or "").strip(), MAILBOX_SORT_SQL["date_desc"])


def _insert_recipients(conn, email_id, recipient_type, raw_value):
    """Insert recipients.
    """
    # Shared helper for this file.
    for address in _split_addresses(raw_value):
        conn.execute(
            """
            INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
            VALUES (?, ?, ?)
            """,
            (email_id, recipient_type, address),
        )


def _normalize_ai_category(value):
    """Normalize AI category.
    """
    # Normalize ai category to one format used across the app.
    category = str(value or "").strip().lower()
    return category if category in AI_CATEGORIES else None


def _normalize_ai_needs_response(value):
    """Normalize AI needs response.
    """
    # Normalize ai needs response to one format used across the app.
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return 1
    if lowered in {"0", "false", "no", "n"}:
        return 0
    return None


def _normalize_ai_confidence(value):
    """Normalize AI confidence.
    """
    # Normalize ai confidence to one format used across the app.
    if value is None or value == "":
        return None
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, confidence))
def _normalize_archived_flag(value):
    """Normalize archived flag.
    """
    # Normalize archived flag to one format used across the app.
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return None
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return 1
    if lowered in {"0", "false", "no", "n"}:
        return 0
    return None


def count_mailbox_emails(
    *,
    email_type=None,
    exclude_types=None,
    include_archived=False,
    archived_only=False,
    search_query=None,
    db_path=DB_DEFAULT,
):
    """Count rows for a mailbox list query."""
    clause, params = _build_mailbox_filter_clause(
        email_type=email_type,
        exclude_types=exclude_types,
        include_archived=include_archived,
        archived_only=archived_only,
        search_query=search_query,
    )
    with db_session(db_path) as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM email_messages m
            {clause}
            """,
            params,
        ).fetchone()
    return int(row["total"] or 0) if row else 0


def fetch_mailbox_page(
    *,
    email_type=None,
    exclude_types=None,
    include_archived=False,
    archived_only=False,
    search_query=None,
    sort_code="date_desc",
    limit=100,
    offset=0,
    db_path=DB_DEFAULT,
):
    """Fetch one lightweight mailbox page for list views."""
    safe_limit = max(1, int(limit or 100))
    safe_offset = max(0, int(offset or 0))
    clause, params = _build_mailbox_filter_clause(
        email_type=email_type,
        exclude_types=exclude_types,
        include_archived=include_archived,
        archived_only=archived_only,
        search_query=search_query,
    )
    order_by_sql = _resolve_mailbox_sort_sql(sort_code)
    with db_session(db_path) as conn:
        cur = conn.execute(
            f"""
            {MAILBOX_LIST_SELECT_SQL}
            {clause}
            ORDER BY {order_by_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, safe_limit, safe_offset],
        )
        return [_row_to_dict(r) for r in cur.fetchall()]


def get_app_setting(key, default=None, db_path=DB_DEFAULT):
    """Fetch a single app setting value."""
    setting_key = str(key or "").strip()
    if not setting_key:
        return default
    if not Path(db_path).exists():
        return default
    with db_session(db_path) as conn:
        _ensure_settings_table(conn)
        row = conn.execute(
            """
            SELECT value
            FROM app_settings
            WHERE key = ?
            """,
            (setting_key,),
        ).fetchone()
    return row["value"] if row and row["value"] is not None else default


def set_app_setting(key, value, db_path=DB_DEFAULT):
    """Insert, update, or clear a single app setting value."""
    setting_key = str(key or "").strip()
    if not setting_key:
        raise ValueError("Setting key is required.")

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_value = _normalize_setting_value(value)
    with db_session(db_path) as conn:
        _ensure_settings_table(conn)
        if normalized_value is None:
            conn.execute("DELETE FROM app_settings WHERE key = ?", (setting_key,))
            return None
        conn.execute(
            """
            INSERT INTO app_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE
            SET value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (setting_key, normalized_value),
        )
    return normalized_value


def get_user_display_name(db_path=DB_DEFAULT):
    """Return the stored mailbox-owner display name, if any."""
    return _normalize_setting_value(
        get_app_setting(USER_DISPLAY_NAME_SETTING_KEY, default=None, db_path=db_path)
    )


def set_user_display_name(display_name, db_path=DB_DEFAULT):
    """Persist or clear the mailbox-owner display name."""
    return set_app_setting(
        USER_DISPLAY_NAME_SETTING_KEY,
        display_name,
        db_path=db_path,
    )


def fetch_email_by_id(email_id, db_path=DB_DEFAULT):
    """Fetch email by ID.
    """
    # Fetch email by id from storage and return normalized rows.
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

def fetch_email_by_provider_draft_id(provider_draft_id, db_path=DB_DEFAULT):
    """Fetch email by provider draft ID.
    """
    # Fetch email by provider draft id from storage and return normalized rows.
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
    """Fetch thread emails.
    """
    # Fetch thread emails from storage and return normalized rows.
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
    """Mark read.
    """
    # Update read state without mutating unrelated fields.
    with db_session(db_path) as conn:
        conn.execute(
            "UPDATE email_messages SET is_read = ? WHERE id = ?",
            (1 if read else 0, email_id),
        )


def upsert_email_from_provider(email_data, db_path=DB_DEFAULT):
    """Upsert email from provider.
    """
    # Accept either message external_id or provider draft id as the stable upsert key.
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

    # Normalize provider payload into one canonical shape before DB reads/writes.
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
        "ai_category": _normalize_ai_category(email_data.get("ai_category")),
        "ai_needs_response": _normalize_ai_needs_response(
            email_data.get("ai_needs_response")
        ),
        "ai_confidence": _normalize_ai_confidence(email_data.get("ai_confidence")),
        "is_archived": _normalize_archived_flag(email_data.get("is_archived")),
    }

    with db_session(db_path) as conn:
        # First match by external message id; fallback to provider draft id for draft flows.
        cur = conn.execute(
            """
            SELECT
                id,
                body,
                summary,
                draft,
                body_html,
                provider_draft_id,
                type,
                priority,
                is_archived,
                ai_category,
                ai_needs_response,
                ai_confidence
            FROM email_messages
            WHERE external_id = ?
            """,
            (external_id,),
        )
        existing = cur.fetchone()
        if existing is None and provider_draft_id:
            cur = conn.execute(
                """
                SELECT
                    id,
                    body,
                    summary,
                    draft,
                    body_html,
                    provider_draft_id,
                    type,
                    priority,
                    is_archived,
                    ai_category,
                    ai_needs_response,
                    ai_confidence
                FROM email_messages
                WHERE provider_draft_id = ?
                """,
                (provider_draft_id,),
            )
            existing = cur.fetchone()

        if existing:
            # Keep locally generated summary/draft/classification unless provider has newer explicit values.
            summary_value = existing["summary"] if existing["summary"] else normalized["summary"]
            draft_value = existing["draft"] if existing["draft"] else normalized["draft"]
            body_html_value = normalized["body_html"]
            if body_html_value is None:
                body_html_value = existing["body_html"]
            ai_category_value = normalized["ai_category"]
            if ai_category_value is None:
                ai_category_value = existing["ai_category"]
            ai_needs_response_value = normalized["ai_needs_response"]
            if ai_needs_response_value is None:
                ai_needs_response_value = existing["ai_needs_response"]
            ai_confidence_value = normalized["ai_confidence"]
            if ai_confidence_value is None:
                ai_confidence_value = existing["ai_confidence"]
            is_archived_value = normalized["is_archived"]
            if is_archived_value is None:
                is_archived_value = existing["is_archived"]
            existing_priority = existing["priority"]
            priority_value = (
                int(existing_priority)
                if existing_priority is not None
                else normalized["priority"]
            )
            priority_value = max(1, min(3, priority_value))
            type_value = normalized["type"]
            existing_type = (existing["type"] or "").strip()
            # Preserve local/AI triage for existing rows; provider sync should
            # refresh content/read state but not reclassify old emails.
            if existing_type in ALLOWED_TYPES:
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
                    draft = ?,
                    is_archived = ?,
                    ai_category = ?,
                    ai_needs_response = ?,
                    ai_confidence = ?
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
                    is_archived_value,
                    ai_category_value,
                    ai_needs_response_value,
                    ai_confidence_value,
                    existing["id"],
                ),
            )
            email_id = existing["id"]
        else:
            # Insert a new row when provider identifiers do not match any local email.
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
                    draft,
                    is_archived,
                    ai_category,
                    ai_needs_response,
                    ai_confidence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    normalized["is_archived"] or 0,
                    normalized["ai_category"],
                    normalized["ai_needs_response"],
                    normalized["ai_confidence"],
                ),
            )
            email_id = cur.lastrowid

        # Rebuild recipients from normalized payload so local TO/CC stay authoritative.
        conn.execute("DELETE FROM email_recipients WHERE email_id = ?", (email_id,))
        _insert_recipients(conn, email_id, "to", normalized["recipients"])
        _insert_recipients(conn, email_id, "cc", normalized["cc"])

    return email_id


def set_email_type(email_id, new_type, db_path=DB_DEFAULT):
    """Set email type.
    """
    # Update email type and keep local and provider state in sync when we can.
    if new_type not in ALLOWED_TYPES:
        raise ValueError("Invalid email type.")
    with db_session(db_path) as conn:
        conn.execute("UPDATE email_messages SET type = ? WHERE id = ?", (new_type, email_id))


def set_email_archived(email_id, archived=True, db_path=DB_DEFAULT):
    """Set email archived.
    """
    # Update the archived flag and keep local and provider state in sync when we can.
    with db_session(db_path) as conn:
        conn.execute(
            "UPDATE email_messages SET is_archived = ? WHERE id = ?",
            (1 if archived else 0, email_id),
        )


def update_draft(email_id, draft_text, db_path=DB_DEFAULT):
    """Update draft.
    """
    # Update draft fields while preserving schema and business constraints.
    clean_draft = normalize_outgoing_text(draft_text or "")
    with db_session(db_path) as conn:
        conn.execute("UPDATE email_messages SET draft = ? WHERE id = ?", (clean_draft, email_id))


def create_reply_email(source_email_id, reply_text, recipients, cc, db_path=DB_DEFAULT):
    """Create reply email.
    """
    # Create reply email from validated inputs and return the new identifier.
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
        title = repair_header_text(source["title"] or "No subject")
        if not title.lower().startswith("re:"):
            title = f"Re: {title}"
        clean_reply_text = normalize_outgoing_text(reply_text or "")

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
            (thread_id, title, LOCAL_USER_EMAIL, clean_reply_text, priority),
        )

        new_email_id = cur.lastrowid
        _insert_recipients(conn, new_email_id, "to", recipients)
        _insert_recipients(conn, new_email_id, "cc", cc)
        return new_email_id


def delete_email(email_id, db_path=DB_DEFAULT):
    """Delete email.
    """
    # Delete the email and clean up anything that depends on it.
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
    """Save local draft.
    """
    clean_title = repair_header_text(title or "(No subject)")
    clean_body = normalize_outgoing_text(body or "")
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
            # Update in place when we already know the local row or provider draft mapping.
            conn.execute(
                """
                UPDATE email_messages
                SET provider_draft_id = ?,
                    thread_id = ?,
                    title = ?,
                    sender = ?,
                    body = ?,
                    type = 'draft',
                    is_archived = 0,
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
            # Create a brand-new local draft row when no prior mapping exists.
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

        # Rewrite recipients each save so local draft always reflects latest compose fields.
        conn.execute("DELETE FROM email_recipients WHERE email_id = ?", (draft_id,))
        _insert_recipients(conn, draft_id, "to", recipients)
        _insert_recipients(conn, draft_id, "cc", cc)
        return draft_id


def update_email_ai_fields(
    email_id,
    summary=None,
    email_type=None,
    priority=None,
    ai_category=None,
    ai_needs_response=None,
    ai_confidence=None,
    lock_existing_classification=True,
    db_path=DB_DEFAULT,
):
    """Update email AI fields.
    """
    with db_session(db_path) as conn:
        existing = conn.execute(
            """
            SELECT type, priority, ai_category
            FROM email_messages
            WHERE id = ?
            """,
            (email_id,),
        ).fetchone()
        if existing is None:
            return

        assignments = []
        params = []

        if summary is not None:
            assignments.append("summary = ?")
            params.append(repair_body_text(summary, None))

        classification_locked = bool(
            lock_existing_classification
            and str(existing["ai_category"] or "").strip()
        )
        # Once AI classification exists, keep type/priority stable for this row.

        if email_type is not None:
            if email_type not in ALLOWED_TYPES:
                raise ValueError("Invalid email type.")
            if not classification_locked:
                assignments.append("type = ?")
                params.append(email_type)

        if priority is not None:
            safe_priority = max(1, min(3, int(priority)))
            if not classification_locked:
                assignments.append("priority = ?")
                params.append(safe_priority)

        if ai_category is not None:
            normalized_category = _normalize_ai_category(ai_category)
            if normalized_category is None:
                raise ValueError("Invalid AI category.")
            assignments.append("ai_category = ?")
            params.append(normalized_category)

        if ai_needs_response is not None:
            normalized_needs_response = _normalize_ai_needs_response(ai_needs_response)
            if normalized_needs_response is None:
                raise ValueError("Invalid AI needs_response value.")
            assignments.append("ai_needs_response = ?")
            params.append(normalized_needs_response)

        if ai_confidence is not None:
            normalized_confidence = _normalize_ai_confidence(ai_confidence)
            if normalized_confidence is None:
                raise ValueError("Invalid AI confidence value.")
            assignments.append("ai_confidence = ?")
            params.append(normalized_confidence)

        if not assignments:
            return

        params.append(email_id)
        sql = f"UPDATE email_messages SET {', '.join(assignments)} WHERE id = ?"
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
    """Create local sent email.
    """
    # Create local sent email from validated inputs and return the new identifier.
    clean_title = repair_header_text(title or "(No subject)")
    clean_body = normalize_outgoing_text(body or "")
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
            (thread_id, clean_title, sender, clean_body),
        )
        email_id = cur.lastrowid
        _insert_recipients(conn, email_id, "to", recipients)
        _insert_recipients(conn, email_id, "cc", cc)
        return email_id
