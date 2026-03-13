import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import DB_DEFAULT, upsert_email_from_provider
from app.gmail_service import _get_draft_data, _get_service, _to_db_record, sync_message_by_external_id


STATUS_PATH = PROJECT_ROOT / "instance" / "mailbox_reset_status.json"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_status(payload):
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = dict(payload)
    data["updated_at"] = utc_now()
    tmp_path = STATUS_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(STATUS_PATH)


def collect_inbox_ids(service):
    inbox_ids = []
    page_token = None
    while True:
        response = (
            service.users()
            .messages()
            .list(userId="me", labelIds=["INBOX"], maxResults=500, pageToken=page_token)
            .execute()
        )
        inbox_ids.extend(
            message.get("id") for message in (response.get("messages") or []) if message.get("id")
        )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return inbox_ids


def collect_draft_ids(service):
    draft_ids = []
    page_token = None
    while True:
        response = (
            service.users()
            .drafts()
            .list(userId="me", maxResults=500, pageToken=page_token)
            .execute()
        )
        draft_ids.extend(
            entry.get("id") for entry in (response.get("drafts") or []) if entry.get("id")
        )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return draft_ids


def reset_local_mailbox(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("DELETE FROM email_recipients")
        conn.execute("DELETE FROM email_messages")
        try:
            conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('email_messages', 'email_recipients')")
        except sqlite3.Error:
            pass
        conn.commit()


def sync_drafts(service, db_path, draft_ids):
    synced = 0
    for provider_draft_id in draft_ids:
        draft_data = _get_draft_data(service, provider_draft_id)
        if not draft_data:
            continue
        message_data = draft_data.get("message") or {}
        record = _to_db_record(
            message_data,
            service=service,
            provider_draft_id=provider_draft_id,
        )
        if not record:
            continue
        record["type"] = "draft"
        record["is_read"] = True
        upsert_email_from_provider(record, db_path=db_path)
        synced += 1
    return synced


def summarize_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            "messages": conn.execute("SELECT COUNT(*) AS c FROM email_messages").fetchone()["c"],
            "html_messages": conn.execute(
                "SELECT COUNT(*) AS c FROM email_messages WHERE body_html IS NOT NULL AND trim(body_html) <> ''"
            ).fetchone()["c"],
            "summary_messages": conn.execute(
                "SELECT COUNT(*) AS c FROM email_messages WHERE summary IS NOT NULL AND trim(summary) <> ''"
            ).fetchone()["c"],
            "draft_messages": conn.execute(
                "SELECT COUNT(*) AS c FROM email_messages WHERE draft IS NOT NULL AND trim(draft) <> ''"
            ).fetchone()["c"],
        }
    return counts


def main():
    db_path = Path(DB_DEFAULT)
    service = _get_service()
    if not service:
        raise SystemExit("Gmail service unavailable")
    started_at = utc_now()

    write_status(
        {
            "phase": "discovering",
            "started_at": started_at,
            "db_path": str(db_path),
        }
    )

    inbox_ids = collect_inbox_ids(service)
    draft_ids = collect_draft_ids(service)

    backup_path = db_path.with_name(
        f"{db_path.stem}.pre-reset-{datetime.now().strftime('%Y%m%d-%H%M%S')}{db_path.suffix}"
    )
    shutil.copy2(db_path, backup_path)

    write_status(
        {
            "phase": "resetting_local_db",
            "started_at": started_at,
            "db_path": str(db_path),
            "backup_path": str(backup_path),
            "total_inbox": len(inbox_ids),
            "total_drafts": len(draft_ids),
        }
    )
    reset_local_mailbox(db_path)

    synced_inbox = 0
    html_messages = 0
    errors = 0
    write_status(
        {
            "phase": "syncing_inbox",
            "started_at": started_at,
            "db_path": str(db_path),
            "backup_path": str(backup_path),
            "total_inbox": len(inbox_ids),
            "total_drafts": len(draft_ids),
            "synced_inbox": synced_inbox,
            "html_messages": html_messages,
            "errors": errors,
        }
    )

    for index, message_id in enumerate(inbox_ids, start=1):
        try:
            record = sync_message_by_external_id(message_id, db_path=str(db_path), service=service)
            if record:
                synced_inbox += 1
                if str(record.get("body_html") or "").strip():
                    html_messages += 1
            else:
                errors += 1
        except Exception:
            errors += 1

        if index % 100 == 0 or index == len(inbox_ids):
            write_status(
                {
                    "phase": "syncing_inbox",
                    "started_at": started_at,
                    "db_path": str(db_path),
                    "backup_path": str(backup_path),
                    "total_inbox": len(inbox_ids),
                    "total_drafts": len(draft_ids),
                    "processed_inbox": index,
                    "synced_inbox": synced_inbox,
                    "html_messages": html_messages,
                    "errors": errors,
                }
            )

    write_status(
        {
            "phase": "syncing_drafts",
            "started_at": started_at,
            "db_path": str(db_path),
            "backup_path": str(backup_path),
            "total_inbox": len(inbox_ids),
            "total_drafts": len(draft_ids),
            "processed_inbox": len(inbox_ids),
            "synced_inbox": synced_inbox,
            "html_messages": html_messages,
            "errors": errors,
        }
    )

    synced_drafts = sync_drafts(service, str(db_path), draft_ids)
    counts = summarize_db(db_path)
    write_status(
        {
            "phase": "complete",
            "started_at": started_at,
            "finished_at": utc_now(),
            "db_path": str(db_path),
            "backup_path": str(backup_path),
            "total_inbox": len(inbox_ids),
            "total_drafts": len(draft_ids),
            "processed_inbox": len(inbox_ids),
            "synced_inbox": synced_inbox,
            "synced_drafts": synced_drafts,
            "html_messages": html_messages,
            "errors": errors,
            "db_counts": counts,
        }
    )


if __name__ == "__main__":
    main()
