import base64
import hashlib
import mimetypes
import os
import re
import threading
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import getaddresses
from html import unescape
from pathlib import Path

from .db import upsert_email_from_provider

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ImportError:  # Optional dependency for local-only mode.
    Request = None
    Credentials = None
    InstalledAppFlow = None
    build = None

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.compose",
]
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOKEN_PATH = PROJECT_ROOT / "instance" / "gmail_token.json"
SYNC_INTERVAL_SECONDS = int(os.getenv("GMAIL_SYNC_INTERVAL_SECONDS", "20"))
SYNC_MAX_RESULTS = int(os.getenv("GMAIL_SYNC_MAX_RESULTS", "25"))

_LAST_SYNC_AT = 0.0
_SYNC_LOCK = threading.Lock()


def _candidate_credentials_paths():
    configured_path = os.getenv("GMAIL_CREDENTIALS_FILE")
    candidates = []
    if configured_path:
        candidates.append(Path(configured_path).expanduser())
    candidates.append(PROJECT_ROOT / "credentials.json")
    candidates.append(PROJECT_ROOT.parent / "credentials.json")
    return candidates


def _resolve_credentials_path():
    for path in _candidate_credentials_paths():
        if path.exists():
            return path
    return None


def gmail_available():
    return bool(
        _resolve_credentials_path()
        and Request
        and Credentials
        and InstalledAppFlow
        and build
    )


def _save_token(credentials):
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(credentials.to_json(), encoding="utf-8")


def _load_credentials():
    if not gmail_available():
        return None

    credentials = None
    if TOKEN_PATH.exists():
        try:
            credentials = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception:
            credentials = None

    if credentials and credentials.valid and credentials.has_scopes(SCOPES):
        return credentials

    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            if credentials.has_scopes(SCOPES):
                _save_token(credentials)
                return credentials
        except Exception:
            credentials = None

    credentials_path = _resolve_credentials_path()
    if not credentials_path:
        return None

    try:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        credentials = flow.run_local_server(port=0)
    except Exception as exc:
        print(f"Gmail OAuth setup skipped: {exc}")
        return None

    _save_token(credentials)
    return credentials


def _get_service():
    credentials = _load_credentials()
    if not credentials:
        return None
    try:
        return build("gmail", "v1", credentials=credentials, cache_discovery=False)
    except Exception as exc:
        print(f"Gmail service init failed: {exc}")
        return None


def _extract_header(payload, header_name):
    for header in payload.get("headers", []) or []:
        if header.get("name", "").lower() == header_name.lower():
            return header.get("value", "")
    return ""


def _decode_body_part(raw_data):
    decoded = _decode_body_bytes(raw_data)
    if not decoded:
        return ""
    return decoded.decode("utf-8", errors="ignore")


def _decode_body_bytes(raw_data):
    if not raw_data:
        return b""
    padded = f"{raw_data}{'=' * (-len(raw_data) % 4)}"
    try:
        return base64.urlsafe_b64decode(padded.encode("utf-8"))
    except Exception:
        return b""


def _extract_part_header(part, header_name):
    for header in part.get("headers", []) or []:
        if header.get("name", "").lower() == header_name.lower():
            return header.get("value", "")
    return ""


def _normalize_cid(raw_value):
    if not raw_value:
        return ""
    return raw_value.strip().strip("<>").lower()


def _attachment_bytes(service, message_id, attachment_id):
    if not service or not message_id or not attachment_id:
        return b""
    try:
        response = (
            service.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=message_id, id=attachment_id)
            .execute()
        )
    except Exception:
        return b""
    return _decode_body_bytes(response.get("data"))


def _iter_parts(payload):
    if not payload:
        return
    stack = [payload]
    while stack:
        current = stack.pop()
        children = current.get("parts") or []
        if children:
            stack.extend(reversed(children))
        yield current


def _guess_filename(content_type, index):
    mime = (content_type or "").split(";", 1)[0].strip().lower()
    extension = mimetypes.guess_extension(mime) if mime else None
    suffix = extension or ".bin"
    return f"attachment-{index}{suffix}"


def _extract_attachment_payloads(payload, service=None, message_id=None):
    attachments = []
    for part in _iter_parts(payload):
        body_info = part.get("body") or {}
        attachment_id = body_info.get("attachmentId")
        filename = (part.get("filename") or "").strip()
        mime_type = (part.get("mimeType") or "application/octet-stream").strip().lower()
        if "/" not in mime_type:
            mime_type = "application/octet-stream"
        if mime_type.startswith("multipart/"):
            continue
        if not filename and not attachment_id:
            continue

        content_bytes = _decode_body_bytes(body_info.get("data"))
        if not content_bytes and attachment_id:
            content_bytes = _attachment_bytes(service, message_id, attachment_id)
        if content_bytes is None:
            content_bytes = b""

        attachments.append(
            {
                "filename": filename or _guess_filename(mime_type, len(attachments) + 1),
                "content": content_bytes,
                "content_type": mime_type,
            }
        )
    return attachments


def _extract_attachment_metadata(payload):
    metadata = []
    for part in _iter_parts(payload):
        body_info = part.get("body") or {}
        attachment_id = body_info.get("attachmentId")
        filename = (part.get("filename") or "").strip()
        mime_type = (part.get("mimeType") or "application/octet-stream").strip().lower()
        if "/" not in mime_type:
            mime_type = "application/octet-stream"
        if mime_type.startswith("multipart/"):
            continue
        if not filename and not attachment_id:
            continue

        size = body_info.get("size")
        try:
            size_value = int(size) if size is not None else 0
        except (TypeError, ValueError):
            size_value = 0
        metadata.append(
            {
                "filename": filename or _guess_filename(mime_type, len(metadata) + 1),
                "content_type": mime_type,
                "size": max(0, size_value),
            }
        )
    return metadata


def _merge_attachment_payloads(existing_attachments, incoming_attachments):
    merged = []
    seen = set()
    for attachment in (existing_attachments or []) + (incoming_attachments or []):
        filename = (attachment.get("filename") or "attachment.bin").strip() or "attachment.bin"
        content = attachment.get("content") or b""
        content_type = (attachment.get("content_type") or "").strip().lower() or "application/octet-stream"
        if "/" not in content_type:
            content_type = "application/octet-stream"
        digest = hashlib.sha1(content).hexdigest() if content else "empty"
        key = (filename, content_type, len(content), digest)
        if key in seen:
            continue
        seen.add(key)
        merged.append(
            {
                "filename": filename,
                "content": content,
                "content_type": content_type,
            }
        )
    return merged


def _get_draft_data(service, provider_draft_id):
    if not service or not provider_draft_id:
        return None
    try:
        return (
            service.users()
            .drafts()
            .get(userId="me", id=provider_draft_id, format="full")
            .execute()
        )
    except Exception as exc:
        print(f"Gmail draft fetch failed for {provider_draft_id}: {exc}")
        return None


def _get_message_data(service, external_id):
    if not service or not external_id:
        return None
    try:
        return (
            service.users()
            .messages()
            .get(userId="me", id=external_id, format="full")
            .execute()
        )
    except Exception as exc:
        print(f"Gmail message fetch failed for {external_id}: {exc}")
        return None


def fetch_draft_attachments(provider_draft_id):
    service = _get_service()
    if not service or not provider_draft_id:
        return []
    draft_data = _get_draft_data(service, provider_draft_id)
    if not draft_data:
        return []
    message_data = draft_data.get("message") or {}
    return _extract_attachment_payloads(
        message_data.get("payload") or {},
        service=service,
        message_id=message_data.get("id"),
    )


def fetch_draft_attachment_metadata(provider_draft_id):
    service = _get_service()
    if not service or not provider_draft_id:
        return []
    draft_data = _get_draft_data(service, provider_draft_id)
    if not draft_data:
        return []
    message_data = draft_data.get("message") or {}
    return _extract_attachment_metadata(message_data.get("payload") or {})


def fetch_message_attachments(external_id):
    service = _get_service()
    if not service or not external_id:
        return []
    message_data = _get_message_data(service, external_id)
    if not message_data:
        return []
    return _extract_attachment_payloads(
        message_data.get("payload") or {},
        service=service,
        message_id=message_data.get("id"),
    )


def fetch_message_attachment_metadata(external_id):
    service = _get_service()
    if not service or not external_id:
        return []
    message_data = _get_message_data(service, external_id)
    if not message_data:
        return []
    return _extract_attachment_metadata(message_data.get("payload") or {})


def _html_to_text(raw_html):
    if not raw_html:
        return ""
    no_scripts = re.sub(
        r"(?is)<(script|style)[^>]*>.*?</\1>",
        " ",
        raw_html,
    )
    stripped = re.sub(r"<[^>]+>", " ", no_scripts)
    normalized = re.sub(r"\s+", " ", stripped).strip()
    return unescape(normalized)


def _replace_inline_cid_sources(raw_html, cid_sources):
    if not raw_html or not cid_sources:
        return raw_html

    def replacer(match):
        quote = match.group(1)
        cid_key = _normalize_cid(match.group(2))
        resolved = cid_sources.get(cid_key)
        if not resolved:
            return match.group(0)
        return f"src={quote}{resolved}{quote}"

    return re.sub(
        r"""src\s*=\s*(['"])cid:([^'"]+)\1""",
        replacer,
        raw_html,
        flags=re.IGNORECASE,
    )


def _extract_message_content(payload, service=None, message_id=None):
    if not payload:
        return "", None

    plain_text_parts = []
    html_parts = []
    inline_cid_sources = {}
    stack = [payload]

    while stack:
        current = stack.pop()
        children = current.get("parts") or []
        if children:
            stack.extend(reversed(children))

        mime_type = (current.get("mimeType") or "").lower()
        body_info = current.get("body") or {}
        content_id = _normalize_cid(_extract_part_header(current, "Content-ID"))
        content_bytes = _decode_body_bytes(body_info.get("data"))

        attachment_id = body_info.get("attachmentId")
        if not content_bytes and attachment_id:
            should_fetch_attachment = mime_type.startswith("text/") or (
                mime_type.startswith("image/") and content_id
            )
            if should_fetch_attachment:
                content_bytes = _attachment_bytes(service, message_id, attachment_id)

        if not content_bytes:
            continue

        if mime_type.startswith("text/plain"):
            plain_text_parts.append(content_bytes.decode("utf-8", errors="ignore"))
            continue

        if mime_type.startswith("text/html"):
            html_parts.append(content_bytes.decode("utf-8", errors="ignore"))
            continue

        if mime_type.startswith("image/") and content_id:
            encoded = base64.b64encode(content_bytes).decode("ascii")
            inline_cid_sources[content_id] = f"data:{mime_type};base64,{encoded}"

    html_body = "\n".join(part for part in html_parts if part.strip()).strip()
    if html_body:
        html_body = _replace_inline_cid_sources(html_body, inline_cid_sources)
    else:
        html_body = None

    plain_body = "\n".join(part for part in plain_text_parts if part.strip()).strip()
    if not plain_body and html_body:
        plain_body = _html_to_text(html_body)

    return plain_body, html_body


def _parse_addresses(raw_value):
    if not raw_value:
        return ""
    parsed_addresses = [addr for _, addr in getaddresses([raw_value]) if addr]
    return ", ".join(parsed_addresses) if parsed_addresses else raw_value.strip()


def _labels_to_type(label_ids):
    labels = set(label_ids or [])
    if "DRAFT" in labels:
        return "draft"
    if "SENT" in labels:
        return "sent"
    if "SPAM" in labels:
        return "junk"
    if "UNREAD" in labels:
        return "response-needed"
    return "read-only"


def _labels_to_priority(label_ids):
    labels = set(label_ids or [])
    if "STARRED" in labels:
        return 3
    if "IMPORTANT" in labels:
        return 2
    return 1


def _received_at(internal_date):
    try:
        timestamp = int(internal_date) / 1000
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _to_db_record(message, service=None, provider_draft_id=None):
    label_ids = set(message.get("labelIds") or [])
    if "TRASH" in label_ids:
        return None

    payload = message.get("payload") or {}
    body_text, body_html = _extract_message_content(
        payload,
        service=service,
        message_id=message.get("id"),
    )
    body = (body_text or message.get("snippet") or "").strip()
    return {
        "external_id": message.get("id"),
        "provider_draft_id": provider_draft_id,
        "thread_id": message.get("threadId"),
        "title": (_extract_header(payload, "Subject") or "(No subject)").strip(),
        "sender": (_extract_header(payload, "From") or "unknown@unknown").strip(),
        "recipients": _parse_addresses(_extract_header(payload, "To")),
        "cc": _parse_addresses(_extract_header(payload, "Cc")),
        "body": body,
        "body_html": body_html,
        "type": _labels_to_type(label_ids),
        "priority": _labels_to_priority(label_ids),
        "is_read": "UNREAD" not in label_ids,
        "received_at": _received_at(message.get("internalDate")),
    }


def sync_message_by_external_id(
    external_id,
    db_path="instance/app.sqlite",
    service=None,
    provider_draft_id=None,
):
    if not external_id:
        return None
    service = service or _get_service()
    if not service:
        return None

    try:
        message = (
            service.users()
            .messages()
            .get(userId="me", id=external_id, format="full")
            .execute()
        )
    except Exception as exc:
        print(f"Gmail fetch failed for {external_id}: {exc}")
        return None

    record = _to_db_record(
        message,
        service=service,
        provider_draft_id=provider_draft_id,
    )
    if not record:
        return None
    upsert_email_from_provider(record, db_path=db_path)
    return record


def sync_recent_emails(db_path="instance/app.sqlite", max_results=None):
    service = _get_service()
    if not service:
        return 0

    target = max(1, int(max_results or SYNC_MAX_RESULTS))
    page_token = None
    synced = 0
    visited = 0

    while visited < target:
        page_size = min(100, target - visited)
        try:
            response = (
                service.users()
                .messages()
                .list(
                    userId="me",
                    includeSpamTrash=True,
                    maxResults=page_size,
                    pageToken=page_token,
                )
                .execute()
            )
        except Exception as exc:
            print(f"Gmail list failed: {exc}")
            break

        messages = response.get("messages") or []
        if not messages:
            break

        for item in messages:
            visited += 1
            external_id = item.get("id")
            if not external_id:
                continue
            if sync_message_by_external_id(external_id, db_path=db_path, service=service):
                synced += 1
            if visited >= target:
                break

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return synced


def maybe_sync_recent_emails(db_path="instance/app.sqlite", force=False, max_results=None):
    global _LAST_SYNC_AT
    now = time.time()
    if not force and now - _LAST_SYNC_AT < SYNC_INTERVAL_SECONDS:
        return 0

    if not _SYNC_LOCK.acquire(blocking=False):
        return 0

    try:
        synced = sync_recent_emails(db_path=db_path, max_results=max_results)
        _LAST_SYNC_AT = time.time()
        return synced
    finally:
        _SYNC_LOCK.release()


def trigger_background_sync(db_path="instance/app.sqlite", force=False, max_results=None):
    global _LAST_SYNC_AT
    now = time.time()
    if not force and now - _LAST_SYNC_AT < SYNC_INTERVAL_SECONDS:
        return False
    if not _SYNC_LOCK.acquire(blocking=False):
        return False

    def _worker():
        global _LAST_SYNC_AT
        try:
            sync_recent_emails(db_path=db_path, max_results=max_results)
            _LAST_SYNC_AT = time.time()
        finally:
            _SYNC_LOCK.release()

    threading.Thread(target=_worker, daemon=True).start()
    return True


def _modify_labels(service, external_id, add_labels=None, remove_labels=None):
    body = {}
    if add_labels:
        body["addLabelIds"] = sorted(set(add_labels))
    if remove_labels:
        body["removeLabelIds"] = sorted(set(remove_labels))
    if not body:
        return True

    try:
        service.users().messages().modify(userId="me", id=external_id, body=body).execute()
        return True
    except Exception as exc:
        print(f"Gmail label update failed for {external_id}: {exc}")
        return False


def _build_email_message(to_value, cc_value, subject, body_text, attachments=None):
    message = EmailMessage()
    if to_value:
        message["To"] = to_value
    if cc_value:
        message["Cc"] = cc_value
    message["Subject"] = (subject or "(No subject)").strip()
    message.set_content(body_text or "")

    for attachment in attachments or []:
        filename = (attachment.get("filename") or "attachment.bin").strip() or "attachment.bin"
        content = attachment.get("content") or b""
        content_type = (attachment.get("content_type") or "").strip().lower()
        if not content_type:
            guessed_type, _ = mimetypes.guess_type(filename)
            content_type = guessed_type or "application/octet-stream"
        if "/" not in content_type:
            content_type = "application/octet-stream"
        maintype, subtype = content_type.split("/", 1)
        message.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

    return message


def send_compose_message(
    to_value,
    cc_value,
    subject,
    body_text,
    attachments=None,
    thread_id=None,
    db_path="instance/app.sqlite",
):
    service = _get_service()
    if not service:
        return None
    if not to_value:
        return None

    message = _build_email_message(to_value, cc_value, subject, body_text, attachments)
    body = {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")}
    if thread_id:
        body["threadId"] = thread_id

    try:
        sent = service.users().messages().send(userId="me", body=body).execute()
    except Exception as exc:
        print(f"Gmail send failed: {exc}")
        return None

    sent_id = sent.get("id")
    if sent_id:
        sync_message_by_external_id(sent_id, db_path=db_path, service=service)
    return sent_id


def upsert_gmail_draft(
    to_value,
    cc_value,
    subject,
    body_text,
    draft_id=None,
    attachments=None,
    thread_id=None,
    db_path="instance/app.sqlite",
):
    service = _get_service()
    if not service:
        return None

    effective_attachments = attachments or []
    if draft_id:
        draft_data = _get_draft_data(service, draft_id)
        existing_message_data = (draft_data or {}).get("message") or {}
        existing_attachments = _extract_attachment_payloads(
            existing_message_data.get("payload") or {},
            service=service,
            message_id=existing_message_data.get("id"),
        )
        # Preserve Gmail draft attachments when updating from app.
        # If new uploads exist, keep both sets.
        effective_attachments = _merge_attachment_payloads(existing_attachments, attachments or [])

    message = _build_email_message(to_value, cc_value, subject, body_text, effective_attachments)
    message_payload = {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")}
    if thread_id:
        message_payload["threadId"] = thread_id
    body = {"message": message_payload}

    try:
        if draft_id:
            draft_data = (
                service.users()
                .drafts()
                .update(userId="me", id=draft_id, body=body)
                .execute()
            )
        else:
            draft_data = service.users().drafts().create(userId="me", body=body).execute()
    except Exception as exc:
        print(f"Gmail draft upsert failed: {exc}")
        return None

    provider_draft_id = draft_data.get("id")
    message_data = draft_data.get("message") or {}
    if message_data:
        record = _to_db_record(
            message_data,
            service=service,
            provider_draft_id=provider_draft_id,
        )
        if record:
            record["type"] = "draft"
            record["is_read"] = True
            upsert_email_from_provider(record, db_path=db_path)

    return {
        "provider_draft_id": provider_draft_id,
        "external_id": message_data.get("id"),
        "thread_id": message_data.get("threadId"),
    }


def delete_draft_message(provider_draft_id):
    service = _get_service()
    if not service or not provider_draft_id:
        return False

    try:
        service.users().drafts().delete(userId="me", id=provider_draft_id).execute()
        return True
    except Exception as exc:
        print(f"Gmail draft delete failed for {provider_draft_id}: {exc}")
        return False


def sync_drafts_from_gmail(db_path="instance/app.sqlite", max_results=50):
    service = _get_service()
    if not service:
        return 0

    target = max(1, int(max_results or 50))
    page_token = None
    synced = 0
    visited = 0

    while visited < target:
        page_size = min(100, target - visited)
        try:
            response = (
                service.users()
                .drafts()
                .list(userId="me", maxResults=page_size, pageToken=page_token)
                .execute()
            )
        except Exception as exc:
            print(f"Gmail draft list failed: {exc}")
            break

        draft_refs = response.get("drafts") or []
        if not draft_refs:
            break

        for entry in draft_refs:
            visited += 1
            provider_draft_id = entry.get("id")
            if not provider_draft_id:
                continue
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

            if visited >= target:
                break

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return synced


def set_message_read_state(external_id, read, db_path="instance/app.sqlite"):
    service = _get_service()
    if not service or not external_id:
        return False

    add_labels = [] if read else ["UNREAD"]
    remove_labels = ["UNREAD"] if read else []
    if not _modify_labels(service, external_id, add_labels, remove_labels):
        return False

    sync_message_by_external_id(external_id, db_path=db_path, service=service)
    return True


def set_message_type(external_id, new_type, db_path="instance/app.sqlite"):
    service = _get_service()
    if not service or not external_id:
        return False

    label_changes = {
        "junk": (["SPAM"], ["INBOX"]),
        "response-needed": (["INBOX", "UNREAD"], ["SPAM"]),
        "read-only": (["INBOX"], ["SPAM", "UNREAD"]),
        # Keep these in inbox while letting local DB track the uncertain-junk bucket.
        "junk-uncertain": (["INBOX"], ["SPAM"]),
    }
    add_labels, remove_labels = label_changes.get(new_type, (None, None))
    if add_labels is None:
        return False

    if not _modify_labels(service, external_id, add_labels, remove_labels):
        return False

    sync_message_by_external_id(external_id, db_path=db_path, service=service)
    return True


def send_reply_message(
    source_email,
    reply_text,
    to_value,
    cc_value="",
    attachments=None,
    db_path="instance/app.sqlite",
):
    service = _get_service()
    if not service:
        return None

    if not to_value or not reply_text:
        return None

    subject = (source_email.get("title") or "(No subject)").strip()
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"

    message = _build_email_message(
        to_value=to_value,
        cc_value=cc_value,
        subject=subject,
        body_text=reply_text,
        attachments=attachments,
    )

    body = {
        "raw": base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8"),
    }

    thread_id = source_email.get("thread_id")
    if thread_id:
        body["threadId"] = thread_id

    try:
        sent = service.users().messages().send(userId="me", body=body).execute()
    except Exception as exc:
        print(f"Gmail send failed: {exc}")
        return None

    sent_id = sent.get("id")
    if sent_id:
        sync_message_by_external_id(sent_id, db_path=db_path, service=service)
    return sent_id


def trash_message(external_id):
    service = _get_service()
    if not service or not external_id:
        return False

    try:
        service.users().messages().trash(userId="me", id=external_id).execute()
        return True
    except Exception as exc:
        print(f"Gmail trash failed for {external_id}: {exc}")
        return False
