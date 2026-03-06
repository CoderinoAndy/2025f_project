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

from .db import (
    fetch_email_by_id,
    update_email_ai_fields,
    upsert_email_from_provider,
)
from .ollama_client import (
    ai_enabled as ai_triage_enabled,
    classification_to_email_type,
    classify_email,
)
from .debug_logger import log_event, log_exception

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
DB_DEFAULT = str(PROJECT_ROOT / "instance" / "app.sqlite")
SYNC_INTERVAL_SECONDS = int(os.getenv("GMAIL_SYNC_INTERVAL_SECONDS", "20"))
SYNC_MAX_RESULTS = int(os.getenv("GMAIL_SYNC_MAX_RESULTS", "25"))
AI_TRIAGE_PER_SYNC = int(os.getenv("GMAIL_AI_TRIAGE_PER_SYNC", "0"))
BULK_SENDER_MARKERS = (
    "no-reply",
    "noreply",
    "donotreply",
    "newsletter",
    "digest",
    "news",
    "notifications",
    "announcements",
    "marketing",
)


# Background sync state so repeated page loads do not hammer Gmail API.
class _GmailSyncState:
    """In-memory sync throttling state for background Gmail sync."""

    def __init__(self):
        self.last_sync_at = 0.0
        self.lock = threading.Lock()


GMAIL_SYNC_STATE = _GmailSyncState()


def _candidate_credentials_paths():
    """Candidate credentials paths.
    """
    # Construct fallback candidates in priority order.
    configured_path = os.getenv("GMAIL_CREDENTIALS_FILE")
    candidates = []
    if configured_path:
        candidates.append(Path(configured_path).expanduser())
    candidates.append(PROJECT_ROOT / "credentials.json")
    candidates.append(PROJECT_ROOT.parent / "credentials.json")
    return candidates


def _resolve_credentials_path():
    """Resolve credentials path.
    """
    # Resolve resolve credentials path with configured values and a safe fallback.
    for path in _candidate_credentials_paths():
        if path.exists():
            return path
    return None


def gmail_available():
    """Gmail available.
    """
    # Used by other functions in this file.
    return bool(
        _resolve_credentials_path()
        and Request
        and Credentials
        and InstalledAppFlow
        and build
    )


def _save_token(credentials):
    """Save token.
    """
    # Save token after sanitizing user-provided values.
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(credentials.to_json(), encoding="utf-8")


def _load_credentials():
    """Load credentials.
    """
    if not gmail_available():
        return None

    # Start with token file if present, then fall back to refresh/login.
    credentials = None
    if TOKEN_PATH.exists():
        try:
            credentials = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception:
            credentials = None

    # Reuse cached token when valid, otherwise refresh or launch OAuth consent.
    # Fast path: valid token with required scopes.
    if credentials and credentials.valid and credentials.has_scopes(SCOPES):
        return credentials

    # Next best: refresh existing token without opening browser.
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

    # Last resort: interactive OAuth flow.
    try:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        credentials = flow.run_local_server(port=0)
    except Exception as exc:
        log_exception(
            action_type="gmail_auth",
            action="oauth_setup",
            error=exc,
            component="gmail_service",
            details="Gmail OAuth setup skipped.",
        )
        return None

    _save_token(credentials)
    return credentials


def _get_service():
    # Return the requested value and return a safe fallback when it fails.
    # Build Gmail API client only when auth is available.
    credentials = _load_credentials()
    if not credentials:
        return None
    try:
        return build("gmail", "v1", credentials=credentials, cache_discovery=False)
    except Exception as exc:
        log_exception(
            action_type="gmail_auth",
            action="service_init",
            error=exc,
            component="gmail_service",
            details="Gmail service initialization failed.",
        )
        return None


def _extract_header(payload, header_name):
    """Extract header.
    """
    # Read this field from payloads that may be missing keys.
    for header in payload.get("headers", []) or []:
        if header.get("name", "").lower() == header_name.lower():
            return header.get("value", "")
    return ""


def _decode_body_bytes(raw_data):
    """Decode body bytes.
    """
    # Used by other functions in this file.
    if not raw_data:
        return b""
    padded = f"{raw_data}{'=' * (-len(raw_data) % 4)}"
    try:
        return base64.urlsafe_b64decode(padded.encode("utf-8"))
    except Exception:
        return b""


def _extract_part_header(part, header_name):
    """Extract part header.
    """
    # Read this field from payloads that may be missing keys.
    for header in part.get("headers", []) or []:
        if header.get("name", "").lower() == header_name.lower():
            return header.get("value", "")
    return ""


def _normalize_cid(raw_value):
    """Normalize cid.
    """
    # Normalize cid to one format used across the app.
    if not raw_value:
        return ""
    return raw_value.strip().strip("<>").lower()


def _attachment_bytes(service, message_id, attachment_id):
    """Attachment bytes.
    """
    # Process attachment bytes while preserving filename/type metadata for send/save flows.
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
    """Iter parts.
    """
    # Used by other functions in this file.
    if not payload:
        return
    stack = [payload]
    # Iterative traversal avoids recursion depth issues on large MIME trees.
    while stack:
        current = stack.pop()
        children = current.get("parts") or []
        if children:
            stack.extend(reversed(children))
        yield current


def _guess_filename(content_type, index):
    """Guess filename.
    """
    # Used by other functions in this file.
    mime = (content_type or "").split(";", 1)[0].strip().lower()
    extension = mimetypes.guess_extension(mime) if mime else None
    suffix = extension or ".bin"
    return f"attachment-{index}{suffix}"


def _extract_attachment_payloads(payload, service=None, message_id=None):
    """Extract attachment payloads.
    """
    # Read this field from payloads that may be missing keys.
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
    """Extract attachment metadata.
    """
    # Read this field from payloads that may be missing keys.
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
    """Merge attachment payloads.
    """
    merged = []
    seen = set()
    # Deduplicate by stable content signature so updating drafts does not duplicate files.
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
    """Get draft data.
    """
    # Return the requested value and return a safe fallback when it fails.
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
        log_exception(
            action_type="gmail_api",
            action="draft_fetch",
            error=exc,
            component="gmail_service",
            provider_draft_id=provider_draft_id,
            details="Gmail draft fetch failed.",
        )
        return None


def _get_message_data(service, external_id):
    """Get message data.
    """
    # Return the requested value and return a safe fallback when it fails.
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
        log_exception(
            action_type="gmail_api",
            action="message_fetch",
            error=exc,
            component="gmail_service",
            external_id=external_id,
            details="Gmail message fetch failed.",
        )
        return None


def fetch_draft_attachments(provider_draft_id):
    """Fetch draft attachments.
    """
    # Fetch draft attachments from storage and return normalized rows.
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
    """Fetch draft attachment metadata.
    """
    # Fetch draft attachment metadata from storage and return normalized rows.
    service = _get_service()
    if not service or not provider_draft_id:
        return []
    draft_data = _get_draft_data(service, provider_draft_id)
    if not draft_data:
        return []
    message_data = draft_data.get("message") or {}
    return _extract_attachment_metadata(message_data.get("payload") or {})


def fetch_message_attachments(external_id):
    """Fetch message attachments.
    """
    # Fetch message attachments from storage and return normalized rows.
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
    """Fetch message attachment metadata.
    """
    # Fetch message attachment metadata from storage and return normalized rows.
    service = _get_service()
    if not service or not external_id:
        return []
    message_data = _get_message_data(service, external_id)
    if not message_data:
        return []
    return _extract_attachment_metadata(message_data.get("payload") or {})


def _html_to_text(raw_html):
    """Html recipient text.
    """
    # Used by other functions in this file.
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
    """Replace inline cid sources.
    """
    # Used by other functions in this file.
    if not raw_html or not cid_sources:
        return raw_html

    def replacer(match):
        """Swap `cid:` image references with resolved inline data URLs when available."""
        # Used by other functions in this file.
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
    """Extract message content.
    """
    if not payload:
        return "", None

    plain_text_parts = []
    html_parts = []
    inline_cid_sources = {}
    stack = [payload]

    # Traverse all MIME parts once and collect plain/html bodies + inline image data.
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
    """Parse addresses.
    """
    # Parse and validate this input before using it.
    if not raw_value:
        return ""
    parsed_addresses = [addr for _, addr in getaddresses([raw_value]) if addr]
    return ", ".join(parsed_addresses) if parsed_addresses else raw_value.strip()


def _header_value(payload, name):
    """Header value.
    """
    # Used by other functions in this file.
    return (_extract_header(payload, name) or "").strip()


def _sender_looks_bulk(payload):
    """Sender looks bulk.
    """
    # Used by other functions in this file.
    sender_value = _header_value(payload, "From").lower()
    if any(marker in sender_value for marker in BULK_SENDER_MARKERS):
        return True

    precedence = _header_value(payload, "Precedence").lower()
    if precedence in {"bulk", "list", "junk"}:
        return True

    if _header_value(payload, "List-Unsubscribe"):
        return True
    if _header_value(payload, "List-Id"):
        return True

    auto_submitted = _header_value(payload, "Auto-Submitted").lower()
    if auto_submitted and auto_submitted != "no":
        return True

    return False


def _labels_to_type(label_ids, payload=None):
    """Labels recipient type.
    """
    # Used by other functions in this file.
    labels = set(label_ids or [])
    if "DRAFT" in labels:
        return "draft"
    if "SENT" in labels:
        return "sent"
    if "SPAM" in labels:
        return "junk"
    if "UNREAD" in labels:
        if payload and _sender_looks_bulk(payload):
            return "read-only"
        return "response-needed"
    return "read-only"


def _labels_to_priority(label_ids):
    """Labels recipient priority.
    """
    # Used by other functions in this file.
    labels = set(label_ids or [])
    if "STARRED" in labels:
        return 3
    if "IMPORTANT" in labels:
        return 2
    return 1


def _received_at(internal_date):
    """Received at.
    """
    # Used by other functions in this file.
    try:
        timestamp = int(internal_date) / 1000
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _to_db_record(message, service=None, provider_draft_id=None):
    """Recipient database record.
    """
    label_ids = set(message.get("labelIds") or [])
    # Skip TRASH rows because they are mirrored as deletes in the local mailbox.
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
        "type": _labels_to_type(label_ids, payload=payload),
        "priority": _labels_to_priority(label_ids),
        "is_read": "UNREAD" not in label_ids,
        "received_at": _received_at(message.get("internalDate")),
    }


def sync_message_by_external_id(
    external_id,
    db_path=DB_DEFAULT,
    service=None,
    provider_draft_id=None,
):
    """Sync message by external ID.
    """
    # Sync message by external id between Gmail and the local database.
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
        log_exception(
            action_type="gmail_api",
            action="sync_message_fetch",
            error=exc,
            component="gmail_service",
            external_id=external_id,
            details="Gmail sync fetch failed.",
        )
        return None

    record = _to_db_record(
        message,
        service=service,
        provider_draft_id=provider_draft_id,
    )
    if not record:
        return None
    email_id = upsert_email_from_provider(record, db_path=db_path)
    record["id"] = email_id
    return record


def _should_ai_triage_email(email_data):
    """Return whether AI triage email.
    """
    # Keep this rule in one place so behavior stays consistent.
    if not email_data:
        return False
    if email_data.get("type") in {"sent", "draft"}:
        return False
    if bool(email_data.get("is_archived")):
        return False
    body = str(email_data.get("body") or "").strip()
    title = str(email_data.get("title") or "").strip()
    if not body and not title:
        return False
    classification_missing = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )
    return classification_missing


def _triage_email_with_ai(email_data, db_path):
    """Triage email with ai.
    """
    # Normalize to the fixed labels used by mailbox triage.
    classification = classify_email(
        email_data=email_data,
        email_id=email_data.get("id"),
    )
    if not classification:  # Skip updates when classification output is missing/invalid.
        return False

    current_type = str(email_data.get("type") or "").strip()
    ai_type = classification_to_email_type(classification)
    if current_type in {"junk", "junk-uncertain"}:
        ai_type = current_type

    update_email_ai_fields(
        email_id=email_data["id"],
        email_type=ai_type,
        ai_category=classification.get("category"),
        ai_needs_response=classification.get("needs_response"),
        ai_confidence=classification.get("confidence"),
        db_path=db_path,
    )
    return True


def sync_recent_emails(db_path=DB_DEFAULT, max_results=None):
    """Sync recent emails.
    """
    service = _get_service()
    if not service:
        log_event(
            action_type="gmail_sync",
            action="sync_recent_skip",
            status="skipped",
            component="gmail_service",
            reason="service_unavailable",
            details="Gmail service unavailable; skipping recent sync.",
        )
        return 0

    # Separate throughput controls: sync up to caller target, but cap AI triage work
    # per run so classification latency does not dominate normal mailbox syncing.
    target = max(1, int(max_results or SYNC_MAX_RESULTS))
    log_event(
        action_type="gmail_sync",
        action="sync_recent_start",
        status="start",
        component="gmail_service",
        db_path=db_path,
        max_results=target,
    )
    triage_budget = max(0, min(10, int(AI_TRIAGE_PER_SYNC or 0)))
    triage_used = 0
    page_token = None
    synced = 0
    visited = 0

    # Pull message IDs page-by-page and stop on either budget exhaustion, empty pages,
    # or list API errors (logged and treated as a graceful early exit).
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
            log_exception(
                action_type="gmail_sync",
                action="list_messages",
                error=exc,
                component="gmail_service",
                details="Gmail message list failed.",
                db_path=db_path,
            )
            break
        messages = response.get("messages") or []
        if not messages:
            break

        # Sync each message id immediately; only after a successful sync do we attempt
        # optional AI triage, and only while triage budget remains for this run.
        for item in messages:
            visited += 1
            external_id = item.get("id")
            if not external_id:
                continue
            synced_record = sync_message_by_external_id(
                external_id,
                db_path=db_path,
                service=service,
            )
            if synced_record:
                synced += 1
                if triage_used < triage_budget and ai_triage_enabled():
                    try:
                        email_id = synced_record.get("id")
                        email_data = fetch_email_by_id(email_id, db_path=db_path)
                        if _should_ai_triage_email(email_data):
                            if _triage_email_with_ai(email_data, db_path):
                                triage_used += 1
                    except Exception as exc:
                        log_exception(
                            action_type="gmail_sync",
                            action="ai_triage",
                            error=exc,
                            component="gmail_service",
                            external_id=external_id,
                            details="Gmail AI triage failed.",
                        )
            if visited >= target:
                break
        # Advance to the next page token from Gmail; missing token means we reached
        # the end of the result set and should finish this sync cycle.
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    # Emit one completion event with counters so downstream logs can track sync depth
    # and how much AI triage work happened within this invocation.
    log_event(
        action_type="gmail_sync",
        action="sync_recent_complete",
        status="ok",
        component="gmail_service",
        synced=synced,
        visited=visited,
        triage_used=triage_used,
    )
    return synced


def trigger_background_sync(db_path=DB_DEFAULT, force=False, max_results=None):
    """Trigger background sync.
    """
    sync_state = GMAIL_SYNC_STATE
    now = time.time()
    # Skip background work when a recent sync already ran unless caller explicitly forces it.
    if not force and now - sync_state.last_sync_at < SYNC_INTERVAL_SECONDS:
        log_event(
            action_type="gmail_sync",
            action="background_sync_skip",
            status="skipped",
            component="gmail_service",
            reason="rate_limited",
        )
        return False
    if not sync_state.lock.acquire(blocking=False):
        log_event(
            action_type="gmail_sync",
            action="background_sync_skip",
            status="skipped",
            component="gmail_service",
            reason="already_running",
        )
        return False

    def _worker():
        """Run one background sync cycle and always release the sync lock afterward."""
        # Manage worker lifecycle so asynchronous UI polling stays consistent.
        try:
            sync_recent_emails(db_path=db_path, max_results=max_results)
            sync_state.last_sync_at = time.time()
            log_event(
                action_type="gmail_sync",
                action="background_sync_complete",
                status="ok",
                component="gmail_service",
            )
        except Exception as exc:
            log_exception(
                action_type="gmail_sync",
                action="background_sync_worker",
                error=exc,
                component="gmail_service",
                details="Background sync worker crashed.",
            )
        finally:
            sync_state.lock.release()

    log_event(
        action_type="gmail_sync",
        action="background_sync_start",
        status="start",
        component="gmail_service",
        force=bool(force),
        max_results=max_results,
    )
    threading.Thread(target=_worker, daemon=True).start()
    return True


def _modify_labels(service, external_id, add_labels=None, remove_labels=None):
    """Modify labels.
    """
    # Used by other functions in this file.
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
        log_exception(
            action_type="gmail_api",
            action="modify_labels",
            error=exc,
            component="gmail_service",
            external_id=external_id,
            details="Gmail label update failed.",
        )
        return False


def _build_email_message(to_value, cc_value, subject, body_text, attachments=None):
    """Build email message.
    """
    # Build email message in the exact shape expected by the next API/database call.
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
    db_path=DB_DEFAULT,
):
    """Send compose message.
    """
    # Translate between API payloads and our local mailbox shape.
    service = _get_service()
    if not service:
        return None
    if not to_value:
        return None

    log_event(
        action_type="gmail_send",
        action="compose_send_start",
        status="start",
        component="gmail_service",
        to_value=to_value,
        thread_id=thread_id,
        attachment_count=len(attachments or []),
    )

    # Build RFC822 payload once, then optionally attach thread context for replies.
    message = _build_email_message(to_value, cc_value, subject, body_text, attachments)
    body = {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")}
    if thread_id:
        body["threadId"] = thread_id

    try:
        sent = service.users().messages().send(userId="me", body=body).execute()
    except Exception as exc:
        log_exception(
            action_type="gmail_send",
            action="compose_send",
            error=exc,
            component="gmail_service",
            details="Gmail compose send failed.",
            to_value=to_value,
        )
        return None

    # Re-sync the just-sent message so local mailbox state reflects provider labels/body.
    sent_id = sent.get("id")
    if sent_id:
        sync_message_by_external_id(sent_id, db_path=db_path, service=service)
    log_event(
        action_type="gmail_send",
        action="compose_send_complete",
        status="ok",
        component="gmail_service",
        sent_id=sent_id,
    )
    return sent_id


def upsert_gmail_draft(
    to_value,
    cc_value,
    subject,
    body_text,
    draft_id=None,
    attachments=None,
    thread_id=None,
    db_path=DB_DEFAULT,
):
    """Upsert Gmail draft.
    """
    service = _get_service()
    if not service:
        return None

    log_event(
        action_type="gmail_draft",
        action="upsert_start",
        status="start",
        component="gmail_service",
        draft_id=draft_id or "",
        thread_id=thread_id or "",
        attachment_count=len(attachments or []),
    )

    # For draft updates, preserve existing Gmail attachments and merge in new uploads.
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
        log_exception(
            action_type="gmail_draft",
            action="upsert",
            error=exc,
            component="gmail_service",
            details="Gmail draft upsert failed.",
            draft_id=draft_id or "",
        )
        return None

    provider_draft_id = draft_data.get("id")
    message_data = draft_data.get("message") or {}
    # Mirror provider draft state into the local DB so compose + drafts views stay aligned.
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

    log_event(
        action_type="gmail_draft",
        action="upsert_complete",
        status="ok",
        component="gmail_service",
        provider_draft_id=provider_draft_id or "",
        external_id=message_data.get("id") or "",
    )
    return {
        "provider_draft_id": provider_draft_id,
        "external_id": message_data.get("id"),
        "thread_id": message_data.get("threadId"),
    }


def delete_draft_message(provider_draft_id):
    """Delete draft message.
    """
    # Delete draft message and clean dependent state where required.
    service = _get_service()
    if not service or not provider_draft_id:
        return False

    try:
        service.users().drafts().delete(userId="me", id=provider_draft_id).execute()
        log_event(
            action_type="gmail_draft",
            action="delete",
            status="ok",
            component="gmail_service",
            provider_draft_id=provider_draft_id,
        )
        return True
    except Exception as exc:
        log_exception(
            action_type="gmail_draft",
            action="delete",
            error=exc,
            component="gmail_service",
            details="Gmail draft delete failed.",
            provider_draft_id=provider_draft_id,
        )
        return False


def sync_drafts_from_gmail(db_path=DB_DEFAULT, max_results=50):
    """Sync drafts from Gmail.
    """
    # Sync drafts from gmail between Gmail and the local database.
    service = _get_service()
    if not service:
        return 0

    target = max(1, int(max_results or 50))
    log_event(
        action_type="gmail_draft",
        action="sync_start",
        status="start",
        component="gmail_service",
        max_results=target,
    )
    page_token = None
    synced = 0
    visited = 0

    # Page through provider drafts until caller budget is exhausted or list ends/errors.
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
            log_exception(
                action_type="gmail_draft",
                action="sync_list",
                error=exc,
                component="gmail_service",
                details="Gmail draft list failed.",
            )
            break

        draft_refs = response.get("drafts") or []
        if not draft_refs:
            break

        # Fetch each draft payload and mirror it into local storage as a "draft" row.
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

        # Advance list pagination; no next token means we reached the end of drafts.
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    log_event(
        action_type="gmail_draft",
        action="sync_complete",
        status="ok",
        component="gmail_service",
        synced=synced,
        visited=visited,
    )
    return synced


def set_message_read_state(external_id, read, db_path=DB_DEFAULT):
    """Set message read state.
    """
    # Set message read state while keeping local and provider state aligned when possible.
    service = _get_service()
    if not service or not external_id:
        return False

    add_labels = [] if read else ["UNREAD"]
    remove_labels = ["UNREAD"] if read else []
    if not _modify_labels(service, external_id, add_labels, remove_labels):
        return False

    sync_message_by_external_id(external_id, db_path=db_path, service=service)
    log_event(
        action_type="gmail_labels",
        action="set_read_state",
        status="ok",
        component="gmail_service",
        external_id=external_id,
        read=bool(read),
    )
    return True


def set_message_type(external_id, new_type, db_path=DB_DEFAULT):
    """Set message type.
    """
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
    log_event(
        action_type="gmail_labels",
        action="set_message_type",
        status="ok",
        component="gmail_service",
        external_id=external_id,
        new_type=new_type,
    )
    return True


def send_reply_message(
    source_email,
    reply_text,
    to_value,
    cc_value="",
    attachments=None,
    db_path=DB_DEFAULT,
):
    """Send reply message.
    """
    # Generate, revise, or validate send reply message used by reply and draft workflows.
    service = _get_service()
    if not service:
        return None

    if not to_value or not reply_text:
        return None

    log_event(
        action_type="gmail_send",
        action="reply_send_start",
        status="start",
        component="gmail_service",
        to_value=to_value,
        attachment_count=len(attachments or []),
        source_email_id=source_email.get("id") if isinstance(source_email, dict) else "",
    )

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
        log_exception(
            action_type="gmail_send",
            action="reply_send",
            error=exc,
            component="gmail_service",
            details="Gmail reply send failed.",
            to_value=to_value,
        )
        return None

    sent_id = sent.get("id")
    if sent_id:
        sync_message_by_external_id(sent_id, db_path=db_path, service=service)
    log_event(
        action_type="gmail_send",
        action="reply_send_complete",
        status="ok",
        component="gmail_service",
        sent_id=sent_id or "",
    )
    return sent_id


def trash_message(external_id):
    """Trash message.
    """
    # Translate between API payloads and our local mailbox shape.
    service = _get_service()
    if not service or not external_id:
        return False

    try:
        service.users().messages().trash(userId="me", id=external_id).execute()
        log_event(
            action_type="gmail_message",
            action="trash",
            status="ok",
            component="gmail_service",
            external_id=external_id,
        )
        return True
    except Exception as exc:
        log_exception(
            action_type="gmail_message",
            action="trash",
            error=exc,
            component="gmail_service",
            external_id=external_id,
            details="Gmail trash failed.",
        )
        return False
