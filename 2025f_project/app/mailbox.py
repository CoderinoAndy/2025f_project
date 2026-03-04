import os
import re
import threading
import time
from datetime import datetime

from .db import fetch_emails
from .gmail_service import sync_drafts_from_gmail

HIDDEN_FROM_MAIN_LIST_TYPES = {"sent", "draft"}
VALID_SORTS = {
    "date_desc",
    "date_asc",
    "priority_desc",
    "priority_asc",
    "unread_first",
    "read_first",
}
TOKEN_PATTERN = re.compile(r"[a-z0-9._%+\-]+")
LIVE_LIST_CONFIGS = {
    "all": {
        "exclude_types": HIDDEN_FROM_MAIN_LIST_TYPES,
        "empty_message": "Your All Emails tab is empty.",
        "search_empty_message": "No emails matched your search.",
    },
    "read-only": {
        "email_type": "read-only",
        "empty_message": "Your Read Only tab is empty.",
        "search_empty_message": "No emails matched your search in Read only.",
    },
    "response-needed": {
        "email_type": "response-needed",
        "empty_message": "Your Response Needed tab is empty.",
        "search_empty_message": "No emails matched your search in Response needed.",
    },
    "junk": {
        "email_type": "junk",
        "empty_message": "Your Junk tab is empty.",
        "search_empty_message": "No emails matched your search in Junk.",
    },
    "junk-uncertain": {
        "email_type": "junk-uncertain",
        "empty_message": "You have no Junk Mail to confirm.",
        "search_empty_message": "No emails matched your search in Junk Confirmation.",
    },
    "archived": {
        "archived_only": True,
        "exclude_types": HIDDEN_FROM_MAIN_LIST_TYPES,
        "empty_message": "Your Archive is empty.",
        "search_empty_message": "No emails matched your search in Archive.",
    },
    "sent": {
        "email_type": "sent",
        "empty_message": "Your Sent tab is empty.",
        "search_empty_message": "No emails matched your search in Sent.",
    },
    "draft": {
        "email_type": "draft",
        "empty_message": "Your Drafts tab is empty.",
        "search_empty_message": "No emails matched your search in Drafts.",
        "sync_drafts": True,
    },
}


def _env_int(name, default):
    """Read an integer setting from env with a fallback value."""
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


LIVE_EMAIL_POLL_INTERVAL_MS = max(1000, _env_int("LIVE_EMAIL_POLL_INTERVAL_MS", 2000))
LIVE_EMAIL_SYNC_MAX_RESULTS = max(5, _env_int("LIVE_EMAIL_SYNC_MAX_RESULTS", 15))
LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS = max(
    5, _env_int("LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS", 30)
)
LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS = max(
    LIVE_EMAIL_SYNC_MAX_RESULTS,
    _env_int("LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS", 60),
)
DRAFT_SYNC_INTERVAL_SECONDS = max(10, _env_int("DRAFT_SYNC_INTERVAL_SECONDS", 45))


class _MailboxSyncState:
    """Small in-memory state used to throttle sync requests."""

    def __init__(self):
        self.last_deep_live_sync_at = 0.0
        self.last_draft_sync_at = 0.0
        self.draft_sync_lock = threading.Lock()


MAILBOX_SYNC_STATE = _MailboxSyncState()


def maybe_get_live_sync_max_results(sync_requested):
    """Return sync batch size for live polling, or None if polling is off."""
    if not sync_requested:
        return None

    now = time.time()
    batch_size = LIVE_EMAIL_SYNC_MAX_RESULTS
    if now - MAILBOX_SYNC_STATE.last_deep_live_sync_at >= LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS:
        batch_size = LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS
        MAILBOX_SYNC_STATE.last_deep_live_sync_at = now
    return batch_size


def trigger_draft_sync_async(max_results=40, force=False):
    """Run draft sync in a background thread."""
    now = time.time()
    if not force and now - MAILBOX_SYNC_STATE.last_draft_sync_at < DRAFT_SYNC_INTERVAL_SECONDS:
        return False
    if not MAILBOX_SYNC_STATE.draft_sync_lock.acquire(blocking=False):
        return False

    target = max(1, int(max_results or 40))

    def _worker():
        # Always release lock, even if Gmail sync fails.
        try:
            sync_drafts_from_gmail(max_results=target)
            MAILBOX_SYNC_STATE.last_draft_sync_at = time.time()
        finally:
            MAILBOX_SYNC_STATE.draft_sync_lock.release()

    threading.Thread(target=_worker, daemon=True).start()
    return True


def _email_search_text(email):
    """Build one lowercase string used for simple substring search."""
    return " ".join(
        [
            str(email.get("title") or ""),
            str(email.get("sender") or ""),
            str(email.get("recipients") or ""),
            str(email.get("cc") or ""),
            str(email.get("body") or ""),
        ]
    ).lower()


def filter_emails_by_query(emails, query_text):
    """Filter rows using token-index narrowing plus final text verification."""
    query = (query_text or "").strip().lower()
    if not query:
        return emails

    # Keep token parsing simple but robust for punctuation-heavy queries.
    query_tokens = TOKEN_PATTERN.findall(query)
    if not query_tokens:
        return emails

    haystacks = [_email_search_text(email) for email in emails]

    # Build token -> row index map once for this query.
    token_rows = {}
    for row_index, haystack in enumerate(haystacks):
        for token in set(TOKEN_PATTERN.findall(haystack)):
            token_rows.setdefault(token, set()).add(row_index)

    # Intersect row sets so we only scan likely matches.
    candidate_rows = None
    for token in query_tokens:
        rows = token_rows.get(token)
        if not rows:
            return []
        candidate_rows = rows if candidate_rows is None else candidate_rows & rows
        if not candidate_rows:
            return []

    filtered = []
    for row_index in sorted(candidate_rows):
        if query in haystacks[row_index]:
            filtered.append(emails[row_index])
    return filtered


def _parse_date(date_text):
    """Parse the app's date formats into datetime, or None."""
    text = str(date_text or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _date_number(dt):
    """Convert datetime to a comparable integer."""
    if dt is None:
        return -1
    return dt.toordinal() * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second


def _email_sort_key(email, sort_code):
    """Return a tuple key for manual insertion sort."""
    date_num = _date_number(_parse_date(email.get("date")))
    priority = int(email.get("priority") or 0)
    is_read = bool(email.get("is_read"))

    if sort_code == "date_asc":
        return (date_num,)
    if sort_code == "priority_desc":
        return (-priority, -date_num)
    if sort_code == "priority_asc":
        return (priority, -date_num)
    if sort_code == "unread_first":
        return (is_read, -date_num)
    if sort_code == "read_first":
        return (not is_read, -date_num)
    return (-date_num,)


def _merge_sorted_pairs(left_pairs, right_pairs):
    """Merge two sorted (email, key) lists."""
    merged = []
    left_i = 0
    right_i = 0
    while left_i < len(left_pairs) and right_i < len(right_pairs):
        if left_pairs[left_i][1] <= right_pairs[right_i][1]:
            merged.append(left_pairs[left_i])
            left_i += 1
        else:
            merged.append(right_pairs[right_i])
            right_i += 1
    if left_i < len(left_pairs):
        merged.extend(left_pairs[left_i:])
    if right_i < len(right_pairs):
        merged.extend(right_pairs[right_i:])
    return merged


def _merge_sort_pairs(pairs):
    """Sort (email, key) pairs with merge sort in O(n log n)."""
    if len(pairs) <= 1:
        return pairs
    mid = len(pairs) // 2
    left = _merge_sort_pairs(pairs[:mid])
    right = _merge_sort_pairs(pairs[mid:])
    return _merge_sorted_pairs(left, right)


def sort_emails(emails, sort_code):
    """Sort emails with custom merge sort (stable, O(n log n))."""
    if sort_code not in VALID_SORTS:
        sort_code = "date_desc"

    pairs = []
    for email in emails:
        pairs.append((email, _email_sort_key(email, sort_code)))
    sorted_pairs = _merge_sort_pairs(pairs)

    sorted_rows = []
    for email, _key in sorted_pairs:
        sorted_rows.append(email)
    return sorted_rows


def emails_fingerprint(emails):
    """Return a light hash string for live list refresh checks."""
    rows = []
    for email in emails:
        rows.append(
            ":".join(
                [
                    str(email.get("id") or ""),
                    str(int(bool(email.get("is_read")))),
                    str(email.get("type") or ""),
                    str(email.get("date") or ""),
                    str(int(email.get("priority") or 0)),
                    str(email.get("title") or ""),
                ]
            )
        )
    return "|".join(rows)


def build_mailbox_context(
    emails,
    *,
    sort_code,
    current_list_url,
    search_query,
    live_poll_interval_ms,
    include_fingerprint=True,
):
    """Build the template context used by mailbox pages."""
    emails_sorted = sort_emails(emails, sort_code)
    context = {
        "emails": emails_sorted,
        "sort": sort_code,
        "current_list_url": current_list_url,
        "search_query": search_query,
        "live_poll_interval_ms": live_poll_interval_ms,
    }
    if include_fingerprint:
        context["list_fingerprint"] = emails_fingerprint(emails_sorted)
    return context


def fetch_live_list_emails(list_view, search_query=""):
    """Fetch rows + empty message for a live mailbox tab."""
    config = LIVE_LIST_CONFIGS.get(list_view)
    if not config:
        return None, None

    if config.get("sync_drafts"):
        trigger_draft_sync_async(max_results=40)

    email_type = config.get("email_type")
    exclude_types = config.get("exclude_types")
    archived_only = bool(config.get("archived_only"))
    if email_type:
        emails = fetch_emails(email_type=email_type, archived_only=archived_only)
    else:
        emails = fetch_emails(exclude_types=exclude_types, archived_only=archived_only)

    filtered = filter_emails_by_query(emails, search_query)
    if (search_query or "").strip():
        return filtered, config.get("search_empty_message", "No emails matched your search.")
    return filtered, config["empty_message"]
