"""Mailbox list helpers: paging, polling, filtering, and stable sorting."""

import os
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlsplit

from .db import count_mailbox_emails, fetch_mailbox_ids, fetch_mailbox_page
from .datetime_utils import parse_known_datetime
from .gmail_service import sync_drafts_from_gmail

# This module keeps list-view behavior centralized so routes only need to ask
# for "the draft tab" or "the archive tab" instead of rebuilding query rules.
HIDDEN_FROM_MAIN_LIST_TYPES = {"sent", "draft"}
VALID_SORTS = {
    "date_desc",
    "date_asc",
    "priority_desc",
    "priority_asc",
    "unread_first",
    "read_first",
}
# Per-tab rules for live mailbox pages and API views.
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
        "live_polling_enabled": False,
    },
    "draft": {
        "email_type": "draft",
        "empty_message": "Your Drafts tab is empty.",
        "search_empty_message": "No emails matched your search in Drafts.",
        "sync_drafts": True,
        "live_polling_enabled": False,
    },
}


def _env_int(name, default):
    """Read an integer setting from env with a fallback value."""
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


# Polling and sync knobs from the environment, each clamped to a safe minimum.
LIVE_EMAIL_POLL_INTERVAL_MS = max(1000, _env_int("LIVE_EMAIL_POLL_INTERVAL_MS", 10000))
LIVE_EMAIL_SYNC_MAX_RESULTS = max(5, _env_int("LIVE_EMAIL_SYNC_MAX_RESULTS", 15))
LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS = max(
    5, _env_int("LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS", 30)
)
LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS = max(
    LIVE_EMAIL_SYNC_MAX_RESULTS,
    _env_int("LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS", 60),
)
DRAFT_SYNC_INTERVAL_SECONDS = max(10, _env_int("DRAFT_SYNC_INTERVAL_SECONDS", 45))
MAILBOX_PAGE_SIZE = max(25, min(250, _env_int("MAILBOX_PAGE_SIZE", 100)))


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
    # Most polls do a small sync; every N seconds we run a deeper pass.
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
    # Draft views can trigger refreshes repeatedly, so this stays single-flight and
    # quietly skips overlapping runs instead of letting Gmail draft syncs stack up.
    if not MAILBOX_SYNC_STATE.draft_sync_lock.acquire(blocking=False):
        return False

    # Clamp user or config input so the worker always gets a valid positive count.
    target = max(1, int(max_results or 40))

    def _worker():
        # Always release the lock, even if Gmail sync fails.
        try:
            sync_drafts_from_gmail(max_results=target)
            MAILBOX_SYNC_STATE.last_draft_sync_at = time.time()
        finally:
            MAILBOX_SYNC_STATE.draft_sync_lock.release()

    threading.Thread(target=_worker, daemon=True).start()
    return True


def build_mailbox_pagination(current_list_url, *, page, page_size, total_count):
    """Build canonical paging URLs plus summary fields for mailbox templates."""
    total = max(0, int(total_count or 0))
    safe_page_size = max(1, int(page_size or MAILBOX_PAGE_SIZE))
    total_pages = max(1, (total + safe_page_size - 1) // safe_page_size) if total else 1
    current_page = min(max(1, int(page or 1)), total_pages)

    def _page_url(target_page):
        parsed = urlsplit(current_list_url or "/")
        query_pairs = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key != "page"
        ]
        if target_page > 1:
            query_pairs.append(("page", str(target_page)))
        query_text = urlencode(query_pairs)
        path = parsed.path or "/"
        return f"{path}?{query_text}" if query_text else path

    start_index = 0 if total == 0 else ((current_page - 1) * safe_page_size) + 1
    end_index = min(total, current_page * safe_page_size) if total else 0

    return {
        "current_page": current_page,
        "page_size": safe_page_size,
        "total_count": total,
        "total_pages": total_pages,
        "start_index": start_index,
        "end_index": end_index,
        "has_prev": current_page > 1,
        "has_next": current_page < total_pages,
        "prev_url": _page_url(current_page - 1) if current_page > 1 else None,
        "next_url": _page_url(current_page + 1) if current_page < total_pages else None,
        "current_url": _page_url(current_page),
    }


def mailbox_live_polling_enabled(list_view, search_query="", page=1):
    """Return whether a mailbox page should use live polling."""
    config = LIVE_LIST_CONFIGS.get(list_view) or {}
    if not config.get("live_polling_enabled", True):
        return False
    if (search_query or "").strip():
        return False
    return max(1, int(page or 1)) == 1


def _live_list_query_filters(list_view):
    """Resolve mailbox query filters for a list-view code."""
    config = LIVE_LIST_CONFIGS.get(list_view)
    if not config:
        return None
    return {
        "email_type": config.get("email_type"),
        "exclude_types": config.get("exclude_types"),
        "archived_only": bool(config.get("archived_only")),
    }


def _date_number(date_text):
    """Convert one stored mailbox timestamp into a comparable integer."""
    dt = parse_known_datetime(date_text)
    if dt is None:
        return -1
    return dt.toordinal() * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second


def _email_sort_key(email, sort_code):
    """Return the tuple used for stable mailbox sorting."""
    date_num = _date_number(email.get("date"))
    priority = int(email.get("priority") or 0)
    is_read = bool(email.get("is_read"))

    # Turn the requested sort mode into one tuple so merge sort can stay generic.
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


def sort_emails(emails, sort_code):
    """Sort mailbox rows using Python's stable built-in sort."""
    if sort_code not in VALID_SORTS:
        sort_code = "date_desc"

    # The key function stays centralized above, while the runtime gets to rely
    # on CPython's well-tested stable sort instead of our own merge-sort copy.
    return sorted(list(emails), key=lambda email: _email_sort_key(email, sort_code))


def emails_fingerprint(emails, *, total_count=None, page=None):
    """Return a light hash string for live list refresh checks."""
    rows = []
    rows.append(str(max(0, int(total_count or 0))))
    rows.append(str(max(1, int(page or 1))))
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
    presorted=False,
    empty_message="",
    pagination=None,
    current_page=1,
    live_polling_enabled=True,
):
    """Build the template context used by mailbox pages."""
    # Give every mailbox template the same core fields for consistency.
    emails_sorted = emails if presorted else sort_emails(emails, sort_code)
    context = {
        "emails": emails_sorted,
        "sort": sort_code,
        "current_list_url": current_list_url,
        "search_query": search_query,
        "live_poll_interval_ms": live_poll_interval_ms,
        "empty_message": empty_message,
        "pagination": pagination or {},
        "current_page": max(1, int(current_page or 1)),
        "live_polling_enabled": bool(live_polling_enabled),
    }
    if include_fingerprint:
        pagination_data = pagination or {}
        context["list_fingerprint"] = emails_fingerprint(
            emails_sorted,
            total_count=pagination_data.get("total_count"),
            page=pagination_data.get("current_page", current_page),
        )
    return context


def fetch_live_list_emails(
    list_view,
    search_query="",
    sort_code="date_desc",
    page=1,
    page_size=MAILBOX_PAGE_SIZE,
):
    """Fetch one mailbox page plus empty-state and count metadata."""
    config = LIVE_LIST_CONFIGS.get(list_view)
    filters = _live_list_query_filters(list_view)
    if not config or not filters:
        return None, None, 0, 1

    # The draft tab asks Gmail for fresh drafts before reading the local DB.
    if config.get("sync_drafts"):
        trigger_draft_sync_async(max_results=40)

    safe_page_size = max(1, int(page_size or MAILBOX_PAGE_SIZE))
    total_count = count_mailbox_emails(
        email_type=filters["email_type"],
        exclude_types=filters["exclude_types"],
        archived_only=filters["archived_only"],
        search_query=search_query,
    )
    total_pages = max(1, (total_count + safe_page_size - 1) // safe_page_size) if total_count else 1
    current_page = min(max(1, int(page or 1)), total_pages)
    offset = (current_page - 1) * safe_page_size
    emails = fetch_mailbox_page(
        email_type=filters["email_type"],
        exclude_types=filters["exclude_types"],
        archived_only=filters["archived_only"],
        search_query=search_query,
        sort_code=sort_code,
        limit=safe_page_size,
        offset=offset,
    )
    if (search_query or "").strip():
        return (
            emails,
            config.get("search_empty_message", "No emails matched your search."),
            total_count,
            current_page,
        )
    return emails, config["empty_message"], total_count, current_page


def fetch_live_list_email_ids(list_view, search_query=""):
    """Fetch all email IDs that belong to a mailbox list view/search."""
    filters = _live_list_query_filters(list_view)
    if not filters:
        return None
    return fetch_mailbox_ids(
        email_type=filters["email_type"],
        exclude_types=filters["exclude_types"],
        archived_only=filters["archived_only"],
        search_query=search_query,
    )
