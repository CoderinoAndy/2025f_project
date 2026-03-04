import os
import re
import threading
import time

from ..db import fetch_emails
from ..gmail_service import sync_drafts_from_gmail

HIDDEN_FROM_MAIN_LIST_TYPES = {"sent", "draft"}
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
SEARCH_TOKEN_PATTERN = re.compile(r"[a-z0-9._%+\-]+")


def _env_int(name, default):
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
DRAFT_SYNC_INTERVAL_SECONDS = max(
    10, _env_int("DRAFT_SYNC_INTERVAL_SECONDS", 45)
)


class _MailboxSyncState:
    """In-memory mailbox sync throttling state."""

    def __init__(self):
        self.last_deep_live_sync_at = 0.0
        self.last_draft_sync_at = 0.0
        self.draft_sync_lock = threading.Lock()


MAILBOX_SYNC_STATE = _MailboxSyncState()


def maybe_get_live_sync_max_results(sync_requested):
    """Return sync max results when live sync should run, else None."""
    if not sync_requested:
        return None
    sync_state = MAILBOX_SYNC_STATE
    now = time.time()
    sync_max_results = LIVE_EMAIL_SYNC_MAX_RESULTS
    if now - sync_state.last_deep_live_sync_at >= LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS:
        sync_max_results = max(sync_max_results, LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS)
        sync_state.last_deep_live_sync_at = now
    return sync_max_results


def trigger_draft_sync_async(max_results=40, force=False):
    """Run draft sync in a background thread with interval and lock guards."""
    sync_state = MAILBOX_SYNC_STATE
    now = time.time()
    if not force and now - sync_state.last_draft_sync_at < DRAFT_SYNC_INTERVAL_SECONDS:
        return False
    if not sync_state.draft_sync_lock.acquire(blocking=False):
        return False

    target = max(1, int(max_results or 40))

    def _worker():
        try:
            sync_drafts_from_gmail(max_results=target)
            sync_state.last_draft_sync_at = time.time()
        finally:
            sync_state.draft_sync_lock.release()

    threading.Thread(target=_worker, daemon=True).start()
    return True


def fetch_live_list_emails(list_view, search_query=""):
    """Fetch rows for a live mailbox list view."""
    config = LIVE_LIST_CONFIGS.get(list_view)
    if not config:
        return None, None
    if config.get("sync_drafts"):
        trigger_draft_sync_async(max_results=40)

    email_type = config.get("email_type")
    exclude_types = config.get("exclude_types")
    archived_only = bool(config.get("archived_only"))
    if email_type:
        emails = fetch_emails(
            email_type=email_type,
            archived_only=archived_only,
        )
    else:
        emails = fetch_emails(
            exclude_types=exclude_types,
            archived_only=archived_only,
        )

    filtered_emails = filter_emails_by_query(emails, search_query)
    if (search_query or "").strip():
        return filtered_emails, config.get(
            "search_empty_message",
            "No emails matched your search.",
        )
    return filtered_emails, config["empty_message"]


def _search_haystack(email):
    """Return normalized full-text content for one email row."""
    return " ".join(
        [
            email.get("title") or "",
            email.get("sender") or "",
            email.get("recipients") or "",
            email.get("cc") or "",
            email.get("body") or "",
        ]
    ).lower()


def _search_tokens(text):
    """Return normalized query tokens."""
    return SEARCH_TOKEN_PATTERN.findall(str(text or "").lower())


def _binary_search_token(tokens_sorted, token):
    """Return index of token in sorted token list, or -1 if missing."""
    left = 0
    right = len(tokens_sorted) - 1
    while left <= right:
        middle = (left + right) // 2
        mid_token = tokens_sorted[middle]
        if mid_token == token:
            return middle
        if mid_token < token:
            left = middle + 1
        else:
            right = middle - 1
    return -1


def filter_emails_by_query(emails, query_text):
    """Filter mailbox rows by free-text query."""
    query = (query_text or "").strip()
    if not query:
        return emails

    query_lc = query.lower()
    haystacks = [_search_haystack(email) for email in emails]

    token_to_rows = {}
    for row_index, haystack in enumerate(haystacks):
        for token in set(_search_tokens(haystack)):
            token_to_rows.setdefault(token, []).append(row_index)

    candidate_rows = None
    query_tokens = _search_tokens(query_lc)
    if query_tokens:
        sorted_tokens = sorted(token_to_rows.keys())
        for token in query_tokens:
            token_index = _binary_search_token(sorted_tokens, token)
            if token_index < 0:
                return []
            token_rows = set(token_to_rows[sorted_tokens[token_index]])
            candidate_rows = token_rows if candidate_rows is None else candidate_rows & token_rows
            if not candidate_rows:
                return []

    if candidate_rows is None:
        candidate_rows = set(range(len(emails)))

    filtered = []
    for row_index, email in enumerate(emails):
        if row_index not in candidate_rows:
            continue
        if query_lc in haystacks[row_index]:
            filtered.append(email)
    return filtered
