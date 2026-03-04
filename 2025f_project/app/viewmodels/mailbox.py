from .sorting import sort_emails


def emails_fingerprint(emails):
    """Return a lightweight content hash for live row updates."""
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
    """Build template context for mailbox list pages."""
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
