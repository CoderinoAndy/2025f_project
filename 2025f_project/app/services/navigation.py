from urllib.parse import parse_qs, urlsplit

from flask import url_for


def safe_next_url(raw_next, fallback_endpoint="main.allemails"):
    """Return a local list-style URL and collapse nested /email/... next chains."""
    fallback = url_for(fallback_endpoint)
    candidate = (raw_next or "").strip()
    if not candidate:
        return fallback

    seen = set()
    while candidate and candidate not in seen:
        seen.add(candidate)
        if not candidate.startswith("/"):
            return fallback

        parsed = urlsplit(candidate)
        if parsed.scheme or parsed.netloc:
            return fallback

        path = parsed.path or "/"
        query = parsed.query
        if path.startswith("/email/"):
            nested = parse_qs(query).get("next", [None])[0]
            if nested:
                candidate = nested
                continue
            return fallback

        return f"{path}?{query}" if query else path

    return fallback
