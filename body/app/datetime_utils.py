"""Shared helpers for the loose datetime strings stored in the mailbox DB."""

from datetime import datetime


# Old rows mix date-only values with second-level timestamps, so the parser
# accepts the small family of formats that already exist in storage.
KNOWN_EMAIL_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def parse_known_datetime(value):
    """Parse one of the mailbox datetime formats into a ``datetime`` object."""
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in KNOWN_EMAIL_DATETIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def format_known_datetime(value, output_format="%d/%m/%Y %H:%M"):
    """Format a mailbox datetime string, or fall back to the original text."""
    if value is None:
        return ""

    parsed = parse_known_datetime(value)
    if parsed is None:
        return str(value).strip()
    return parsed.strftime(output_format)
