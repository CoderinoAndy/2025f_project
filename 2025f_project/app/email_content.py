import quopri
import re
import unicodedata
from html import unescape


_QP_ESCAPE_PATTERN = re.compile(r"=(?:[0-9A-F]{2}|[\r\n])")
_MARKDOWN_LINK_PATTERN = re.compile(
    r"\[[^\]]{1,160}\]\((?:https?://|mailto:|ht=\s*\r?\n?\s*tps://)[^)]+\)",
    re.IGNORECASE,
)
_HTML_COMMENT_PATTERN = re.compile(r"(?is)<!--.*?-->")
_HTML_DROP_BLOCK_PATTERN = re.compile(
    r"(?is)<(script|style|head|title|meta|noscript|svg)[^>]*>.*?</\1>"
)
_HTML_ENTITY_PATTERN = re.compile(
    r"&(?:#\d{2,7}|#x[0-9a-f]{2,6}|[a-z][a-z0-9]{1,31});",
    re.IGNORECASE,
)
_HTML_BREAK_PATTERN = re.compile(r"(?is)<\s*br\s*/?\s*>")
_HTML_BLOCK_TAG_PATTERN = re.compile(
    r"(?is)</?\s*(?:p|div|li|tr|table|td|th|section|article|header|footer|"
    r"blockquote|ul|ol|h[1-6])(?:\s+[^>]*)?>"
)
_HTML_TAG_PATTERN = re.compile(r"(?is)<[^>]+>")
_CHARSET_PATTERN = re.compile(r"charset\s*=\s*[\"']?([^\"';\s]+)", re.IGNORECASE)
_INVISIBLE_CHAR_PATTERN = re.compile(
    r"[\u00ad\u034f\u061c\u115f\u1160\u17b4\u17b5\u180e\u2000-\u200f\u2028-\u202f\u2060-\u206f\ufeff]"
)
_UNSAFE_TEXT_CONTROL_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_MOJIBAKE_SEQUENCE_PATTERN = re.compile(
    r"(?:"
    r"[\u00c2\u00c3\u00e2\u00ef\u00f0]"
    r"[\u0080-\u00bf\u00a0-\u00ff\u0152\u0153\u0160\u0161\u0178\u017d\u017e\u0192"
    r"\u2010-\u203a\u20ac\u2122]{1,4}"
    r")+"
)
_COMMON_EMAIL_CHAR_TRANSLATION = {
    0x00A0: " ",
    0x00A9: "(C)",
    0x00AB: '"',
    0x00AE: "(R)",
    0x00B7: "-",
    0x00BB: '"',
    0x2010: "-",
    0x2011: "-",
    0x2012: "-",
    0x2013: "-",
    0x2014: "-",
    0x2015: "-",
    0x2018: "'",
    0x2019: "'",
    0x201A: "'",
    0x201B: "'",
    0x201C: '"',
    0x201D: '"',
    0x201E: '"',
    0x201F: '"',
    0x2022: "-",
    0x2023: "-",
    0x2024: ".",
    0x2025: "..",
    0x2026: "...",
    0x2032: "'",
    0x2033: '"',
    0x2039: "<",
    0x203A: ">",
    0x2043: "-",
    0x2122: "TM",
    0x2190: "<-",
    0x2192: "->",
    0x21D0: "<=",
    0x21D2: "=>",
    0x25B6: ">",
    0x25C0: "<",
    0x25CF: "*",
    0x25E6: "*",
    0x2605: "*",
    0x2606: "*",
    0x2713: "OK",
    0x2714: "OK",
}


def _compact_text(value):
    return " ".join(str(value or "").split()).strip()


def _extract_charset(content_type):
    match = _CHARSET_PATTERN.search(str(content_type or ""))
    return match.group(1).strip() if match else ""


def _is_html_content_type(content_type):
    return "text/html" in str(content_type or "").lower()


def _mojibake_marker_count(text):
    value = str(text or "")
    return len(_MOJIBAKE_SEQUENCE_PATTERN.findall(value))


def contains_common_mojibake(text):
    """Return True when text still looks like UTF-8 bytes decoded with the wrong charset."""
    return _mojibake_marker_count(text) > 0


def _html_entity_count(text):
    return len(_HTML_ENTITY_PATTERN.findall(str(text or "")))


def _replace_common_email_symbols(text):
    return str(text or "").translate(_COMMON_EMAIL_CHAR_TRANSLATION)


def _repair_common_mojibake(text):
    value = str(text or "")
    if not value:
        return ""

    marker_count = _mojibake_marker_count(value)
    if marker_count <= 0:
        return value

    best = value
    best_marker_count = marker_count
    best_score = _text_quality_score(value) - (best_marker_count * 0.04)
    for _ in range(2):
        improved = False
        next_best = best
        next_marker_count = best_marker_count
        next_score = best_score
        for encoding in ("cp1252", "latin-1"):
            try:
                repaired = best.encode(encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            repaired = _UNSAFE_TEXT_CONTROL_PATTERN.sub("", repaired)
            repaired_marker_count = _mojibake_marker_count(repaired)
            candidate_score = _text_quality_score(repaired) - (repaired_marker_count * 0.04)
            if (
                repaired_marker_count < next_marker_count
                and candidate_score >= (next_score - 0.02)
            ) or candidate_score > (next_score + 0.08):
                next_best = repaired
                next_marker_count = repaired_marker_count
                next_score = candidate_score
                improved = True
        if not improved:
            break
        best = next_best
        best_marker_count = next_marker_count
        best_score = next_score
    return best


def _decode_bytes(content_bytes, charset=""):
    raw = content_bytes or b""
    if not raw:
        return ""

    candidate_charsets = [charset, "utf-8", "utf-8-sig", "cp1252", "latin-1"]
    tried = set()
    for candidate in candidate_charsets:
        normalized = str(candidate or "").strip().strip('"').strip("'").lower()
        if not normalized or normalized in tried:
            continue
        tried.add(normalized)
        try:
            return raw.decode(normalized)
        except (LookupError, UnicodeDecodeError):
            continue
    return raw.decode("utf-8", errors="ignore")


def _normalize_visible_text(text):
    cleaned = _repair_common_mojibake(str(text or "")).replace("\xa0", " ")
    cleaned = _replace_common_email_symbols(cleaned)
    cleaned = _INVISIBLE_CHAR_PATTERN.sub(" ", cleaned)
    cleaned = _UNSAFE_TEXT_CONTROL_PATTERN.sub("", cleaned)
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _text_quality_score(text):
    compact = _compact_text(text)
    if not compact:
        return 0.0

    length = float(len(compact))
    readable_chars = sum(
        1
        for char in compact
        if char.isalnum()
        or char.isspace()
        or unicodedata.category(char).startswith("P")
        or unicodedata.category(char) in {"Sc", "Sm"}
    )
    qp_hits = len(_QP_ESCAPE_PATTERN.findall(compact))
    markdown_hits = len(_MARKDOWN_LINK_PATTERN.findall(compact))
    html_entity_hits = _html_entity_count(text)
    soft_break_hits = compact.count("=\n") + compact.count("=\r\n")
    penalty = min(
        0.9,
        (qp_hits * 0.03)
        + (markdown_hits * 0.05)
        + (soft_break_hits * 0.04)
        + (html_entity_hits * 0.006),
    )
    return max(0.0, min(1.0, (readable_chars / length) - penalty))


def looks_transfer_encoded_text(text):
    value = str(text or "")
    if not value:
        return False
    qp_hits = len(_QP_ESCAPE_PATTERN.findall(value))
    markdown_hits = len(_MARKDOWN_LINK_PATTERN.findall(value))
    if any(marker in value for marker in ("=3D", "=20", "=09", "=0A", "=0D", "=E2=")):
        return True
    if "=\r\n" in value or "=\n" in value:
        return True
    if qp_hits >= 4:
        return True
    if markdown_hits >= 4 and len(_compact_text(value)) >= 300:
        return True
    return False


def decode_transfer_encoded_text(content_bytes, *, content_type="", transfer_encoding=""):
    raw = content_bytes or b""
    if not raw:
        return ""

    charset = _extract_charset(content_type)
    encoding = str(transfer_encoding or "").strip().lower()
    is_html = _is_html_content_type(content_type)
    text = _decode_bytes(raw, charset=charset)
    if "quoted-printable" in encoding and looks_transfer_encoded_text(text):
        decoded_bytes = quopri.decodestring(raw)
        repaired_text = _decode_bytes(decoded_bytes, charset=charset)
        if _text_quality_score(repaired_text) >= _text_quality_score(text):
            text = repaired_text
    if looks_transfer_encoded_text(text) and not is_html:
        reparsed = quopri.decodestring(text.encode("utf-8", errors="ignore"))
        repaired_text = _decode_bytes(reparsed, charset=charset)
        if _text_quality_score(repaired_text) >= _text_quality_score(text):
            text = repaired_text
    return text


def html_to_text(raw_html):
    html = str(raw_html or "")
    if not html:
        return ""

    cleaned = html.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = _HTML_COMMENT_PATTERN.sub(" ", cleaned)
    cleaned = _HTML_DROP_BLOCK_PATTERN.sub(" ", cleaned)
    cleaned = _HTML_BREAK_PATTERN.sub("\n", cleaned)
    cleaned = _HTML_BLOCK_TAG_PATTERN.sub("\n", cleaned)
    cleaned = _HTML_TAG_PATTERN.sub(" ", cleaned)
    cleaned = unescape(cleaned)
    return _normalize_visible_text(cleaned)


def repair_html_content(raw_html):
    """Repair mojibake and risky punctuation in HTML content without stripping markup."""
    html = str(raw_html or "")
    if not html:
        return ""
    cleaned = _repair_common_mojibake(html)
    cleaned = _replace_common_email_symbols(cleaned)
    cleaned = _INVISIBLE_CHAR_PATTERN.sub(" ", cleaned)
    cleaned = _UNSAFE_TEXT_CONTROL_PATTERN.sub("", cleaned)
    return cleaned.strip()


def normalize_outgoing_text(value, *, preserve_newlines=True):
    """Normalize outgoing text into email-safe punctuation before save/send."""
    text = _repair_common_mojibake(str(value or ""))
    text = _replace_common_email_symbols(text)
    text = _INVISIBLE_CHAR_PATTERN.sub(" ", text)
    text = _UNSAFE_TEXT_CONTROL_PATTERN.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if preserve_newlines:
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()
    return " ".join(text.split()).strip()


def repair_body_text(body_text, body_html=None):
    plain_text = str(body_text or "")
    if looks_transfer_encoded_text(plain_text):
        decoded_plain = decode_transfer_encoded_text(
            plain_text.encode("utf-8", errors="ignore"),
            transfer_encoding="quoted-printable",
        )
        if _text_quality_score(decoded_plain) >= _text_quality_score(plain_text):
            plain_text = decoded_plain

    plain_html_entity_hits = _html_entity_count(plain_text)
    plain_text = unescape(plain_text)
    plain_text = _normalize_visible_text(plain_text.replace("\r\n", "\n").replace("\r", "\n"))
    html_text = html_to_text(body_html)
    plain_markdown_links = len(_MARKDOWN_LINK_PATTERN.findall(plain_text))
    if html_text:
        html_is_useful = len(_compact_text(html_text)) >= 40
        if html_is_useful and (
            not plain_text
            or looks_transfer_encoded_text(plain_text)
            or plain_markdown_links >= 3
            or plain_html_entity_hits >= 6
            or _text_quality_score(html_text) > (_text_quality_score(plain_text) + 0.08)
        ):
            plain_text = html_text

    return _normalize_visible_text(plain_text)


def repair_header_text(value):
    """Repair and normalize short header text like subject or sender."""
    return normalize_outgoing_text(value, preserve_newlines=False)
