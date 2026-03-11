import re
import quopri
from html import unescape


_QP_ESCAPE_PATTERN = re.compile(r"=(?:[0-9A-F]{2}|[\r\n])", re.IGNORECASE)
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
_MOJIBAKE_MARKERS = (
    "â€™",
    "â€œ",
    "â€\x9d",
    "â€˜",
    "â€“",
    "â€”",
    "â€¦",
    "â€¢",
    "â„¢",
    "Ã",
    "Â",
)


def _compact_text(value):
    return " ".join(str(value or "").split()).strip()


def _extract_charset(content_type):
    match = _CHARSET_PATTERN.search(str(content_type or ""))
    return match.group(1).strip() if match else ""


def _mojibake_marker_count(text):
    value = str(text or "")
    return sum(value.count(marker) for marker in _MOJIBAKE_MARKERS)


def _html_entity_count(text):
    return len(_HTML_ENTITY_PATTERN.findall(str(text or "")))


def _repair_common_mojibake(text):
    value = str(text or "")
    if not value:
        return ""

    marker_count = _mojibake_marker_count(value)
    if marker_count <= 0 and not re.search(r"[ÃÂâ].{0,2}[€™€œ”‘–—…•™]", value):
        return value

    best = value
    best_score = _text_quality_score(value) - (_mojibake_marker_count(value) * 0.04)
    for encoding in ("cp1252", "latin-1"):
        try:
            repaired = value.encode(encoding).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        candidate_score = _text_quality_score(repaired) - (_mojibake_marker_count(repaired) * 0.04)
        if candidate_score > (best_score + 0.08):
            best = repaired
            best_score = candidate_score
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
    cleaned = _INVISIBLE_CHAR_PATTERN.sub(" ", cleaned)
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
        1 for char in compact if char.isalnum() or char.isspace() or char in ".,:;!?'-()/%&"
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
    if "=3D" in value or "=20" in value or "=E2=" in value:
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
    decoded_bytes = raw
    if "quoted-printable" in encoding:
        decoded_bytes = quopri.decodestring(raw)

    text = _decode_bytes(decoded_bytes, charset=charset)
    if looks_transfer_encoded_text(text):
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
    return _normalize_visible_text(str(value or ""))
