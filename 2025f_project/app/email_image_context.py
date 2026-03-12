import base64
import hashlib
import re
from html.parser import HTMLParser
from urllib.parse import unquote, urlparse

from .email_content import repair_body_text


_DECORATIVE_MARKERS = (
    "logo",
    "icon",
    "spacer",
    "pixel",
    "tracker",
    "tracking",
    "facebook",
    "instagram",
    "linkedin",
    "twitter",
    "youtube",
    "social",
    "avatar",
    "profile",
)
_INFORMATIVE_MARKERS = (
    "screenshot",
    "receipt",
    "invoice",
    "statement",
    "bill",
    "chart",
    "graph",
    "diagram",
    "timeline",
    "schedule",
    "form",
    "document",
    "scan",
    "poster",
    "flyer",
    "menu",
    "whiteboard",
    "presentation",
    "slide",
    "photo",
    "picture",
)
_IMAGE_CONTEXT_MARKERS = (
    "see attached",
    "attached",
    "attachment",
    "screenshot",
    "screen shot",
    "photo",
    "image",
    "picture",
    "scan",
    "scanned",
    "invoice",
    "receipt",
    "statement",
    "poster",
    "chart",
    "graph",
)
_DATA_URL_RE = re.compile(r"^data:(image/[a-z0-9.+-]+);base64,([a-z0-9+/=\s]+)$", re.IGNORECASE)


def _compact_text(value):
    return " ".join(str(value or "").split()).strip()


def _short_text(value, max_chars=180):
    compact = _compact_text(value)
    if len(compact) <= max_chars:
        return compact
    return f"{compact[: max_chars - 3].rstrip()}..."


def _parse_dimension(value):
    match = re.search(r"\d+", str(value or ""))
    if not match:
        return None
    try:
        return int(match.group(0))
    except (TypeError, ValueError):
        return None


def _filename_from_src(src):
    value = str(src or "").strip()
    if not value:
        return ""
    if value.lower().startswith("data:image/"):
        return "inline-image"
    parsed = urlparse(value)
    path = unquote(parsed.path or "")
    if not path:
        return ""
    filename = path.rsplit("/", 1)[-1].strip()
    if filename.lower().startswith("cid:"):
        return ""
    return filename


def _candidate_label(candidate):
    for key in ("alt", "title", "aria_label", "filename", "nearby_text"):
        label = _short_text(candidate.get(key) or "")
        if label:
            return label
    return ""


def _looks_decorative(text):
    lowered = _compact_text(text).lower()
    return bool(lowered) and any(marker in lowered for marker in _DECORATIVE_MARKERS)


def _looks_informative(text):
    lowered = _compact_text(text).lower()
    return bool(lowered) and any(marker in lowered for marker in _INFORMATIVE_MARKERS)


def _is_meaningful_html_candidate(candidate):
    label_blob = " ".join(
        [
            str(candidate.get("alt") or ""),
            str(candidate.get("title") or ""),
            str(candidate.get("aria_label") or ""),
            str(candidate.get("filename") or ""),
            str(candidate.get("nearby_text") or ""),
        ]
    )
    width = candidate.get("width")
    height = candidate.get("height")
    informative = _looks_informative(label_blob)
    decorative = _looks_decorative(label_blob)
    if decorative and not informative:
        return False
    if width and height and width <= 48 and height <= 48 and not informative:
        return False
    if not _compact_text(label_blob):
        if width and height and width <= 96 and height <= 96:
            return False
        if not candidate.get("is_inline_data"):
            return False
    return True


def _attachment_priority(filename, *, size=0, hint_text=""):
    score = 0
    blob = f"{filename} {hint_text}".lower()
    if _looks_informative(blob):
        score += 4
    if _looks_decorative(blob):
        score -= 5
    if size >= 40_000:
        score += 1
    if size >= 120_000:
        score += 1
    if len(filename or "") >= 10:
        score += 1
    return score


class _HtmlImageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.candidates = []
        self._recent_text = ""
        self._pending_index = None
        self._ignored_tag_depth = 0

    def handle_starttag(self, tag, attrs):
        lowered = str(tag or "").lower()
        if lowered in {"script", "style", "svg", "noscript"}:
            self._ignored_tag_depth += 1
            return
        if lowered != "img" or self._ignored_tag_depth:
            return
        attr_map = {}
        for key, value in attrs:
            normalized_key = str(key or "").lower()
            if not normalized_key:
                continue
            attr_map[normalized_key] = str(value or "")
        src = attr_map.get("src") or attr_map.get("data-src") or attr_map.get("data-original")
        candidate = {
            "alt": _short_text(attr_map.get("alt")),
            "title": _short_text(attr_map.get("title")),
            "aria_label": _short_text(attr_map.get("aria-label")),
            "filename": _filename_from_src(src),
            "src": src or "",
            "width": _parse_dimension(attr_map.get("width")),
            "height": _parse_dimension(attr_map.get("height")),
            "before_text": _short_text(self._recent_text, max_chars=160),
            "after_text": "",
            "nearby_text": "",
            "is_inline_data": str(src or "").lower().startswith("data:image/"),
        }
        self.candidates.append(candidate)
        self._pending_index = len(self.candidates) - 1

    def handle_endtag(self, tag):
        lowered = str(tag or "").lower()
        if lowered in {"script", "style", "svg", "noscript"} and self._ignored_tag_depth:
            self._ignored_tag_depth -= 1

    def handle_data(self, data):
        if self._ignored_tag_depth:
            return
        text = _compact_text(data)
        if not text:
            return
        if self._pending_index is not None:
            candidate = self.candidates[self._pending_index]
            if not candidate.get("after_text"):
                candidate["after_text"] = _short_text(text, max_chars=160)
            self._pending_index = None
        merged = f"{self._recent_text} {text}".strip()
        self._recent_text = _short_text(merged, max_chars=320)


def extract_html_image_candidates(raw_html):
    html = str(raw_html or "").strip()
    if not html or "<img" not in html.lower():
        return []
    parser = _HtmlImageParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return []
    candidates = []
    for candidate in parser.candidates:
        nearby_parts = [candidate.get("before_text"), candidate.get("after_text")]
        candidate["nearby_text"] = _short_text(" ".join(part for part in nearby_parts if part))
        if _is_meaningful_html_candidate(candidate):
            candidates.append(candidate)
    return candidates


def filter_image_attachment_metadata(metadata_rows):
    filtered = []
    for item in metadata_rows or []:
        content_type = str(item.get("content_type") or "").strip().lower()
        if not content_type.startswith("image/"):
            continue
        filename = str(item.get("filename") or "").strip() or "attachment-image"
        size = item.get("size") or 0
        try:
            size_value = max(0, int(size))
        except (TypeError, ValueError):
            size_value = 0
        if _looks_decorative(filename) and not _looks_informative(filename):
            continue
        filtered.append(
            {
                "filename": filename,
                "content_type": content_type,
                "size": size_value,
                "priority": _attachment_priority(filename, size=size_value),
            }
        )
    filtered.sort(
        key=lambda item: (
            -int(item.get("priority") or 0),
            -int(item.get("size") or 0),
            str(item.get("filename") or ""),
        )
    )
    return filtered


def body_suggests_image_context(title, body_text):
    combined = f"{title or ''} {body_text or ''}".lower()
    return any(marker in combined for marker in _IMAGE_CONTEXT_MARKERS)


def should_fetch_attachment_image_metadata(title, body_text, html_candidates):
    if html_candidates:
        return True
    compact_body = _compact_text(body_text)
    if body_suggests_image_context(title, compact_body):
        return True
    return len(compact_body) < 220


def should_enrich_with_vision(title, body_text, html_candidates, attachment_images):
    image_count = len(html_candidates or []) + len(attachment_images or [])
    if image_count <= 0:
        return False
    compact_body = _compact_text(body_text)
    if not compact_body:
        return True
    if body_suggests_image_context(title, compact_body):
        return True
    if image_count >= 2:
        return True
    return len(compact_body) < 420


def build_basic_image_context(html_candidates, attachment_images, vision_notes=None, max_chars=1600):
    lines = []
    total_images = len(html_candidates or []) + len(attachment_images or [])
    if total_images <= 0 and not _compact_text(vision_notes):
        return ""

    if total_images > 0:
        lines.append(f"Detected {total_images} potentially relevant email image(s).")
    for candidate in (html_candidates or [])[:2]:
        details = []
        label = _candidate_label(candidate)
        if label:
            details.append(f'label "{label}"')
        nearby = _short_text(candidate.get("nearby_text") or "", max_chars=120)
        if nearby and nearby.lower() != str(label or "").lower():
            details.append(f'nearby text "{nearby}"')
        if candidate.get("filename") and candidate.get("filename") != "inline-image":
            details.append(f'file "{candidate["filename"]}"')
        if not details:
            details.append("inline image present")
        lines.append(f"- Inline image: {'; '.join(details)}.")

    for attachment in (attachment_images or [])[:2]:
        size_value = int(attachment.get("size") or 0)
        size_kb = max(1, round(size_value / 1024)) if size_value else 0
        size_label = f", about {size_kb} KB" if size_kb else ""
        lines.append(
            "- Attached image file "
            f'"{attachment.get("filename") or "attachment-image"}" '
            f'({attachment.get("content_type") or "image"}{size_label}).'
        )

    vision_text = _compact_text(vision_notes)
    if vision_text:
        lines.append(f"- Vision/OCR notes: {vision_text}")

    output = ""
    for line in lines:
        candidate_output = line if not output else f"{output}\n{line}"
        if len(candidate_output) > max_chars:
            break
        output = candidate_output
    return output.strip()


def extract_inline_image_payloads(html_candidates, max_images=2, max_bytes=800_000):
    payloads = []
    seen = set()
    for candidate in html_candidates or []:
        src = str(candidate.get("src") or "").strip()
        match = _DATA_URL_RE.match(src)
        if not match:
            continue
        content_type = match.group(1).strip().lower()
        try:
            raw = base64.b64decode(match.group(2), validate=False)
        except Exception:
            continue
        if not raw or len(raw) > max_bytes:
            continue
        digest = hashlib.sha1(raw).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        payloads.append(
            {
                "filename": candidate.get("filename") or "inline-image",
                "content_type": content_type,
                "content_base64": base64.b64encode(raw).decode("ascii"),
                "label": _candidate_label(candidate),
                "priority": _attachment_priority(
                    candidate.get("filename") or "",
                    size=len(raw),
                    hint_text=_candidate_label(candidate),
                ),
            }
        )
        if len(payloads) >= max_images:
            break
    return payloads


def extract_attachment_image_payloads(attachments, max_images=2, max_bytes=800_000):
    payloads = []
    seen = set()
    for item in attachments or []:
        content_type = str(item.get("content_type") or "").strip().lower()
        if not content_type.startswith("image/"):
            continue
        filename = str(item.get("filename") or "").strip() or "attachment-image"
        content = item.get("content") or b""
        if not content or len(content) > max_bytes:
            continue
        if _looks_decorative(filename) and not _looks_informative(filename):
            continue
        digest = hashlib.sha1(content).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        payloads.append(
            {
                "filename": filename,
                "content_type": content_type,
                "content_base64": base64.b64encode(content).decode("ascii"),
                "label": filename,
                "priority": _attachment_priority(filename, size=len(content)),
            }
        )
        if len(payloads) >= max_images:
            break
    return payloads


def email_text_for_image_gating(email_data):
    if not isinstance(email_data, dict):
        return repair_body_text(email_data or "", None)
    return repair_body_text(email_data.get("body") or "", email_data.get("body_html"))
