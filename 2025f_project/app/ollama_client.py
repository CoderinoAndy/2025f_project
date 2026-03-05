import ipaddress
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4
from .db import fetch_email_by_id, update_draft, update_email_ai_fields
from .debug_logger import log_event

# This module now contains both model-calling code and async AI task orchestration.
OLLAMA_API_URL_DEFAULT = "http://localhost:11434/api/chat"
OLLAMA_MODEL_DEFAULT = "qwen2.5:14b-instruct"
OLLAMA_TIMEOUT_SECONDS_DEFAULT = 12
SUMMARY_MIN_CHARS_DEFAULT = 200
VALID_CATEGORIES = {"urgent", "informational", "junk"}
VALID_EMAIL_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}
LOCALHOST_NAMES = {"localhost"}
JUNK_LOW_CONFIDENCE_THRESHOLD = 0.78
PERSONAL_EMAIL_DOMAINS = (
    "gmail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "protonmail.com",
    "proton.me",
)
AUTOMATED_SENDER_MARKERS = (
    "no-reply",
    "noreply",
    "do-not-reply",
    "donotreply",
    "newsletter",
    "digest",
    "notification",
    "notifications",
    "alerts",
    "updates",
    "announcements",
    "marketing",
    "promotions",
)
REPLY_CHAIN_PATTERNS = (
    r"\n-----\s*Original Message\s*-----",
    r"\nOn\s.+?wrote:",
    r"\nFrom:\s.+\nSent:\s.+\nTo:\s.+\nSubject:\s.+",
)
SUMMARY_FAILURE_MARKERS = (
    "unable to summarize",
    "unable to provide a summary",
    "unable to interpret",
    "unable to understand",
    "i cannot summarize",
    "i can't summarize",
    "cannot provide a summary",
    "can't provide a summary",
    "insufficient information",
    "not enough information",
    "no content provided",
    "no email content",
)
SUMMARY_NOISE_MARKERS = (
    "is this email difficult to read",
    "view in browser",
    "open in browser",
    "read online",
    "manage preferences",
    "manage email preferences",
    "email preferences",
    "manage subscription",
    "unsubscribe",
    "privacy policy",
    "privacy notice",
    "cookie notice",
    "terms of service",
    "terms of use",
    "subscriber id",
    "subscription id",
    "all rights reserved",
    "you are receiving this email",
    "this email was sent to",
    "add us to your address book",
)
SUMMARY_HALLUCINATION_MARKERS = (
    "if you're not subscribed",
    "if you are not subscribed",
    "sign up here",
    "subscribe here",
    "subscribe now",
    "click here to subscribe",
)
SUMMARY_STOPWORDS = {
    "the",
    "and",
    "for",
    "that",
    "with",
    "this",
    "from",
    "your",
    "you",
    "are",
    "was",
    "were",
    "been",
    "have",
    "has",
    "had",
    "into",
    "about",
    "their",
    "there",
    "will",
    "would",
    "could",
    "should",
    "they",
    "them",
    "then",
    "than",
    "also",
    "just",
    "more",
    "most",
    "very",
    "what",
    "when",
    "where",
    "which",
    "while",
    "after",
    "before",
    "over",
    "under",
    "between",
    "because",
    "through",
    "these",
    "those",
    "within",
    "without",
    "about",
    "into",
    "onto",
    "upon",
}
FOOTER_NOISE_REGEX_PATTERNS = (
    r"\bmanage\s+(?:email\s+)?preferences\b",
    r"\bemail\s+preferences\b",
    r"\bmanage\s+subscription\b",
    r"\bunsubscribe\b",
    r"\bprivacy\s+(?:policy|notice)\b",
    r"\bcookie\s+notice\b",
    r"\bterms\s+of\s+(?:service|use)\b",
    r"\bview\s+in\s+browser\b",
    r"\bopen\s+in\s+browser\b",
    r"\bread\s+online\b",
    r"\byou\s+are\s+receiving\s+this\s+email\b",
    r"\bthis\s+email\s+was\s+sent\s+to\b",
    r"\badd\s+us\s+to\s+your\s+address\s+book\b",
    r"\ball\s+rights\s+reserved\b",
    r"\b(?:subscriber|subscription|member|customer)\s*id\s*[:#]?\s*[a-z0-9\-]{5,}\b",
)
DRAFT_FAILURE_MARKERS = (
    "as an ai",
    "unable to draft",
    "unable to write",
    "cannot draft",
    "can't draft",
    "insufficient information",
    "not enough information",
)
REQUEST_SENTENCE_MARKERS = (
    "please reply",
    "please confirm",
    "can you",
    "could you",
    "would you",
    "let me know",
    "respond by",
    "action required",
    "rsvp",
    "approval needed",
    "deadline",
    "asap",
)
EMAIL_ADDRESS_PATTERN = re.compile(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})")
SUMMARY_MAX_CHARS = 720
DRAFT_MIN_CHARS = 20
DRAFT_MIN_ACTIONABLE_CHARS = 60


def _utc_now():
    """Utc now.
    """
    # Used by other functions in this file.
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _action_log_path():
    """Action log path.
    """
    # Write a structured log entry so this step is easy to trace later.
    configured = (os.getenv("AI_ACTION_LOG_PATH") or "").strip()
    if configured:
        return Path(configured)
    return Path("instance/ai_actions.txt")


def _one_line(value):
    """One line.
    """
    # Used by other functions in this file.
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return text[:500]


def _log_action(task, status, email_id=None, detail=""):
    """Log action.
    """
    try:
        log_event(
            action_type="ai_task",
            action=task,
            status=status,
            level="ERROR" if str(status).lower() == "error" else "INFO",
            component="ollama_client",
            details=detail,
            email_id=email_id if email_id is not None else "",
        )
    except Exception:
        pass

    line = (
        f"{_utc_now()}\ttask={task}\tstatus={status}"
        f"\temail_id={email_id if email_id is not None else '-'}"
        f"\tdetail={_one_line(detail)}"
    )
    try:
        target = _action_log_path()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")
    except OSError:
        # Avoid breaking request paths if logging fails.
        pass


def log_ai_event(task, status, email_id=None, detail=""):
    """Log AI event.
    """
    # Write a structured log entry so this step is easy to trace later.
    _log_action(task=task, status=status, email_id=email_id, detail=detail)


def _api_url():
    """Api url.
    """
    # Resolve api url with configured values and a safe fallback.
    value = (os.getenv("OLLAMA_API_URL") or OLLAMA_API_URL_DEFAULT).strip()
    return value or OLLAMA_API_URL_DEFAULT


def _model_name():
    """Model name.
    """
    # Used by other functions in this file.
    value = (os.getenv("OLLAMA_MODEL") or OLLAMA_MODEL_DEFAULT).strip()
    return value or OLLAMA_MODEL_DEFAULT


def _timeout_seconds():
    """Timeout seconds.
    """
    # Used by other functions in this file.
    raw = (os.getenv("OLLAMA_TIMEOUT_SECONDS") or "").strip()
    try:
        parsed = float(raw) if raw else float(OLLAMA_TIMEOUT_SECONDS_DEFAULT)
    except ValueError:
        parsed = float(OLLAMA_TIMEOUT_SECONDS_DEFAULT)
    return max(1.0, min(60.0, parsed))


def _summary_min_chars():
    """Summary min chars.
    """
    # Used by other functions in this file.
    raw = (os.getenv("OLLAMA_SUMMARY_MIN_CHARS") or "").strip()
    try:
        parsed = int(raw) if raw else SUMMARY_MIN_CHARS_DEFAULT
    except ValueError:
        parsed = SUMMARY_MIN_CHARS_DEFAULT
    return max(50, min(5000, parsed))


def _is_loopback_host(hostname):
    """Return whether loopback host.
    """
    # Used by other functions in this file.
    if not hostname:
        return False
    lowered = hostname.lower()
    if lowered in LOCALHOST_NAMES:
        return True
    try:
        return ipaddress.ip_address(lowered).is_loopback
    except ValueError:
        return False


def _endpoint_allowed():
    """Endpoint allowed.
    """
    # Used by other functions in this file.
    parsed = urlparse(_api_url())
    if parsed.scheme != "http":
        return False
    return _is_loopback_host(parsed.hostname)


def ai_enabled():
    """Ai enabled.
    """
    # Used by other functions in this file.
    return _endpoint_allowed()


def should_summarize_email(email_data):
    """Return whether summarize email.
    """
    # Return whether summarize email should run based on current message/context state.
    body = (email_data.get("body") or "").strip()
    return len(body) >= _summary_min_chars()


def classification_to_email_type(classification):
    """Classification recipient email type.
    """
    # Normalize to the fixed labels used by mailbox triage.
    if not isinstance(classification, dict):
        return "read-only"
    explicit_type = str(classification.get("email_type") or "").strip().lower()
    if explicit_type in VALID_EMAIL_TYPES:
        return explicit_type
    category = str(classification.get("category") or "").strip().lower()
    needs_response = bool(classification.get("needs_response"))
    if category == "junk":
        try:
            confidence = float(classification.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
            return "junk-uncertain"
        return "junk"
    if needs_response:
        return "response-needed"
    return "read-only"


def _compact_text(value):
    """Compact text.
    """
    # Used by other functions in this file.
    return " ".join(str(value or "").split()).strip()


def _ensure_sentence_ending(text):
    """Return compacted text with terminal punctuation."""
    # Keep generated copy readable by avoiding duplicate punctuation patterns.
    value = _compact_text(text)
    if not value:
        return ""
    if value[-1] in ".!?":
        return value
    return f"{value}."


def _strip_footer_noise_text(text):
    """Remove common newsletter/legal footer phrases from text."""
    # Keep prompts and summaries focused on actionable content rather than boilerplate.
    cleaned = str(text or "")
    if not cleaned:
        return ""
    for pattern in FOOTER_NOISE_REGEX_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"(?:\(\s*\[link\]\s*\)\s*){2,}",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"(?:\(?\s*\[link\]\s*\)?\s*[|,;/:-]?\s*){2,}",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\[\s*link\s*\](?:\s*[|,;])?\s*\[\s*link\s*\]",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\[\s*link\s*\]", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*[|]+\s*", " ", cleaned)
    cleaned = re.sub(r"\.{2,}", ".", cleaned)
    cleaned = re.sub(r"\?{2,}", "?", cleaned)
    cleaned = re.sub(r"!{2,}", "!", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = cleaned.strip(" -|,;:")
    if _looks_source_signature_sentence(cleaned):
        return ""
    return cleaned


def _looks_footer_noise_fragment(text):
    """Return True when text appears to be newsletter/footer boilerplate."""
    # Keep this rule in one place so behavior stays consistent.
    normalized = _compact_text(text).lower()
    if not normalized:
        return False
    for pattern in FOOTER_NOISE_REGEX_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            return True
    if normalized.count("[link]") >= 2 and len(_text_tokens(normalized)) <= 14:
        return True
    return False


def _looks_source_signature_sentence(text):
    """Return True when text looks like a publisher/source signature line."""
    # Keep summaries focused by dropping short masthead lines (e.g., "<Publisher> Online").
    normalized = _compact_text(text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if "[link]" in lowered:
        return False
    if any(marker in lowered for marker in REQUEST_SENTENCE_MARKERS):
        return False

    tokens = _text_tokens(lowered)
    if len(tokens) < 3 or len(tokens) > 10:
        return False
    common_verbs = {
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "can",
        "could",
        "will",
        "would",
        "should",
        "must",
        "need",
        "needs",
        "join",
        "reply",
        "confirm",
        "review",
        "update",
    }
    has_likely_verb = any(
        token in common_verbs or token.endswith("ed") or token.endswith("ing")
        for token in tokens
    )
    if has_likely_verb:
        return False
    return tokens[-1] in {"online", "newsletter", "digest"}


def _text_tokens(value):
    """Text tokens.
    """
    # Used by other functions in this file.
    text = str(value or "").lower()
    return [token for token in re.findall(r"[a-z0-9']+", text) if len(token) > 2]


def _token_overlap_ratio(left, right):
    """Token overlap ratio.
    """
    # Used by other functions in this file.
    left_tokens = set(_text_tokens(left))
    right_tokens = set(_text_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / float(min(len(left_tokens), len(right_tokens)))


def _is_noise_fragment(text):
    """Return whether noise fragment.
    """
    # Used by other functions in this file.
    normalized = _compact_text(text).lower()
    if not normalized or normalized == "[link]":
        return True
    if _looks_footer_noise_fragment(normalized):
        return True
    if _looks_source_signature_sentence(normalized):
        return True
    return any(marker in normalized for marker in SUMMARY_NOISE_MARKERS)


def _is_near_subject_copy(candidate_text, title_text):
    """Return whether near subject copy.
    """
    # Used by other functions in this file.
    candidate = _compact_text(candidate_text)
    title = _compact_text(title_text)
    if not candidate or not title or title.lower() == "(no subject)":
        return False
    lowered_candidate = candidate.lower()
    lowered_title = title.lower()
    if lowered_candidate == lowered_title:
        return True
    overlap = _token_overlap_ratio(lowered_candidate, lowered_title)
    if overlap >= 0.92:
        return True
    if lowered_candidate.startswith(lowered_title):
        remainder = lowered_candidate[len(lowered_title):].strip(" .:-")
        if len(remainder) < 36:
            return True
    return False


def _strip_reply_chain(text):
    """Strip reply chain.
    """
    content = str(text or "")
    cut_positions = []
    # Trim quoted history so prompts focus on the newest message only.
    for pattern in REPLY_CHAIN_PATTERNS:
        match = re.search(
            pattern,
            content,
            flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
        )
        if match:
            cut_positions.append(match.start())
    if not cut_positions:
        return content
    return content[: min(cut_positions)]


def _clean_body_for_prompt(body, max_chars=8000):
    """Clean body for prompt.
    """
    # Clean this value so the rest of the code gets predictable input.
    text = str(body or "").replace("\r\n", "\n").replace("\r", "\n")
    text = _strip_reply_chain(text)
    cleaned_lines = []
    for line in text.split("\n"):
        trimmed = line.strip()
        if not trimmed:
            # Keep paragraph boundaries for downstream sentence splitting.
            cleaned_lines.append("")
            continue
        if trimmed.startswith(">"):
            # Ignore quoted history from earlier messages in a thread.
            continue
        normalized_line = re.sub(r"https?://\S+", "[link]", trimmed)
        normalized_line = _strip_footer_noise_text(normalized_line)
        normalized_line = _compact_text(normalized_line)
        if not normalized_line:
            # Drop lines that collapse to empty after noise stripping.
            continue
        cleaned_lines.append(normalized_line)

    cleaned = "\n".join(cleaned_lines)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()[:max_chars]


def _extract_key_sentences(body_text, max_sentences=8):
    """Extract key sentences.
    """
    # Read this field from payloads that may be missing keys.
    cleaned_body = _clean_body_for_prompt(body_text or "", max_chars=5000)
    if not cleaned_body:
        return []

    flattened = _compact_text(cleaned_body)
    if not flattened:
        return []
    flattened = re.sub(r"\s*[|*]\s*", ". ", flattened)
    flattened = re.sub(r"\s*[-]{2,}\s*", ". ", flattened)

    parts = re.split(r"(?<=[.!?])\s+", flattened)
    if len(parts) <= 1:
        # Fallback splitter for emails with weak punctuation structure.
        parts = re.split(r";\s+|\.\s+", flattened)

    selected = []
    for part in parts:
        sentence = _compact_text(part).strip(" -:")
        sentence = _strip_footer_noise_text(sentence)
        if len(sentence) < 24:
            continue
        for marker in SUMMARY_NOISE_MARKERS:
            sentence = re.sub(re.escape(marker), " ", sentence, flags=re.IGNORECASE)
        sentence = _compact_text(sentence).strip(" -:")
        if len(sentence) < 24:
            continue
        lowered = sentence.lower()
        if lowered.startswith(("from:", "to:", "cc:", "bcc:", "sent:", "date:", "subject:")):
            # Skip copied header metadata that is not semantic body content.
            continue
        if _looks_source_signature_sentence(sentence):
            continue
        if _is_noise_fragment(sentence):
            continue
        if any(_token_overlap_ratio(sentence, existing) > 0.94 for existing in selected):
            # Avoid retaining near-duplicate lines in extracted candidates.
            continue
        selected.append(sentence)
        if len(selected) >= max_sentences:
            break
    return selected


def _body_for_context(email_data, max_chars=8000):
    """Body for context.
    """
    # Build body for context text that is passed into model prompts.
    key_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=14)
    if key_sentences:
        text = " ".join(key_sentences)
    else:
        text = _clean_body_for_prompt(email_data.get("body") or "", max_chars=max_chars)
        for marker in SUMMARY_NOISE_MARKERS:
            text = re.sub(re.escape(marker), " ", text, flags=re.IGNORECASE)
    text = _strip_footer_noise_text(text)
    text = _compact_text(text)
    if len(text) > max_chars:
        return f"{text[: max_chars - 3].rstrip()}..."
    return text


def _looks_summary_failure(summary_text):
    """Looks summary failure.
    """
    # Keep this rule in one place so behavior stays consistent.
    normalized = _compact_text(summary_text).lower()
    if not normalized:
        return True
    if normalized in {"n/a", "none", "unknown", "summary unavailable"}:
        return True
    if normalized.startswith("{") or normalized.startswith("["):
        return True
    if _is_noise_fragment(normalized):
        return True
    return any(marker in normalized for marker in SUMMARY_FAILURE_MARKERS)


def _content_tokens(value):
    """Return content-heavy tokens for grounding checks."""
    # Used by other functions in this file.
    tokens = _text_tokens(value)
    return [token for token in tokens if token not in SUMMARY_STOPWORDS]


def _summary_support_ratio(sentence_text, reference_tokens):
    """Return how strongly sentence tokens are grounded in the source."""
    # Keep hallucination filtering deterministic and cheap.
    tokens = _content_tokens(sentence_text)
    if not tokens:
        return 0.0
    if not reference_tokens:
        return 0.0
    supported = sum(1 for token in tokens if token in reference_tokens)
    return supported / float(len(tokens))


def _sanitize_model_summary(summary_text, email_data):
    """Normalize and ground model summary text against source content."""
    # Keep only concise, source-supported sentences from model output.
    summary = _compact_text(summary_text)
    if not summary:
        return None
    summary = re.sub(r"\?", ".", summary)
    summary = re.sub(r"\bkey details\s*:\s*", "", summary, flags=re.IGNORECASE)
    summary = re.sub(r"\baction for you\s*:\s*", "", summary, flags=re.IGNORECASE)
    summary = re.sub(r"\bsummary\s*:\s*", "", summary, flags=re.IGNORECASE)
    summary = re.sub(r"\s{2,}", " ", summary).strip()
    if not summary:
        return None

    title = _compact_text(email_data.get("title") or "")
    source = _compact_text(
        " ".join(
            part
            for part in [
                title,
                _body_for_context(email_data, max_chars=3600),
            ]
            if part
        )
    )
    source_tokens = set(_content_tokens(source))

    parts = [
        _compact_text(fragment).strip(" -:")
        for fragment in re.split(r"(?<=[.!?])\s+", summary)
    ]
    parts = [fragment for fragment in parts if fragment]
    if not parts:
        parts = [summary]

    kept = []
    for sentence in parts:
        cleaned = _compact_text(_strip_footer_noise_text(sentence))
        if len(cleaned) < 22:
            continue
        lowered = cleaned.lower()
        if any(marker in lowered for marker in SUMMARY_HALLUCINATION_MARKERS):
            continue
        if _is_noise_fragment(cleaned):
            continue
        if any(_token_overlap_ratio(cleaned, existing) > 0.93 for existing in kept):
            continue
        support_ratio = _summary_support_ratio(cleaned, source_tokens)
        if support_ratio < 0.52 and not _is_near_subject_copy(cleaned, title):
            continue
        kept.append(cleaned)
        if len(kept) >= 3:
            break

    if not kept:
        return None

    normalized = " ".join(_ensure_sentence_ending(sentence) for sentence in kept)
    normalized = _compact_text(normalized)
    if len(normalized) > SUMMARY_MAX_CHARS:
        normalized = f"{normalized[: SUMMARY_MAX_CHARS - 3]}..."
    return normalized or None


def _looks_summary_parrot(summary_text, email_data):
    """Return True when summary appears copied from the email body."""
    # Keep model summaries readable by rejecting near-verbatim source echoes.
    summary = _compact_text(summary_text)
    if len(summary) < 60:
        return False

    source = _compact_text(_body_for_context(email_data, max_chars=3600))
    if not source:
        return False

    summary_lower = summary.lower()
    source_lower = source.lower()
    if len(summary) >= 80 and summary_lower in source_lower:
        return True

    summary_sentences = [
        _compact_text(part).strip(" -:")
        for part in re.split(r"(?<=[.!?])\s+", summary)
    ]
    summary_sentences = [part for part in summary_sentences if len(part) >= 24]
    if not summary_sentences:
        summary_sentences = [summary]

    source_sentences = _extract_key_sentences(source, max_sentences=20)
    if not source_sentences:
        source_sentences = [
            _compact_text(part).strip(" -:")
            for part in re.split(r"(?<=[.!?])\s+", source)
            if _compact_text(part)
        ]

    copied_sentences = 0
    for sentence in summary_sentences:
        sentence_lower = sentence.lower()
        if len(sentence) >= 36 and sentence_lower in source_lower:
            copied_sentences += 1
            continue
        if any(
            _token_overlap_ratio(sentence, source_sentence) >= 0.96
            and abs(len(sentence) - len(source_sentence)) <= 40
            for source_sentence in source_sentences
        ):
            copied_sentences += 1

    required_copied = max(1, (len(summary_sentences) + 1) // 2)
    return copied_sentences >= required_copied


def _rewrite_parroted_summary(summary_text, email_data, email_id=None):
    """Attempt to rewrite a copied summary into original condensed wording."""
    # Give the model one more chance to paraphrase before falling back.
    candidate = _compact_text(summary_text)
    if not candidate:
        return None

    system_prompt = (
        "You rewrite copied email summaries into original condensed wording. "
        "Address the mailbox owner directly using you/your. "
        "Use only explicit facts present in the email context. "
        "Do not copy or quote source sentences. "
        "Do not add subscription/sign-up suggestions unless explicitly requested in the email. "
        "Use declarative statements only (no questions). "
        "Return one compact plain-text paragraph, 2-4 sentences."
    )
    user_prompt = (
        "Rewrite this summary in your own words so it is not copied from the email.\n\n"
        f"Current summary:\n{candidate}\n\n"
        f"{_email_context_block(email_data)}"
    )
    response_text = _call_ollama(
        task="summarize_rewrite",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=280,
    )
    if not response_text:
        return None

    rewritten = _rewrite_summary_for_second_person(
        _sanitize_model_summary(response_text, email_data) or ""
    )
    if _looks_summary_failure(rewritten):
        return None
    if _looks_summary_parrot(rewritten, email_data):
        return None
    if len(rewritten) > SUMMARY_MAX_CHARS:
        rewritten = f"{rewritten[: SUMMARY_MAX_CHARS - 3]}..."
    return rewritten or None


def _extractive_summary_fallback(email_data):
    """Contextual summary fallback.
    """
    # Keep fallback summaries useful without copying long spans from the body.
    title = _compact_text(email_data.get("title") or "")
    sender_name = _sender_display_name(email_data.get("sender")) or "the sender"
    actionable = _looks_actionable(email_data)
    newsletter_like = _looks_bulk_or_newsletter(email_data)

    intro = (
        f"This email from {sender_name} is about {title}."
        if title and title.lower() != "(no subject)"
        else f"This email is from {sender_name}."
    )

    if actionable:
        posture = "It appears to include a request that needs your response."
        action = "Review the message and reply with the requested details."
    elif newsletter_like:
        posture = "It looks like an informational update or newsletter."
        action = "No direct reply appears to be required."
    else:
        posture = "It appears to be an informational message."
        action = "No explicit action request is obvious from the content."

    summary = _compact_text(f"{intro} {posture} {action}")
    if _is_noise_fragment(summary):
        if title and title.lower() != "(no subject)":
            return f"You received an email about {title}."
        return None
    if len(summary) > SUMMARY_MAX_CHARS:
        summary = f"{summary[: SUMMARY_MAX_CHARS - 3]}..."
    return summary or None


def _rewrite_summary_for_second_person(summary_text):
    """Normalize generated summaries to second-person wording."""
    # Keep summary style predictable so the UI addresses the mailbox owner directly.
    summary = str(summary_text or "")
    if not summary:
        return ""
    replacements = (
        (r"\bthe user\b", "you"),
        (r"\bthis user\b", "you"),
        (r"\buser's\b", "your"),
        (r"\bthe recipient\b", "you"),
        (r"\brecipient's\b", "your"),
        (r"\bthe email recipient\b", "you"),
        (r"\bmailbox owner\b", "you"),
    )
    for pattern, replacement in replacements:
        summary = re.sub(pattern, replacement, summary, flags=re.IGNORECASE)
    return _compact_text(summary)


def _uses_second_person(text):
    """Return True when text explicitly uses second-person wording."""
    return bool(re.search(r"\b(you|your)\b", str(text or "").lower()))


def _normalized_email_for_classification(email_data):
    """Normalized email for classification.
    """
    # Normalize to the fixed labels used by mailbox triage.
    return {
        "title": _compact_text(email_data.get("title") or "(No subject)"),
        "sender": _compact_text(email_data.get("sender")),
        "recipients": _compact_text(email_data.get("recipients")),
        "cc": _compact_text(email_data.get("cc")),
        "body": _clean_body_for_prompt(email_data.get("body") or ""),
    }


def _email_context_block(email_data):
    """Email context block.
    """
    # Build email context block text that is passed into model prompts.
    title = _compact_text(email_data.get("title") or "(No subject)")
    sender = _compact_text(email_data.get("sender"))
    recipients = _compact_text(email_data.get("recipients"))
    cc = _compact_text(email_data.get("cc"))
    body = _body_for_context(email_data, max_chars=8000)
    ai_category = _compact_text(email_data.get("ai_category"))
    ai_type = _compact_text(email_data.get("type"))
    ai_hint = ""
    if ai_category or ai_type:
        ai_hint = (
            "\nAI triage context:\n"
            f"- category: {ai_category or '(unknown)'}\n"
            f"- mailbox type: {ai_type or '(unknown)'}"
        )
    return (
        f"Subject: {title}\n"
        f"From: {sender}\n"
        f"To: {recipients}\n"
        f"Cc: {cc}\n"
        f"Body:\n{body}{ai_hint}"
    )


def _sender_parts(sender_text):
    """Sender parts.
    """
    # Split sender text into structured parts used by heuristic classification logic.
    raw = _compact_text(sender_text).lower()
    if not raw:
        return {
            "raw": "",
            "email": "",
            "local": "",
            "domain": "",
            "display": "",
            "identity": "",
        }

    email = ""
    email_match = EMAIL_ADDRESS_PATTERN.search(raw)
    if email_match:
        email = email_match.group(1)
    else:
        bracket_match = re.search(r"<([^>]+)>", raw)
        if bracket_match:
            bracket_value = _compact_text(bracket_match.group(1)).lower()
            bracket_email_match = EMAIL_ADDRESS_PATTERN.search(bracket_value)
            if bracket_email_match:
                email = bracket_email_match.group(1)

    local = ""
    domain = ""
    if "@" in email:
        local, domain = email.split("@", 1)

    display = _compact_text(re.sub(r"<[^>]*>", " ", raw))
    if email and display:
        display = _compact_text(display.replace(email, " "))

    identity = _compact_text(
        " ".join(part for part in [raw, email, local, domain, display] if part)
    )
    return {
        "raw": raw,
        "email": email,
        "local": local,
        "domain": domain,
        "display": display,
        "identity": identity,
    }


def _sender_address(sender_text):
    """Sender address.
    """
    # Used by other functions in this file.
    return _sender_parts(sender_text).get("email", "")


def _has_any_pattern(text, patterns):
    """Return whether any pattern.
    """
    # Check whether any pattern exists before running heavier work.
    value = str(text or "").lower()
    return any(pattern in value for pattern in patterns)


def _sender_looks_automated(sender_info):
    """Sender looks automated.
    """
    # Used by other functions in this file.
    if not isinstance(sender_info, dict):
        return False
    identity = sender_info.get("identity", "")
    return _has_any_pattern(identity, AUTOMATED_SENDER_MARKERS)


def _sender_uses_personal_domain(sender_info):
    """Sender uses personal domain.
    """
    # Used by other functions in this file.
    if not isinstance(sender_info, dict):
        return False
    domain = str(sender_info.get("domain") or "").strip().lower()
    if not domain:
        return False
    return domain in PERSONAL_EMAIL_DOMAINS


def _sender_hint_block(email_data):
    """Sender hint block.
    """
    # Used by other functions in this file.
    sender_info = _sender_parts(email_data.get("sender"))
    sender_automated = _sender_looks_automated(sender_info)
    sender_personal_domain = _sender_uses_personal_domain(sender_info)
    return (
        "Sender signal:\n"
        f"- sender_email: {sender_info.get('email') or '(unknown)'}\n"
        f"- sender_domain: {sender_info.get('domain') or '(unknown)'}\n"
        f"- sender_local_part: {sender_info.get('local') or '(unknown)'}\n"
        f"- sender_looks_automated: {'yes' if sender_automated else 'no'}\n"
        f"- sender_uses_personal_domain: {'yes' if sender_personal_domain else 'no'}\n"
    )


def _classification_few_shot_block():
    """Classification few shot block.
    """
    # Normalize to the fixed labels used by mailbox triage.
    return (
        "Few-shot examples:\n"
        "1) From: promotions@store-updates.example\n"
        "Subject: Exclusive member savings this week\n"
        "Body: Limited-time offer. Click to shop now. Unsubscribe here.\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.90}\n\n'
        "2) From: dailybrief@news-digest.example\n"
        "Subject: Morning briefing: top stories\n"
        "Body: Today's headlines and links. Manage preferences.\n"
        'Output: {"category":"informational","needs_response":false,"priority":1,"confidence":0.92}\n\n'
        "3) From: manager@team.example\n"
        "Subject: Can you confirm by 3 PM?\n"
        "Body: Please reply with approval before the deadline.\n"
        'Output: {"category":"urgent","needs_response":true,"priority":3,"confidence":0.91}\n\n'
        "4) From: billing@service-notify.example\n"
        "Subject: Your monthly statement is ready\n"
        "Body: View your statement online. No action required unless there is an issue.\n"
        'Output: {"category":"informational","needs_response":false,"priority":1,"confidence":0.84}'
    )


def _looks_bulk_or_newsletter(email_data):
    """Looks bulk or newsletter.
    """
    # Keep this rule in one place so behavior stays consistent.
    sender = _sender_address(email_data.get("sender"))
    body = str(email_data.get("body") or "").lower()
    title = str(email_data.get("title") or "").lower()
    combined = " ".join([sender, title, body])

    sender_markers = (
        "no-reply",
        "noreply",
        "donotreply",
        "newsletter",
        "digest",
        "updates",
        "notification",
        "announcements",
        "marketing",
    )
    content_markers = (
        "unsubscribe",
        "manage preferences",
        "view in browser",
        "read more",
        "latest news",
        "top stories",
        "today in",
        "latest in",
        "what's news",
        "morning brew",
        "quora digest",
        "daily briefing",
        "weekly briefing",
        "daily digest",
        "weekly digest",
        "breaking news",
        "manage subscription",
        "sponsored",
    )
    return _has_any_pattern(combined, sender_markers) or _has_any_pattern(
        combined, content_markers
    )


def _looks_actionable(email_data):
    """Looks actionable.
    """
    title = str(email_data.get("title") or "").lower()
    body = str(email_data.get("body") or "").lower()
    combined = " ".join([title, body])

    explicit_question = "?" in title or "?" in body
    direct_question_markers = (
        "can you",
        "could you",
        "would you",
        "will you",
        "please",
        "let me know",
        "confirm",
    )
    response_markers = (
        "please reply",
        "let me know",
        "can you",
        "could you",
        "please confirm",
        "needs your response",
        "respond by",
        "action required",
        "rsvp",
        "approval needed",
        "deadline",
        "asap",
    )
    # Bulk messages are treated as non-actionable unless they contain explicit response language.
    if _looks_bulk_or_newsletter(email_data) and not _has_any_pattern(
        combined, response_markers
    ):
        return False
    if explicit_question and _has_any_pattern(combined, direct_question_markers):
        return True
    return _has_any_pattern(combined, response_markers)


def _looks_probable_junk(email_data):
    """Looks probable junk.
    """
    # Keep this rule in one place so behavior stays consistent.
    sender = _sender_address(email_data.get("sender"))
    title = str(email_data.get("title") or "").lower()
    body = str(email_data.get("body") or "").lower()
    combined = " ".join([sender, title, body])

    strong_markers = (
        "viagra",
        "crypto giveaway",
        "guaranteed income",
        "claim your prize",
        "winner selected",
        "casino bonus",
        "adult",
        "xxx",
        "lottery",
    )
    soft_markers = (
        "limited time offer",
        "buy now",
        "exclusive deal",
        "act now",
        "special promotion",
        "free trial",
    )

    if _has_any_pattern(combined, strong_markers):
        return "strong"
    if _has_any_pattern(combined, soft_markers):
        return "soft"
    return None


def _heuristic_classification(email_data):
    """Heuristic classification.
    """
    # Normalize to the fixed labels used by mailbox triage.
    actionable = _looks_actionable(email_data)
    bulk_signal = _looks_bulk_or_newsletter(email_data)
    junk_signal = _looks_probable_junk(email_data)
    if junk_signal == "strong":
        return {
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.92,
            "email_type": "junk",
        }

    if bulk_signal and not actionable:
        return {
            "category": "informational",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.88,
            "email_type": "read-only",
        }

    if junk_signal == "soft":
        return {
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.62,
            "email_type": "junk-uncertain",
        }

    if actionable:
        return {
            "category": "urgent",
            "needs_response": True,
            "priority": 2,
            "confidence": 0.7,
            "email_type": "response-needed",
        }

    return {
        "category": "informational",
        "needs_response": False,
        "priority": 1,
        "confidence": 0.55,
        "email_type": "read-only",
    }


def _merge_with_heuristics(model_classification, heuristic_classification):
    """Merge with heuristics.
    """
    if not model_classification:
        return dict(heuristic_classification)

    merged = dict(model_classification)
    try:
        model_confidence = float(merged.get("confidence") or 0.0)
    except (TypeError, ValueError):
        model_confidence = 0.0
    heuristic_type = heuristic_classification.get("email_type")

    if heuristic_type == "read-only":
        if bool(merged.get("needs_response")) and model_confidence < 0.9:
            merged["category"] = "informational"
            merged["needs_response"] = False
            merged["priority"] = min(int(merged.get("priority") or 1), 1)
            merged["email_type"] = "read-only"

    if heuristic_type in {"junk", "junk-uncertain"}:
        model_is_junk = str(merged.get("category") or "").strip().lower() == "junk"
        # If heuristics are strongly junk-like and the model is uncertain, trust heuristics.
        if not model_is_junk and model_confidence < 0.85:
            merged = dict(heuristic_classification)

    if str(merged.get("category") or "").strip().lower() == "junk":
        if model_confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
            merged["email_type"] = "junk-uncertain"
        else:
            merged["email_type"] = "junk"

    return merged


def _extract_json_block(text):
    """Extract JSON block.
    """
    # Read this field from payloads that may be missing keys.
    stripped = str(text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    fenced_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", stripped)
    if fenced_match:
        return fenced_match.group(1).strip()
    match = re.search(r"\{[\s\S]*\}", stripped)
    return match.group(0).strip() if match else None


def _parse_bool(value):
    """Parse bool.
    """
    # Parse and validate this input before using it.
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    return False


def _normalize_classification(raw_value):
    """Normalize classification.
    """
    # Normalize classification to one format used across the app.
    category = str(raw_value.get("category") or "").strip().lower()
    try:
        priority = int(raw_value.get("priority"))
    except (TypeError, ValueError):
        priority = 1
    priority = max(1, min(3, priority))

    try:
        confidence = float(raw_value.get("confidence"))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    needs_response = _parse_bool(raw_value.get("needs_response"))
    email_type = str(raw_value.get("email_type") or "").strip().lower()
    if email_type not in VALID_EMAIL_TYPES:
        email_type = ""

    if category not in VALID_CATEGORIES:
        if needs_response or priority >= 3:
            category = "urgent"
        else:
            category = "informational"

    if email_type == "response-needed":
        needs_response = True
    elif email_type == "read-only":
        needs_response = False
        if category == "junk":
            category = "informational"
    elif email_type in {"junk", "junk-uncertain"}:
        category = "junk"
        needs_response = False
        if priority > 1:
            priority = 1

    if category == "junk":
        needs_response = False
        if priority > 1:
            priority = 1
        if not email_type:
            if confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
                email_type = "junk-uncertain"
            else:
                email_type = "junk"

    normalized = {
        "category": category,
        "needs_response": bool(needs_response),
        "priority": priority,
        "confidence": confidence,
    }
    if email_type:
        normalized["email_type"] = email_type
    return normalized


def _call_ollama(task, messages, email_id=None, temperature=0.1, num_predict=320):
    """Call ollama.
    """
    # Send one non-streaming chat request to Ollama and return the model text payload.
    _log_action(task=task, status="call_start", email_id=email_id, detail="ollama_chat")

    if not _endpoint_allowed():
        _log_action(
            task=task,
            status="error",
            email_id=email_id,
            detail=f"Blocked non-local Ollama endpoint: {_api_url()}",
        )
        return None

    payload = {
        "model": _model_name(),
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
        },
    }
    body = json.dumps(payload).encode("utf-8")
    request_obj = urllib.request.Request(
        _api_url(),
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request_obj, timeout=_timeout_seconds()) as response:
            raw = response.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
        _log_action(task=task, status="error", email_id=email_id, detail=f"request_failed: {exc}")
        return None

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        _log_action(task=task, status="error", email_id=email_id, detail=f"json_decode_failed: {exc}")
        return None

    content = ((parsed.get("message") or {}).get("content") or "").strip()
    if not content:
        _log_action(task=task, status="error", email_id=email_id, detail="empty_response_content")
        return None

    _log_action(
        task=task,
        status="call_success",
        email_id=email_id,
        detail=f"chars={len(content)}",
    )
    return content


def classify_email(email_data, email_id=None):
    """Classify email.
    """
    # Normalize to the fixed labels used by mailbox triage.
    normalized_email = _normalized_email_for_classification(email_data)
    body = (normalized_email.get("body") or "").strip()
    title = (normalized_email.get("title") or "").strip()
    if not body and not title:
        return None

    heuristic = _heuristic_classification(normalized_email)

    system_prompt = (
        "You classify emails for triage. Think step-by-step internally, but never output your chain-of-thought. "
        "Return valid JSON only with exactly these keys: "
        "category, needs_response, priority, confidence. "
        "category must be one of: urgent, informational, junk. "
        "needs_response must be true or false. "
        "priority must be an integer 1 to 3. "
        "confidence must be a float 0 to 1. "
        "Weighted evidence order: body intent is highest, sender identity/domain is second highest, "
        "subject is third. "
        "If body and sender conflict weakly, give sender extra weight unless body has a direct response request. "
        "Infer whether the sender appears to be a brand/company/news source or an individual from sender "
        "name/domain using your general knowledge and context in this email. "
        "Do not rely on a predefined hardcoded brand list. "
        "Use strict policy: newsletters/digests/promotions/notifications are usually informational "
        "with needs_response=false unless the email explicitly asks the recipient to respond. "
        "Mark junk only for spam/scam/promotional noise; use lower confidence for borderline cases. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Classify this email.\n\n"
        f"{_classification_few_shot_block()}\n\n"
        f"{_sender_hint_block(normalized_email)}\n"
        f"{_email_context_block(normalized_email)}\n\n"
        "Return JSON only."
    )

    response_text = _call_ollama(
        task="classify",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.0,
        num_predict=280,
    )
    if not response_text:
        return heuristic

    json_block = _extract_json_block(response_text)
    if not json_block:
        _log_action(task="classify", status="error", email_id=email_id, detail="missing_json_block")
        return heuristic

    try:
        parsed = json.loads(json_block)
    except json.JSONDecodeError as exc:
        _log_action(task="classify", status="error", email_id=email_id, detail=f"invalid_json: {exc}")
        return heuristic

    normalized = _normalize_classification(parsed)
    merged = _merge_with_heuristics(normalized, heuristic)
    return _normalize_classification(merged)


def summarize_email(email_data, email_id=None):
    """Summarize email.
    """
    # Translate between API payloads and our local mailbox shape.
    if not should_summarize_email(email_data):
        return None
    title = _compact_text(email_data.get("title") or "")
    fallback_summary = _rewrite_summary_for_second_person(
        _extractive_summary_fallback(email_data)
    ) or None

    system_prompt = (
        "You write condensed email summaries for the mailbox owner. "
        "Address the summary in second person using you/your. "
        "Never refer to the mailbox owner as 'the user' or 'the recipient'. "
        "Use only facts explicitly present in the email context; do not infer new details. "
        "Write in your own words: paraphrase and compress instead of copying source sentences. "
        "Do not quote long spans from the email body; only keep exact wording for short facts "
        "like names, dates, times, and amounts when necessary. "
        "Do not add subscription/sign-up suggestions unless explicitly requested in the email. "
        "Use declarative statements only (no questions). "
        "Synthesize themes instead of retelling sentences in source order. "
        "Capture: main topic, concrete details, and any requested action or deadline. "
        "Ignore newsletter/footer boilerplate like subscriber IDs, preference-management links, "
        "privacy/cookie/legal notices, and utility links unless they are the main request. "
        "Return plain text only as one compact paragraph (no markdown or bullet points). "
        "Target 2-4 sentences unless the source email is extremely short. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Generate an original condensed summary in your own words. "
        "Do not copy or closely quote the email body. "
        "Only include claims that are explicitly supported by the email content. "
        "Focus on what happened, key facts, and what you need to do next.\n\n"
        f"{_email_context_block(email_data)}"
    )
    response_text = _call_ollama(
        task="summarize",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.0,
        num_predict=420,
    )
    if not response_text:
        if fallback_summary:
            _log_action(
                task="summarize",
                status="fallback",
                email_id=email_id,
                detail="using_extractive_fallback_no_model_response",
            )
        return fallback_summary

    summary = _rewrite_summary_for_second_person(
        _sanitize_model_summary(response_text, email_data) or ""
    )
    if _looks_summary_failure(summary):
        if fallback_summary:
            _log_action(
                task="summarize",
                status="fallback",
                email_id=email_id,
                detail="using_extractive_fallback_unusable_model_output",
            )
        return fallback_summary
    if _is_near_subject_copy(summary, title):
        if fallback_summary:
            _log_action(
                task="summarize",
                status="fallback",
                email_id=email_id,
                detail="using_extractive_fallback_subject_parrot",
            )
        return fallback_summary
    if _looks_summary_parrot(summary, email_data):
        rewritten = _rewrite_parroted_summary(summary, email_data, email_id=email_id)
        if rewritten and not _is_near_subject_copy(rewritten, title):
            return rewritten
        if fallback_summary:
            _log_action(
                task="summarize",
                status="fallback",
                email_id=email_id,
                detail="using_extractive_fallback_model_body_parrot",
            )
        return fallback_summary
    if not _uses_second_person(summary) and fallback_summary and _uses_second_person(fallback_summary):
        # Keep mailbox-facing voice consistent when model drifts into third person.
        _log_action(
            task="summarize",
            status="fallback",
            email_id=email_id,
            detail="using_extractive_fallback_third_person_model_output",
        )
        return fallback_summary
    if len(summary) > SUMMARY_MAX_CHARS:
        summary = f"{summary[: SUMMARY_MAX_CHARS - 3]}..."
    return summary or fallback_summary


def _sender_display_name(sender_text):
    """Extract a readable sender name from the raw sender field."""
    # Used by other functions in this file.
    sender_raw = _compact_text(sender_text)
    if not sender_raw:
        return ""
    display = sender_raw.split("<", 1)[0].strip().strip('"')
    if "@" in display and " " not in display:
        display = display.split("@", 1)[0]
    display = display.replace(".", " ").replace("_", " ").replace("-", " ")
    return " ".join(display.split())


def _first_request_sentence(email_data):
    """Return the first sentence that looks like an explicit request."""
    # Used by other functions in this file.
    for sentence in _extract_key_sentences(email_data.get("body") or "", max_sentences=8):
        lowered = sentence.lower()
        if "?" in sentence:
            return sentence
        if any(marker in lowered for marker in REQUEST_SENTENCE_MARKERS):
            return sentence
    return None


def _draft_reply_fallback(email_data):
    """Build a safe fallback draft when model output is missing or unusable."""
    # Generate, revise, or validate draft reply fallback used by reply and draft workflows.
    sender_name = _sender_display_name(email_data.get("sender"))
    greeting = f"Hi {sender_name}," if sender_name else "Hi,"

    title = _compact_text(email_data.get("title") or "")
    topic = title if title and title.lower() != "(no subject)" else ""
    if not topic:
        # Recover a topical anchor from body text when subject is missing/generic.
        key_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=1)
        topic = key_sentences[0] if key_sentences else "your message"
    if len(topic) > 110:
        topic = f"{topic[:107].rstrip()}..."

    request_sentence = _first_request_sentence(email_data)
    if request_sentence and len(request_sentence) > 150:
        request_sentence = f"{request_sentence[:147].rstrip()}..."
    key_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=2)
    additional_detail = key_sentences[0] if key_sentences else ""
    if additional_detail and len(additional_detail) > 160:
        additional_detail = f"{additional_detail[:157].rstrip()}..."

    if _looks_actionable(email_data):
        body_parts = [
            f"Thanks for your email about {topic}.",
            "I reviewed the details and understand what you need.",
        ]
        if request_sentence:
            body_parts.append(_ensure_sentence_ending(f"You asked: {request_sentence}"))
        if additional_detail and (
            not request_sentence or _token_overlap_ratio(additional_detail, request_sentence) < 0.85
        ):
            # Add context only when it adds new information beyond the request sentence.
            body_parts.append(
                _ensure_sentence_ending(f"I also noted this context: {additional_detail}")
            )
        body_parts.append(
            "I will follow up shortly with a complete response and concrete next steps."
        )
        body_parts.append(
            "If there is a specific deadline or format you want, please let me know."
        )
        body_text = " ".join(body_parts)
    else:
        body_parts = [
            f"Thanks for sharing the update about {topic}.",
            "I reviewed the information and captured the key points.",
        ]
        if additional_detail:
            body_parts.append(
                _ensure_sentence_ending(f"The main detail I noted is: {additional_detail}")
            )
        body_parts.append("Let me know if you want any follow-up action from my side.")
        body_text = " ".join(body_parts)

    return f"{greeting}\n\n{body_text}\n\nBest regards,"


def _looks_draft_failure(draft_text, email_data):
    """Detect placeholder or low-quality drafts that should be replaced."""
    # Keep this rule in one place so behavior stays consistent.
    cleaned = _compact_text(draft_text)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    if len(cleaned) < DRAFT_MIN_CHARS:
        return True
    if _looks_actionable(email_data) and len(cleaned) < DRAFT_MIN_ACTIONABLE_CHARS:
        return True
    if any(marker in lowered for marker in DRAFT_FAILURE_MARKERS):
        return True
    if _is_noise_fragment(lowered):
        return True

    title = _compact_text(email_data.get("title") or "")
    if _is_near_subject_copy(cleaned, title) and len(cleaned) < 150:
        return True

    source = _body_for_context(email_data, max_chars=900)
    if source:
        source_lower = source.lower()
        if len(cleaned) >= 80 and cleaned.lower() in source_lower:
            return True
        if len(cleaned) >= 100 and _token_overlap_ratio(cleaned, source) > 0.95:
            return True
    return False


def _drafts_too_similar(original_text, revised_text):
    """Return True when revised draft is effectively unchanged."""
    # Keep revise behavior useful by detecting near-identical outputs.
    original = _compact_text(original_text).lower()
    revised = _compact_text(revised_text).lower()
    if not original or not revised:
        return False
    if original == revised:
        return True
    overlap = _token_overlap_ratio(original, revised)
    length_delta = abs(len(original) - len(revised))
    if len(original) <= 120 and overlap >= 0.92 and length_delta <= 20:
        return True
    if len(original) > 120 and overlap >= 0.96 and length_delta <= 40:
        return True
    return False


def draft_reply(email_data, to_value="", cc_value="", email_id=None):
    """Draft reply.
    """
    # Generate, revise, or validate draft reply used by reply and draft workflows.
    body = (email_data.get("body") or "").strip()
    title = _compact_text(email_data.get("title") or "(No subject)")
    if not body and not title:
        return None

    system_prompt = (
        "You write high-quality professional email replies grounded in the incoming email context. "
        "Write as the mailbox owner (use I/we), not as an observer. "
        "Do not refer to the mailbox owner as 'the user'. "
        "Do not copy long spans from the original email. "
        "If the message is actionable, include concrete response details and clear next steps. "
        "If the message is informational/newsletter with no explicit ask, write a concise acknowledgment. "
        "Default to a complete response with greeting, substantive body, and closing sign-off. "
        "Return only the email body text, no markdown and no subject. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Draft a complete response email.\n\n"
        f"{_email_context_block(email_data)}\n\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Keep it clear, specific, and ready to send."
    )
    response_text = _call_ollama(
        task="draft",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.35,
        num_predict=650,
    )
    if not response_text:
        fallback_draft = _draft_reply_fallback(email_data)
        _log_action(
            task="draft",
            status="fallback",
            email_id=email_id,
            detail="using_contextual_fallback_no_model_response",
        )
        return fallback_draft

    cleaned = str(response_text or "").strip()
    if _looks_draft_failure(cleaned, email_data):
        fallback_draft = _draft_reply_fallback(email_data)
        _log_action(
            task="draft",
            status="fallback",
            email_id=email_id,
            detail="using_contextual_fallback_unusable_model_output",
        )
        return fallback_draft
    return cleaned or _draft_reply_fallback(email_data)


def revise_reply(
    email_data,
    current_draft_text,
    to_value="",
    cc_value="",
    email_id=None,
):
    """Revise reply.
    """
    # Generate, revise, or validate revise reply used by reply and draft workflows.
    current_draft_text = str(current_draft_text or "").strip()
    if not current_draft_text:
        return None

    system_prompt = (
        "You revise email drafts using the incoming email context. "
        "Write as the mailbox owner (use I/we), not as an observer. "
        "Never refer to the mailbox owner as 'the user'. "
        "Preserve the original intent, but expand vague or very short drafts into complete replies. "
        "Include concrete details and next steps when the original email asks for action. "
        "Do not copy long spans from the original email. "
        "Return only the revised email body text with greeting and closing, no markdown and no subject. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Revise this draft response based on the original email.\n\n"
        f"{_email_context_block(email_data)}\n\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Current draft (between markers):\n"
        "---BEGIN CURRENT DRAFT---\n"
        f"{current_draft_text}\n"
        "---END CURRENT DRAFT---\n\n"
        "Keep intent, improve clarity, and make it complete enough to send."
    )
    response_text = _call_ollama(
        task="revise",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=650,
    )
    if not response_text:
        # If revision generation fails, attempt a safe fallback before giving up.
        fallback = _draft_reply_fallback(email_data)
        return fallback if fallback and not _drafts_too_similar(current_draft_text, fallback) else current_draft_text
    cleaned = str(response_text or "").strip()
    if _looks_draft_failure(cleaned, email_data) or _drafts_too_similar(current_draft_text, cleaned):
        # Recover from unusable/no-op revisions by forcing a fresh draft generation pass.
        regenerated = draft_reply(
            email_data=email_data,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        regenerated = str(regenerated or "").strip()
        if regenerated and not _drafts_too_similar(current_draft_text, regenerated):
            return regenerated
        fallback = _draft_reply_fallback(email_data)
        if fallback and not _drafts_too_similar(current_draft_text, fallback):
            return fallback
        return current_draft_text
    return cleaned or current_draft_text


def analyze_email(email_data, email_id=None):
    """Analyze email.
    """
    # Backward-compatible adapter for legacy qwen_client API consumers.
    classification = classify_email(
        email_data=email_data,
        email_id=email_id,
    )
    if not classification:
        return None

    try:
        priority = int(classification.get("priority") or 1)
    except (TypeError, ValueError):
        priority = 1
    priority = max(1, min(3, priority))

    summary = summarize_email(
        email_data=email_data,
        email_id=email_id,
    )
    if not summary:
        summary = _extractive_summary_fallback(email_data)
    if not summary:
        summary = _compact_text(email_data.get("summary")) or "No summary generated."

    return {
        "type": classification_to_email_type(classification),
        "priority": priority,
        "summary": summary,
    }


def generate_reply_draft(
    email_data,
    to_value="",
    cc_value="",
    current_draft_text="",
    current_reply_text="",
    email_id=None,
):
    """Generate reply draft.
    """
    # Backward-compatible adapter for legacy qwen_client API consumers.
    # Support both old and new parameter names.
    if not current_draft_text and current_reply_text:
        current_draft_text = current_reply_text
    current_draft_text = str(current_draft_text or "").strip()
    if current_draft_text:
        return revise_reply(
            email_data=email_data,
            current_draft_text=current_draft_text,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
    return draft_reply(
        email_data=email_data,
        to_value=to_value,
        cc_value=cc_value,
        email_id=email_id,
    )


def summary_looks_unusable(email_data):
    """Check if current summary is likely placeholder/noise text."""
    summary = " ".join(str(email_data.get("summary") or "").split()).lower().strip()
    if not summary:
        return False
    bad_phrases = (
        "summary unavailable",
        "summary generation failed",
        "unable to summarize",
        "view in browser",
    )
    for phrase in bad_phrases:
        if phrase in summary:
            return True
    return False


def should_auto_analyze_email(email_data, non_main_types=frozenset({"sent", "draft"})):
    """Return True when classify/summary work should run automatically."""
    if not ai_enabled() or not email_data:
        return False
    if email_data.get("type") in non_main_types:
        return False
    if bool(email_data.get("is_archived")):
        return False
    if not str(email_data.get("body") or "").strip():
        return False

    # Trigger when either triage fields or long-email summary is missing.
    missing_classification = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )
    needs_summary = should_summarize_email(email_data) and (
        not str(email_data.get("summary") or "").strip()
        or summary_looks_unusable(email_data)
    )
    return missing_classification or needs_summary


def run_ai_analysis(email_data, force=False):
    """Run classification + summary, then save updated fields."""
    changed = False
    missing_classification = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )

    # Classification and summary are saved independently, so partial success is allowed.
    if force or missing_classification:
        classification = classify_email(email_data, email_id=email_data.get("id"))
        if classification:
            update_email_ai_fields(
                email_id=email_data["id"],
                email_type=classification_to_email_type(classification),
                priority=classification.get("priority"),
                ai_category=classification.get("category"),
                ai_needs_response=classification.get("needs_response"),
                ai_confidence=classification.get("confidence"),
            )
            changed = True

    missing_summary = not str(email_data.get("summary") or "").strip()
    should_make_summary = should_summarize_email(email_data) and (
        force or missing_summary or summary_looks_unusable(email_data)
    )
    if should_make_summary:
        summary = summarize_email(email_data, email_id=email_data.get("id"))
        if summary:
            update_email_ai_fields(email_id=email_data["id"], summary=summary)
            changed = True
    return changed


AI_TASK_MAX_ITEMS = 200
AI_TASK_ACTIVE_STATUSES = {"queued", "running"}
AI_TASKS = {}
AI_TASK_INDEX = {}
AI_TASK_LOCK = threading.Lock()


def _cleanup_tasks_locked():
    """Remove older completed tasks if in-memory cache grows too large."""
    if len(AI_TASKS) <= AI_TASK_MAX_ITEMS:
        return

    # Keep only recent tasks in memory to avoid unbounded growth.
    done_tasks = [
        task for task in AI_TASKS.values() if task.get("status") not in AI_TASK_ACTIVE_STATUSES
    ]
    # Remove oldest completed items first; active tasks stay pinned.
    done_tasks.sort(key=lambda task: float(task.get("created_at") or 0))
    while len(AI_TASKS) > AI_TASK_MAX_ITEMS and done_tasks:
        task = done_tasks.pop(0)
        task_id = task["id"]
        key = (task["type"], task["email_id"])
        if AI_TASK_INDEX.get(key) == task_id:
            AI_TASK_INDEX.pop(key, None)
        AI_TASKS.pop(task_id, None)


def _create_or_get_ai_task(task_type, email_id):
    """Create task unless active task already exists for same type+email."""
    key = (task_type, int(email_id))
    with AI_TASK_LOCK:
        existing_id = AI_TASK_INDEX.get(key)
        existing_task = AI_TASKS.get(existing_id) if existing_id else None
        # Reuse active task so frontend polling has one task id per action.
        if existing_task and existing_task.get("status") in AI_TASK_ACTIVE_STATUSES:
            return dict(existing_task), False

        now = time.time()
        task = {
            "id": uuid4().hex,
            "type": task_type,
            "email_id": int(email_id),
            "status": "queued",
            "result": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }
        AI_TASKS[task["id"]] = task
        AI_TASK_INDEX[key] = task["id"]
        _cleanup_tasks_locked()
        return dict(task), True


def _set_ai_task_status(task_id, status, result=None, error=None):
    """Update task status and optional payload fields."""
    with AI_TASK_LOCK:
        task = AI_TASKS.get(task_id)
        if not task:
            return
        task["status"] = status
        task["updated_at"] = time.time()
        if result is not None:
            task["result"] = result
        if error is not None:
            task["error"] = error
        if status not in AI_TASK_ACTIVE_STATUSES:
            # Release dedupe slot so a new task of the same type/email can be scheduled.
            key = (task["type"], task["email_id"])
            if AI_TASK_INDEX.get(key) == task_id:
                AI_TASK_INDEX.pop(key, None)


def get_ai_task(task_id):
    """Return task snapshot dict by id."""
    with AI_TASK_LOCK:
        task = AI_TASKS.get(task_id)
        return dict(task) if task else None


def serialize_ai_task(task):
    """Return API-friendly task payload."""
    payload = {
        "task_id": task["id"],
        "task_type": task["type"],
        "email_id": task["email_id"],
        "status": task["status"],
    }
    if task.get("result") is not None:
        payload["result"] = task["result"]
    if task.get("error"):
        payload["error"] = task["error"]
    return payload


def _analysis_task_worker(task_id, email_id):
    """Background worker: run analysis and write task result."""
    _set_ai_task_status(task_id, "running")
    try:
        email_data = fetch_email_by_id(email_id)
        if not email_data:
            raise ValueError("Email not found.")

        # Force mode guarantees a fresh pass for explicit user-triggered analysis.
        run_ai_analysis(email_data, force=True)
        refreshed = fetch_email_by_id(email_id) or email_data
        _set_ai_task_status(
            task_id,
            "completed",
            result={
                "summary": refreshed.get("summary"),
                "ai_category": refreshed.get("ai_category"),
                "ai_needs_response": refreshed.get("ai_needs_response"),
                "ai_confidence": refreshed.get("ai_confidence"),
                "priority": refreshed.get("priority"),
                "type": refreshed.get("type"),
            },
        )
    except Exception as exc:
        log_ai_event(
            task="analyze",
            status="error",
            email_id=email_id,
            detail=f"task_exception: {exc}",
        )
        _set_ai_task_status(task_id, "error", error=str(exc))


def _draft_task_worker(task_id, email_id, to_value, cc_value, current_reply_text):
    """Background worker: generate draft and save it."""
    _set_ai_task_status(task_id, "running")
    try:
        email_data = fetch_email_by_id(email_id)
        if not email_data:
            raise ValueError("Email not found.")

        if ai_enabled() and not str(email_data.get("ai_category") or "").strip():
            run_ai_analysis(email_data, force=True)
            email_data = fetch_email_by_id(email_id) or email_data

        # Draft generation supports both blank and user-edited starting text.
        draft_text = generate_reply_draft(
            email_data=email_data,
            to_value=to_value or "",
            cc_value=cc_value or "",
            current_draft_text=current_reply_text or "",
            email_id=email_id,
        )
        update_draft(email_id, draft_text)
        _set_ai_task_status(task_id, "completed", result={"draft": draft_text})
    except Exception as exc:
        log_ai_event(
            task="draft",
            status="error",
            email_id=email_id,
            detail=f"task_exception: {exc}",
        )
        _set_ai_task_status(task_id, "error", error=str(exc))


def start_analysis_task(email_id):
    """Create/start analysis task and return its metadata."""
    task, created = _create_or_get_ai_task("analyze", email_id)
    if created:
        threading.Thread(
            target=_analysis_task_worker,
            args=(task["id"], int(email_id)),
            daemon=True,
        ).start()
    return task


def start_draft_task(email_id, to_value, cc_value, current_reply_text):
    """Create/start draft task and return its metadata."""
    task, created = _create_or_get_ai_task("draft", email_id)
    if created:
        threading.Thread(
            target=_draft_task_worker,
            args=(
                task["id"],
                int(email_id),
                to_value or "",
                cc_value or "",
                current_reply_text or "",
            ),
            daemon=True,
        ).start()
    return task
