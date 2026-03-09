# MVC: Model
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
# Core runtime defaults and normalized label sets shared across classify/summarize flows.
OLLAMA_API_URL_DEFAULT = "http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL_DEFAULT = "qwen2.5:7b"
OLLAMA_TIMEOUT_SECONDS_DEFAULT = 45
OLLAMA_LONG_TASK_TIMEOUT_SECONDS_DEFAULT = 180
SUMMARY_MIN_CHARS_DEFAULT = 200
VALID_CATEGORIES = {"urgent", "informational", "junk"}
VALID_EMAIL_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}
LONG_OLLAMA_TASKS = {"draft", "revise", "draft_rewrite", "summarize", "summarize_rewrite"}
TASK_MODEL_ENV_MAP = {
    "classify": "OLLAMA_CLASSIFY_MODEL",
    "draft": "OLLAMA_DRAFT_MODEL",
    "revise": "OLLAMA_DRAFT_MODEL",
    "draft_rewrite": "OLLAMA_DRAFT_MODEL",
    "summarize": "OLLAMA_SUMMARY_MODEL",
    "summarize_rewrite": "OLLAMA_SUMMARY_MODEL",
}
LOCALHOST_NAMES = {"localhost"}
JUNK_LOW_CONFIDENCE_THRESHOLD = 0.78
# Sender/domain heuristics used to separate person-to-person mail from automated mail.
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
# Marker sets for filtering unusable summaries and stripping newsletter/footer boilerplate.
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
# Phrases frequently found in footer/utility text that should not dominate summaries.
SUMMARY_NOISE_MARKERS = (
    "is this email difficult to read",
    "view in browser",
    "open in browser",
    "read online",
    "sign up here",
    "subscribe here",
    "subscribe now",
    "if you're not subscribed",
    "if you are not subscribed",
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
SUMMARY_UTILITY_MARKERS = (
    "read more",
    "alerts center",
    "contact us",
    "customer service",
    "for further assistance",
    "you are currently subscribed",
    "if you're not subscribed",
    "if you are not subscribed",
    "sign up here",
    "support@",
    "privacy policy",
    "cookie policy",
    "copyright",
    "dow jones",
    "route 1",
    "monmouth junction",
    "sponsored by",
    "is this email difficult to read",
)
# Common model-inserted lines that are not grounded in the source email.
SUMMARY_HALLUCINATION_MARKERS = (
    "if you're not subscribed",
    "if you are not subscribed",
    "sign up here",
    "subscribe here",
    "subscribe now",
    "click here to subscribe",
)
# Stopwords removed before token-overlap grounding checks.
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
# Regex-level footer patterns used for sentence/fragment noise filtering.
FOOTER_NOISE_REGEX_PATTERNS = (
    r"\bmanage\s+(?:email\s+)?preferences\b",
    r"\bemail\s+preferences\b",
    r"\bmanage\s+subscription\b",
    r"\b(?:sign\s+up|subscribe)(?:\s+(?:here|now))?\b",
    r"\bif\s+you(?:'re| are)\s+not\s+subscribed\b",
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
    r"\bis\s+this\s+email\s+difficult\s+to\s+read\??\b",
    r"\bread\s+more\b",
    r"\balerts?\s+center\b",
    r"\bcontact\s+us\b",
    r"\bcustomer\s+service\b",
    r"\bfor\s+further\s+assistance\b",
    r"\byou\s+are\s+currently\s+subscribed(?:\s+as)?\b",
    r"\bsponsored\s+by\b",
)
# Draft-quality failure markers plus request-language cues for actionable detection.
DRAFT_FAILURE_MARKERS = (
    "as an ai",
    "unable to draft",
    "unable to write",
    "cannot draft",
    "can't draft",
    "insufficient information",
    "not enough information",
)
DRAFT_OBSERVER_MARKERS = (
    "the main points i found were",
    "the main point i found was",
    "here are the main points",
    "the key points are",
    "the main detail i noted is",
    "i reviewed the information and captured the key points",
    "the sender is asking",
    "this email is about",
    "here's a draft",
)
DRAFT_GENERIC_MARKERS = (
    "thanks for your email about",
    "thanks for sharing the update about",
    "i appreciate the update",
    "i have the details and will send a complete response",
    "i will send a complete response",
    "i will follow up shortly",
    "i'll follow up shortly",
    "i will get back to you",
    "i'll get back to you",
    "let me know if you want any follow-up action from my side",
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
SUMMARY_PLEASANTRY_MARKERS = (
    "hope you're well",
    "hope you are well",
    "hope you're doing well",
    "hope you are doing well",
    "i hope this email finds you well",
    "trust you're well",
    "trust you are well",
)
SUMMARY_CLOSING_MARKERS = (
    "thanks again",
    "thank you again",
    "best,",
    "best regards",
    "regards,",
    "sincerely,",
    "cheers,",
)
SUMMARY_TASK_MARKERS = (
    "please ",
    "need everyone to",
    "need you to",
    "make sure",
    "let me know",
    "confirm",
    "reply",
    "respond",
    "submit",
    "send",
    "share",
    "review",
    "complete",
    "enter",
    "follow up",
    "stay focused",
    "approve",
)
SUMMARY_OVERVIEW_MARKERS = (
    "thank",
    "appreciate",
    "update",
    "meeting",
    "deliverable",
    "progress",
    "project",
    "timeline",
    "status",
)
JUNK_STRONG_MARKERS = (
    "viagra",
    "casino bonus",
    "adult",
    "xxx",
    "lottery",
    "wire transfer",
    "gift card",
    "inheritance",
    "beneficiary",
    "sugar daddy",
)
JUNK_MONEY_BAIT_MARKERS = (
    "crypto giveaway",
    "crypto presale",
    "guaranteed income",
    "guaranteed returns",
    "claim your prize",
    "winner selected",
    "cash prize",
    "bonus reward",
    "refund due",
    "debt relief",
    "passive income",
    "investment opportunity",
    "double your money",
    "earn from home",
    "work from home",
)
JUNK_PROMOTION_MARKERS = (
    "limited time offer",
    "exclusive deal",
    "special promotion",
    "special offer",
    "buy now",
    "shop now",
    "order now",
    "claim now",
    "redeem now",
    "unlock savings",
    "free trial",
    "free gift",
    "no obligation",
    "lowest price",
    "clearance",
)
JUNK_PRESSURE_MARKERS = (
    "act now",
    "last chance",
    "don't miss out",
    "ends tonight",
    "expires soon",
    "today only",
    "only a few left",
    "urgent action",
    "immediate action",
    "final notice",
    "before it's gone",
)
JUNK_ACCOUNT_BAIT_MARKERS = (
    "verify your account",
    "confirm your account",
    "account suspended",
    "account on hold",
    "unusual activity",
    "secure your account",
    "payment failed",
    "update your payment",
    "billing issue",
    "verify your identity",
    "password expires",
)
JUNK_BULK_FOOTER_MARKERS = (
    "unsubscribe",
    "manage preferences",
    "manage subscription",
    "view in browser",
    "open in browser",
    "why am i getting this",
    "advertisement",
    "sponsored",
    "opt out",
)
JUNK_LEADGEN_MARKERS = (
    "quick question",
    "following up",
    "circle back",
    "book a demo",
    "schedule a demo",
    "increase your revenue",
    "generate more leads",
    "grow your pipeline",
    "sales team",
    "calendar link",
)
TRANSACTIONAL_HAM_MARKERS = (
    "receipt",
    "order confirmation",
    "order shipped",
    "delivery update",
    "statement is ready",
    "appointment",
    "meeting",
    "interview",
    "class schedule",
    "verification code",
    "one-time code",
    "payment receipt",
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


def _api_url_candidates():
    """Return loopback-safe Ollama endpoint candidates."""
    primary = _api_url()
    parsed = urlparse(primary)
    candidates = [primary]
    if not _is_loopback_host(parsed.hostname):
        return candidates

    netloc = parsed.netloc
    suffix = ""
    if "@" in netloc:
        suffix = netloc.split("@", 1)[0] + "@"
        netloc = netloc.split("@", 1)[1]
    port = f":{parsed.port}" if parsed.port else ""
    host_variants = ("127.0.0.1", "localhost")
    for host in host_variants:
        if parsed.hostname == host:
            continue
        candidate = parsed._replace(netloc=f"{suffix}{host}{port}").geturl()
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _model_name(task=None):
    """Model name.
    """
    # Prefer task-specific model overrides for latency-sensitive workflows.
    task_name = str(task or "").strip().lower()
    env_names = []
    task_env = TASK_MODEL_ENV_MAP.get(task_name)
    if task_env:
        env_names.append(task_env)
    env_names.append("OLLAMA_MODEL")
    for env_name in env_names:
        value = (os.getenv(env_name) or "").strip()
        if value:
            return value
    return OLLAMA_MODEL_DEFAULT


def _timeout_seconds(task=None):
    """Timeout seconds.
    """
    # Give slower local generations a larger default timeout than short classification calls.
    task_name = str(task or "").strip().lower()
    env_name = "OLLAMA_LONG_TASK_TIMEOUT_SECONDS" if task_name in LONG_OLLAMA_TASKS else "OLLAMA_TIMEOUT_SECONDS"
    default_value = (
        OLLAMA_LONG_TASK_TIMEOUT_SECONDS_DEFAULT
        if task_name in LONG_OLLAMA_TASKS
        else OLLAMA_TIMEOUT_SECONDS_DEFAULT
    )
    raw = (os.getenv(env_name) or "").strip()
    try:
        parsed = float(raw) if raw else float(default_value)
    except ValueError:
        parsed = float(default_value)
    return max(5.0, min(300.0, parsed))


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


def _looks_utility_sentence(text):
    """Return True when text is mostly footer, support, or utility boilerplate."""
    normalized = _compact_text(text).lower()
    if not normalized:
        return True
    hits = sum(marker in normalized for marker in SUMMARY_UTILITY_MARKERS)
    if hits >= 2:
        return True
    if hits >= 1 and len(_content_tokens(normalized)) <= 14:
        return True
    if EMAIL_ADDRESS_PATTERN.search(normalized) and any(
        marker in normalized for marker in ("support", "customer service", "contact us")
    ):
        return True
    if re.search(r"\b1[-\s]?800[-\s]?[a-z0-9-]+\b", normalized):
        return True
    if re.search(r"\b(?:route|street|st\.|avenue|ave\.|road|rd\.|junction)\b", normalized) and re.search(
        r"\b[a-z]{2}\s+\d{5}(?:-\d{4})?\b",
        normalized,
    ):
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
    # Treat bare publication mastheads as non-content when no action verbs are present.
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


def _strip_title_prefix(candidate_text, title_text):
    """Remove a repeated title prefix from a sentence when the remainder is informative."""
    candidate = _compact_text(candidate_text)
    title = _compact_text(title_text)
    if not candidate or not title or title.lower() == "(no subject)":
        return candidate

    title_variants = [title]
    if ":" in title:
        trailing = _compact_text(title.split(":", 1)[1])
        if trailing:
            title_variants.append(trailing)
        flattened = _compact_text(title.replace(":", " "))
        if flattened:
            title_variants.append(flattened)

    for variant in title_variants:
        lowered_variant = variant.lower().rstrip(" .:-")
        lowered_candidate = candidate.lower()
        if not lowered_candidate.startswith(lowered_variant):
            continue
        remainder = candidate[len(variant) :].strip(" .:-")
        if len(remainder) >= 24:
            return remainder
    return candidate


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
    seen_lines = set()
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
        normalized_line = re.sub(r"\(\s*\[link\]\s*\)", "", normalized_line)
        normalized_line = re.sub(r"^read more:?\s*", "", normalized_line, flags=re.IGNORECASE)
        normalized_line = re.sub(r"^question:\s*", "", normalized_line, flags=re.IGNORECASE)
        normalized_line = re.sub(r"^answer from\s+[^:]+:?[\s-]*", "", normalized_line, flags=re.IGNORECASE)
        normalized_line = _strip_footer_noise_text(normalized_line)
        normalized_line = _compact_text(normalized_line)
        if not normalized_line:
            # Drop lines that collapse to empty after noise stripping.
            continue
        lowered = normalized_line.lower()
        if re.match(
            r"^(?:read more|get tickets|try premium button|follow us|preferences|feedback|alerts center|"
            r"contact us|other\s*\(|top stories for|answer from|view in browser|open in browser)\b",
            lowered,
        ):
            continue
        if re.match(
            r"^(?:this message was sent to|you are currently subscribed as|copyright \d{4}|"
            r"dow jones & company|cbc canadian broadcasting corporation)\b",
            lowered,
        ):
            continue
        if lowered.startswith("platforms:"):
            continue
        if lowered in {"[link]", "joji", "cbc gem: say goodbye to ads."}:
            continue
        if normalized_line in seen_lines:
            continue
        seen_lines.add(normalized_line)
        cleaned_lines.append(normalized_line)

    cleaned = "\n".join(cleaned_lines)
    cleaned = _strip_footer_noise_text(cleaned)
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
        if _looks_utility_sentence(sentence):
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


def _summary_profile(email_data):
    """Return summary length settings based on email body size."""
    body = _clean_body_for_prompt(email_data.get("body") or "", max_chars=20000)
    body_length = len(body)
    if body_length >= 4000:
        return {
            "char_limit": 3000,
            "output_sentences": 10,
            "context_sentences": 44,
            "context_chars": 15000,
            "prompt_target": "8-10 sentences",
            "num_predict": 1200,
        }
    if body_length >= 1800:
        return {
            "char_limit": 2400,
            "output_sentences": 8,
            "context_sentences": 34,
            "context_chars": 13000,
            "prompt_target": "6-8 sentences",
            "num_predict": 950,
        }
    if body_length >= 900:
        return {
            "char_limit": 1600,
            "output_sentences": 6,
            "context_sentences": 26,
            "context_chars": 11000,
            "prompt_target": "4-6 sentences",
            "num_predict": 720,
        }
    return {
        "char_limit": 900,
        "output_sentences": 4,
        "context_sentences": 16,
        "context_chars": 8000,
        "prompt_target": "3-4 sentences",
        "num_predict": 460,
    }


def _body_for_context(email_data, max_chars=8000, max_sentences=14):
    """Body for context.
    """
    # Build body for context text that is passed into model prompts.
    key_sentences = _extract_key_sentences(
        email_data.get("body") or "",
        max_sentences=max_sentences,
    )
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


def _natural_join(items):
    """Join short phrases into natural English."""
    values = [_compact_text(item) for item in items if _compact_text(item)]
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} and {values[1]}"
    return f"{', '.join(values[:-1])}, and {values[-1]}"


def _first_regex_match(text, pattern):
    """Return first regex match as compact text."""
    match = re.search(pattern, str(text or ""), flags=re.IGNORECASE)
    if not match:
        return ""
    return _compact_text(match.group(0))


def _summary_sender_name(email_data):
    """Return sender display text suitable for summaries."""
    sender_raw = _compact_text(email_data.get("sender") or "")
    if not sender_raw:
        return "The sender"
    display = sender_raw.split("<", 1)[0].strip().strip('"')
    if "@" in display and " " not in display:
        display = display.split("@", 1)[0]
    display = display.replace(".", " ").replace("_", " ").replace("-", " ")
    display = " ".join(display.split())
    return display or "The sender"


def _summary_title_topic(title_text):
    """Return the most useful topic phrase from an email subject line."""
    title = _compact_text(title_text)
    if not title or title.lower() == "(no subject)":
        return ""
    if ":" in title:
        trailing = _compact_text(title.split(":", 1)[1])
        if len(trailing) >= 12:
            return trailing
    return title


def _bulk_newsletter_summary(email_data):
    """Return deterministic summary for newsletter/promotional emails."""
    if not _looks_bulk_or_newsletter(email_data):
        return None

    title = _compact_text(email_data.get("title") or "")
    raw_body = str(email_data.get("body") or "")
    body = _clean_body_for_prompt(email_data.get("body") or "", max_chars=12000)
    combined = _compact_text(" ".join(part for part in [title, body] if part))
    if not combined:
        return None

    sender_name = _summary_sender_name(email_data)
    key_sentences = _extract_key_sentences(body, max_sentences=6)
    title_topic = _summary_title_topic(title)

    # Quora/news digests are better summarized by the featured topics than by model paraphrase.
    digest_questions = [
        _compact_text(match).strip(" -:?!")
        for match in re.findall(r"Question:\s*(.+)", raw_body, flags=re.IGNORECASE)
    ]
    digest_questions = [
        question for question in digest_questions if question and len(question) >= 18
    ]
    if digest_questions:
        featured = title if title and not _is_near_subject_copy(title, digest_questions[0]) else ""
        topic_list = digest_questions[:3]
        sentences = []
        if featured:
            sentences.append(_ensure_sentence_ending(f"This digest highlights {featured}"))
        if topic_list:
            lead = topic_list[0]
            if len(topic_list) == 1:
                sentences.append(_ensure_sentence_ending(f"It also includes a question about {lead}"))
            elif len(topic_list) == 2:
                sentences.append(
                    _ensure_sentence_ending(
                        f"It includes questions about {topic_list[0]} and {topic_list[1]}"
                    )
                )
            else:
                sentences.append(
                    _ensure_sentence_ending(
                        f"It includes questions about {topic_list[0]}, {topic_list[1]}, and {topic_list[2]}"
                    )
                )
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    # Ticket promos should emphasize the offer window, code, and a sample of dates/locations.
    if re.search(r"\bupcoming tour\b", combined, flags=re.IGNORECASE) and re.search(
        r"\bpresale starts\b",
        combined,
        flags=re.IGNORECASE,
    ):
        presale = _first_regex_match(raw_body, r"Presale starts at [^\n]+")
        offer_code_match = re.search(r"Offer code:\s*\*?([A-Z0-9-]+)\*?", raw_body, flags=re.IGNORECASE)
        offer_code = offer_code_match.group(1) if offer_code_match else ""
        ends = _first_regex_match(raw_body, r"Ends [^\n]+")
        date_matches = re.findall(
            r"\*?([A-Za-z .'-]+,\s*[A-Z]{2})\*?\s*\n+\s*([A-Z][a-z]+ \d{1,2}, \d{4})",
            raw_body,
        )
        sample_dates = [f"{city} on {date}" for city, date in date_matches[:2]]
        sentences = [
            _ensure_sentence_ending(
                f"{sender_name} is offering you presale access to an upcoming tour"
            )
        ]
        detail_bits = [bit for bit in [presale, f"Offer code {offer_code}" if offer_code else "", ends] if bit]
        if detail_bits:
            sentences.append(_ensure_sentence_ending(" ".join(detail_bits)))
        if sample_dates:
            sentences.append(
                _ensure_sentence_ending(
                    f"The email lists dates including {' and '.join(sample_dates)}"
                )
            )
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    # Subscription upsells should emphasize the product, benefits, and pricing.
    if re.search(r"\bpremium\b", combined, flags=re.IGNORECASE) and re.search(
        r"(?:free trial|\$\d+(?:\.\d{2})?)",
        combined,
        flags=re.IGNORECASE,
    ):
        trial = _first_regex_match(raw_body, r"Try it free for \d+\s+days?")
        price = _first_regex_match(raw_body, r"\$\d+(?:\.\d{2})?/\w+")
        perks = []
        if "eliminate the ads" in combined or "say goodbye to ads" in combined:
            perks.append("ad-free viewing")
        if "cbc news network" in combined:
            perks.append("CBC News Network access")
        if "early premieres" in combined:
            perks.append("early premieres on select titles")
        sentences = [
            _ensure_sentence_ending(
                f"{sender_name} is promoting a Premium upgrade for {title or 'its service'}"
            )
        ]
        if perks:
            sentences.append(
                _ensure_sentence_ending(f"It adds {_natural_join(perks)}")
            )
        pricing_bits = [bit for bit in [trial, price] if bit]
        if pricing_bits:
            sentences.append(_ensure_sentence_ending(" ".join(pricing_bits)))
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    numbered_sections = []
    for match in re.finditer(r"(?:^|\s)(\d+)\.\s+(.*?)(?=(?:\s+\d+\.\s+)|$)", body, flags=re.DOTALL):
        section_text = _compact_text(match.group(2))
        if not section_text:
            continue
        section_lead = _extract_key_sentences(section_text, max_sentences=1)
        candidate = section_lead[0] if section_lead else section_text
        candidate = _strip_title_prefix(candidate, title)
        candidate = _strip_footer_noise_text(candidate)
        candidate = _compact_text(candidate)
        if not candidate or _looks_utility_sentence(candidate) or _is_noise_fragment(candidate):
            continue
        numbered_sections.append(candidate)

    if numbered_sections:
        sentences = []
        if title_topic:
            sentences.append(
                _ensure_sentence_ending(f"You received a newsletter from {sender_name} about {title_topic}")
            )
        lead = numbered_sections[0]
        if lead and not _is_near_subject_copy(lead, title_topic or title):
            sentences.append(_ensure_sentence_ending(f"It highlights {lead}"))
        if len(numbered_sections) > 1:
            sentences.append(_ensure_sentence_ending(f"It also covers {numbered_sections[1]}"))
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    # Article alerts should summarize the headline plus teaser sentence.
    if title and key_sentences:
        teaser = next(
            (
                _strip_title_prefix(sentence, title)
                for sentence in key_sentences
                if not _looks_utility_sentence(sentence)
            ),
            "",
        )
        teaser = _strip_footer_noise_text(teaser)
        teaser = _compact_text(teaser)
        if teaser and len(teaser) >= 30 and len(body) < 1800:
            summary = _compact_text(
                f"You received an article alert from {sender_name} about {title_topic or title}. It highlights {teaser}"
            )
            if summary and len(summary) <= SUMMARY_MAX_CHARS:
                return summary

    if len(body) >= 1200:
        # Long newsletters need real summarization, not the short promo fallback.
        return None

    if len(key_sentences) >= 4:
        return None

    promotion_markers = (
        "% off",
        "promo",
        "discount",
        "offer",
        "save up to",
        "maximum savings",
        "limited time",
        "special offer",
        "shop now",
        "book now",
        "membership",
        "sale",
        "coupon",
    )
    editorial_markers = (
        "top stories",
        "briefing",
        "digest",
        "newsletter",
        "what happened",
        "highlights",
        "roundup",
        "analysis",
        "week in review",
        "today in",
        "latest news",
    )
    promotion_hits = _matching_patterns(combined, promotion_markers)
    editorial_hits = _matching_patterns(combined, editorial_markers)
    offer = _first_regex_match(combined, r"\b\d{1,3}%\s+off\b")
    trip_limit = _first_regex_match(combined, r"\b(?:up to\s+)?\d+\s+(?:trips?|rides?)\b")
    validity = _first_regex_match(
        combined,
        r"\b(?:valid through|through|until|expires?(?: on)?)\s+"
        r"(?:[A-Z][a-z]+ \d{1,2}, \d{4}(?: \d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm))?|"
        r"\d{1,2}/\d{1,2}/\d{2,4})",
    )
    max_savings = _first_regex_match(
        combined,
        r"\b(?:maximum savings of|save up to|up to)\s*\$\d+(?:\.\d{2})?(?:\s+per\s+(?:ride|trip|order|item))?",
    )
    auto_applied = bool(
        re.search(
            r"\b(?:promo|discount|offer)\s+has\s+automatically\s+been\s+applied\b",
            combined,
            flags=re.IGNORECASE,
        )
    )
    if editorial_hits and len(promotion_hits) < 3 and not offer and not max_savings:
        return None
    if not (offer or max_savings or auto_applied or len(promotion_hits) >= 2):
        return None

    features = []
    feature_patterns = (
        (r"\b(?:reserve your ride|schedule your ride|flight schedule|departure gate)\b", "ride scheduling"),
        (r"\bcommute alerts?\b", "commute alerts"),
        (r"\b(?:pin verification|safety preferences|night rides?)\b", "night-ride safety settings"),
        (r"\b(?:uber one|membership)\b", "membership perks"),
    )
    for pattern, label in feature_patterns:
        if re.search(pattern, combined, flags=re.IGNORECASE):
            features.append(label)

    sentences = []
    if offer:
        offer_text = offer
        if trip_limit and trip_limit.lower() not in offer_text.lower():
            offer_text = f"{offer_text} on {trip_limit}"
        sentence = f"{sender_name} is offering you {offer_text}"
        if auto_applied:
            sentence += ", and the promo is already applied to your account"
        sentences.append(_ensure_sentence_ending(sentence))
    elif max_savings:
        sentences.append(_ensure_sentence_ending(f"{sender_name} is promoting a savings offer for you"))
    elif title and title.lower() != "(no subject)":
        sentences.append(_ensure_sentence_ending(f"{sender_name} sent you a promotional update about {title}"))

    if validity:
        validity_lower = validity.lower()
        if validity_lower.startswith(("valid through", "until ")):
            sentences.append(_ensure_sentence_ending(f"The offer is {validity}"))
        elif validity_lower.startswith("through "):
            sentences.append(_ensure_sentence_ending(f"The offer runs {validity}"))
        elif validity_lower.startswith("expires"):
            sentences.append(_ensure_sentence_ending(f"It {validity}"))
        else:
            sentences.append(_ensure_sentence_ending(f"The offer timing is {validity}"))

    if max_savings:
        cap = _first_regex_match(
            max_savings,
            r"\$\d+(?:\.\d{2})?(?:\s+per\s+(?:ride|trip|order|item))?",
        )
        if cap:
            sentences.append(_ensure_sentence_ending(f"Savings are capped at {cap}"))

    if features:
        sentences.append(_ensure_sentence_ending(f"It also promotes {_natural_join(features)}"))
    elif sentences:
        sentences.append("The rest of the email is promotional copy and does not require a reply.")

    summary = _compact_text(" ".join(sentences[:3]))
    if not summary:
        return None
    if len(summary) > SUMMARY_MAX_CHARS:
        summary = f"{summary[: SUMMARY_MAX_CHARS - 3]}..."
    return summary


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
    profile = _summary_profile(email_data)
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
                _body_for_context(
                    email_data,
                    max_chars=min(6000, profile["context_chars"]),
                    max_sentences=min(24, profile["context_sentences"]),
                ),
            ]
            if part
        )
    )
    source_tokens = set(_content_tokens(source))

    # Split generated text into candidate sentences and filter for grounded facts only.
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
        # Require reasonable grounding in source content unless sentence is title-aligned.
        support_ratio = _summary_support_ratio(cleaned, source_tokens)
        if support_ratio < 0.52 and not _is_near_subject_copy(cleaned, title):
            continue
        kept.append(cleaned)
        if len(kept) >= profile["output_sentences"]:
            break

    if not kept:
        return None

    normalized = " ".join(_ensure_sentence_ending(sentence) for sentence in kept)
    normalized = _compact_text(normalized)
    if len(normalized) > profile["char_limit"]:
        normalized = f"{normalized[: profile['char_limit'] - 3]}..."
    return normalized or None


def _looks_summary_parrot(summary_text, email_data):
    """Return True when summary appears copied from the email body."""
    # Keep model summaries readable by rejecting near-verbatim source echoes.
    profile = _summary_profile(email_data)
    summary = _compact_text(summary_text)
    if len(summary) < 60:
        return False

    source = _compact_text(
        _body_for_context(
            email_data,
            max_chars=min(6000, profile["context_chars"]),
            max_sentences=min(24, profile["context_sentences"]),
        )
    )
    if not source:
        return False

    summary_lower = summary.lower()
    source_lower = source.lower()
    if len(summary) >= 80 and summary_lower in source_lower:
        return True

    # Compare sentence-level overlap so paraphrase-light outputs are treated as copied.
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
    profile = _summary_profile(email_data)
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
        "Return one compact plain-text paragraph. "
        f"Use {profile['prompt_target']} when the source email is long."
    )
    user_prompt = (
        "Rewrite this summary in your own words so it is not copied from the email.\n\n"
        f"Current summary:\n{candidate}\n\n"
        f"{_email_context_block(
            email_data,
            body_max_chars=profile['context_chars'],
            body_max_sentences=profile['context_sentences'],
        )}"
    )
    response_text = _call_ollama(
        task="summarize_rewrite",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=max(280, min(800, profile["num_predict"])),
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
    if len(rewritten) > profile["char_limit"]:
        rewritten = f"{rewritten[: profile['char_limit'] - 3]}..."
    return rewritten or None


def _looks_summary_closing_sentence(sentence_text):
    """Return True when sentence is mainly a sign-off or closing pleasantry."""
    normalized = _compact_text(sentence_text).lower().strip(" .,!;:")
    if not normalized:
        return True
    if normalized in {"best", "regards", "sincerely", "cheers", "thanks", "thank you"}:
        return True
    return any(normalized.startswith(marker) for marker in SUMMARY_CLOSING_MARKERS)


def _looks_low_information_sentence(sentence_text):
    """Return True for greetings or pleasantries that add little summary value."""
    normalized = _compact_text(sentence_text).lower()
    if not normalized:
        return True
    if any(marker in normalized for marker in SUMMARY_PLEASANTRY_MARKERS):
        return True
    if normalized.startswith("welcome to ") and any(
        marker in normalized for marker in ("roundup", "digest", "newsletter", "briefing")
    ):
        return True
    if normalized.startswith(("hi ", "hello ", "dear ")) and len(_text_tokens(normalized)) <= 10:
        return True
    return _looks_summary_closing_sentence(normalized)


def _has_temporal_summary_hint(sentence_text):
    """Return True when sentence appears to include timing or deadline detail."""
    normalized = _compact_text(sentence_text).lower()
    if not normalized:
        return False
    if re.search(
        r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|today|tomorrow|"
        r"tonight|this morning|this afternoon|next week|next month|next quarter|"
        r"deadline|noon|midnight|morning|afternoon|evening|eod|eom)\b",
        normalized,
    ):
        return True
    if re.search(
        r"\b(?:by|before|after|until)\s+(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?|"
        r"noon|midnight|end of day|eod|monday|tuesday|wednesday|thursday|friday|"
        r"saturday|sunday)\b",
        normalized,
    ):
        return True
    return bool(
        re.search(
            r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|"
            r"dec(?:ember)?)\b",
            normalized,
        )
    )


def _rewrite_fallback_summary_sentence(sentence_text, email_data):
    """Rewrite an extracted body sentence into concise summary wording."""
    text = _compact_text(sentence_text).strip(" -:")
    if not text:
        return ""

    text = re.sub(
        r"^(?:hi|hello|dear)\b[^.!?]{0,60}[,.!]\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"^(?:hi|hello|dear)\b(?:\s+(?!(?:i|we|just|please|can|could|would|thank|"
        r"thanks|following)\b)[a-z0-9.'&-]+){0,8}\s+"
        r"(?=(?:i|we|just|please|can|could|would|thank|thanks|following)\b)",
        "",
        text,
        flags=re.IGNORECASE,
    )
    if re.match(r"^(?:hi|hello|dear)\b", text, flags=re.IGNORECASE):
        lowered = text.lower()
        for marker in (
            " i wanted to ",
            " we wanted to ",
            " just wanted to ",
            " please ",
            " can you ",
            " could you ",
            " would you ",
            " thank you ",
            " thanks ",
            " following up ",
            " i need ",
            " we need ",
        ):
            marker_index = lowered.find(marker)
            if marker_index > 0:
                text = text[marker_index + 1 :]
                break
    text = _compact_text(text).strip(" -:")
    if not text or _looks_low_information_sentence(text):
        return ""

    sender_name = _sender_display_name(email_data.get("sender")) or "The sender"

    replacements = (
        (
            r"^i wanted to send (?:a )?(?:quick )?(?:note|update)(?: before [^,]+)? "
            r"to thank (?:everyone|the team|all of you) for (.+)$",
            lambda match: f"{sender_name} thanks the team for {match.group(1)}",
        ),
        (
            r"^i wanted to thank (?:everyone|the team|all of you) for (.+)$",
            lambda match: f"{sender_name} thanks the team for {match.group(1)}",
        ),
        (
            r"^i appreciate (.+)$",
            lambda match: f"{sender_name} appreciates {match.group(1)}",
        ),
        (
            r"^going into ([^,]+), i need everyone to (.+)$",
            lambda match: f"For {match.group(1)}, everyone should {match.group(2)}",
        ),
        (
            r"^going into ([^,]+), i need you to (.+)$",
            lambda match: f"For {match.group(1)}, you should {match.group(2)}",
        ),
        (
            r"^i need everyone to (.+)$",
            lambda match: f"Everyone should {match.group(1)}",
        ),
        (
            r"^i need you to (.+)$",
            lambda match: f"You should {match.group(1)}",
        ),
        (
            r"^please make sure (.+)$",
            lambda match: f"Make sure {match.group(1)}",
        ),
        (
            r"^if you are running into (.+?), let me know (.+)$",
            lambda match: f"If you run into {match.group(1)}, let the sender know {match.group(2)}",
        ),
        (
            r"^if you run into (.+?), let me know (.+)$",
            lambda match: f"If you run into {match.group(1)}, let the sender know {match.group(2)}",
        ),
        (
            r"^i am expecting (.+)$",
            lambda match: f"{sender_name} expects {match.group(1)}",
        ),
        (
            r"^let's keep (.+)$",
            lambda match: f"Keep {match.group(1)}",
        ),
    )
    for pattern, replacer in replacements:
        updated = re.sub(pattern, replacer, text, flags=re.IGNORECASE)
        if updated != text:
            text = updated
            break

    generic_replacements = (
        (r"^i(?:'m| am) writing to ", ""),
        (r"^i wanted to ", ""),
        (r"^just wanted to ", ""),
    )
    for pattern, replacement in generic_replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    text = re.sub(r"\blet me know\b", "let the sender know", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\bthe work that has gone into\b",
        "the work on",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bso we can\b", "so the team can", text, flags=re.IGNORECASE)
    text = _compact_text(text).strip(" -:")
    if not text or _looks_low_information_sentence(text):
        return ""
    if text:
        text = text[0].upper() + text[1:]
    return _ensure_sentence_ending(text)


def _summary_sentence_score(sentence_text, position):
    """Score extracted sentences so the fallback keeps concrete details first."""
    normalized = _compact_text(sentence_text)
    if not normalized:
        return -1
    lowered = normalized.lower()
    if _looks_low_information_sentence(lowered):
        return -1

    score = max(0, 6 - position)
    if any(marker in lowered for marker in SUMMARY_TASK_MARKERS):
        score += 6
    if _has_temporal_summary_hint(lowered):
        score += 6
    if any(marker in lowered for marker in SUMMARY_OVERVIEW_MARKERS):
        score += 2
    if re.search(
        r"\b(?:priority|blocker|delay|deliverable|meeting|budget|invoice|proposal|"
        r"schedule|timeline|review|approval|update|follow-up)\b",
        lowered,
    ):
        score += 2
    return score


def _select_fallback_summary_sentences(email_data, max_sentences=3):
    """Select concrete body sentences for the summary fallback."""
    title = _compact_text(email_data.get("title") or "")
    key_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=10)
    if not key_sentences:
        return []

    candidates = []
    overview = None
    for position, sentence in enumerate(key_sentences):
        rewritten = _rewrite_fallback_summary_sentence(sentence, email_data)
        if not rewritten or _is_noise_fragment(rewritten):
            continue
        score = _summary_sentence_score(sentence, position)
        candidates.append((score, position, rewritten))
        lowered = _compact_text(sentence).lower()
        if overview is None and position < 4:
            if any(marker in lowered for marker in ("thank", "appreciate")):
                overview = rewritten
            elif not any(marker in lowered for marker in SUMMARY_TASK_MARKERS):
                overview = rewritten

    if not candidates:
        return []

    selected = []
    if overview:
        selected.append(overview)

    ranked_candidates = sorted(candidates, key=lambda item: (-item[0], item[1]))
    if len(candidates) >= 5 and max_sentences >= 4:
        # Long newsletters need coverage across sections, not just the globally highest scores.
        segment_count = min(max_sentences, len(candidates))
        segment_size = max(1, (len(candidates) + segment_count - 1) // segment_count)
        segmented = []
        for start in range(0, len(candidates), segment_size):
            segment = candidates[start : start + segment_size]
            if not segment:
                continue
            segmented.append(sorted(segment, key=lambda item: (-item[0], item[1]))[0])
        ranked_candidates = segmented + [
            candidate for candidate in ranked_candidates if candidate not in segmented
        ]

    for _, _, rewritten in ranked_candidates:
        if any(_token_overlap_ratio(rewritten, existing) > 0.9 for existing in selected):
            continue
        if _is_near_subject_copy(rewritten, title) and selected:
            continue
        selected.append(rewritten)
        if len(selected) >= max_sentences:
            break

    return selected[:max_sentences]


def _merge_summary_with_fallback_coverage(summary_text, email_data):
    """Supplement long model summaries with missing fallback details when space allows."""
    profile = _summary_profile(email_data)
    summary = _compact_text(summary_text)
    if not summary or profile["output_sentences"] < 5:
        return summary

    summary_sentences = [
        _compact_text(part).strip(" -:")
        for part in re.split(r"(?<=[.!?])\s+", summary)
        if _compact_text(part)
    ]
    if not summary_sentences:
        summary_sentences = [summary]

    fallback_sentences = _select_fallback_summary_sentences(
        email_data,
        max_sentences=profile["output_sentences"],
    )
    if not fallback_sentences:
        return summary

    merged_sentences = list(summary_sentences)
    for sentence in reversed(fallback_sentences):
        if any(_token_overlap_ratio(sentence, existing) > 0.6 for existing in merged_sentences):
            continue
        candidate = _compact_text(
            " ".join(_ensure_sentence_ending(item) for item in merged_sentences + [sentence])
        )
        if len(candidate) > profile["char_limit"]:
            break
        merged_sentences.append(sentence)
        if len(merged_sentences) >= profile["output_sentences"]:
            break

    return _compact_text(" ".join(_ensure_sentence_ending(item) for item in merged_sentences))


def _extractive_summary_fallback(email_data):
    """Contextual summary fallback.
    """
    bulk_summary = _bulk_newsletter_summary(email_data)
    if bulk_summary:
        return bulk_summary
    # Prefer concrete extracted details from the body before generic prose.
    profile = _summary_profile(email_data)
    title = _compact_text(email_data.get("title") or "")
    extracted_sentences = _select_fallback_summary_sentences(
        email_data,
        max_sentences=profile["output_sentences"],
    )
    if extracted_sentences:
        extracted_sentences = [
            _compact_text(_strip_title_prefix(sentence, title))
            for sentence in extracted_sentences
            if _compact_text(_strip_title_prefix(sentence, title))
        ]
        summary = _compact_text(" ".join(extracted_sentences))
        if not _is_noise_fragment(summary):
            if len(summary) > profile["char_limit"]:
                summary = f"{summary[: profile['char_limit'] - 3]}..."
            return summary or None

    sender_name = _sender_display_name(email_data.get("sender")) or "the sender"
    actionable = _looks_actionable(email_data)
    newsletter_like = _looks_bulk_or_newsletter(email_data)

    intro = (
        f"This email from {sender_name} is about {title}."
        if title and title.lower() != "(no subject)"
        else f"This email is from {sender_name}."
    )

    if actionable:
        posture = "It appears to include follow-up instructions or a requested next step."
        action = "Review the message for the requested action and timing."
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
    if len(summary) > profile["char_limit"]:
        summary = f"{summary[: profile['char_limit'] - 3]}..."
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


def _email_context_block(email_data, body_max_chars=8000, body_max_sentences=14):
    """Email context block.
    """
    # Build email context block text that is passed into model prompts.
    title = _compact_text(email_data.get("title") or "(No subject)")
    sender = _compact_text(email_data.get("sender"))
    recipients = _compact_text(email_data.get("recipients"))
    cc = _compact_text(email_data.get("cc"))
    body = _body_for_context(
        email_data,
        max_chars=body_max_chars,
        max_sentences=body_max_sentences,
    )
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
    # Prefer explicit addresses; fallback to bracketed sender forms when needed.
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


def _matching_patterns(text, patterns):
    """Return matching patterns."""
    value = str(text or "").lower()
    return [pattern for pattern in patterns if pattern in value]


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
        'Output: {"category":"informational","needs_response":false,"priority":1,"confidence":0.84}\n\n'
        "5) From: security-check@pay-verify.example\n"
        "Subject: Urgent action required: verify your account\n"
        "Body: We noticed unusual activity. Click here to confirm your identity or your access may be suspended.\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.88}\n\n'
        "6) From: growth@revenue-ops.example\n"
        "Subject: Quick question about pipeline growth\n"
        "Body: We help teams generate more leads and book more demos. Worth 15 minutes next week?\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.67}'
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
    promotion_markers = (
        "% off",
        "promo",
        "discount",
        "offer",
        "save up to",
        "maximum savings",
        "limited time",
        "special offer",
        "take a trip",
        "shop now",
        "book now",
        "membership",
    )
    promotion_hits = _matching_patterns(combined, promotion_markers)
    return (
        _has_any_pattern(combined, sender_markers)
        or _has_any_pattern(combined, content_markers)
        or len(promotion_hits) >= 2
    )


def _looks_actionable(email_data):
    """Looks actionable.
    """
    title = str(email_data.get("title") or "").lower()
    body = str(email_data.get("body") or "").lower()
    combined = " ".join([title, body])

    # Require stronger response-language cues when message resembles bulk mail.
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
    non_actionable_markers = (
        "no action required",
        "no response needed",
        "no reply needed",
        "for your records",
        "unless there is an issue",
    )
    # Bulk messages are treated as non-actionable unless they contain explicit response language.
    if _has_any_pattern(combined, non_actionable_markers):
        return False
    if _looks_bulk_or_newsletter(email_data) and not _has_any_pattern(
        combined, response_markers
    ):
        return False
    if explicit_question and _has_any_pattern(combined, direct_question_markers):
        return True
    return _has_any_pattern(combined, response_markers)


def _junk_signal_assessment(email_data):
    """Return generalized junk-signal features for heuristics and prompts."""
    sender_info = _sender_parts(email_data.get("sender"))
    title_text = _compact_text(email_data.get("title") or "")
    body_text = _clean_body_for_prompt(email_data.get("body") or "")
    combined_text = " ".join(
        part
        for part in [sender_info.get("identity"), title_text.lower(), body_text.lower()]
        if part
    )
    bulk_signal = _looks_bulk_or_newsletter(email_data)
    sender_automated = _sender_looks_automated(sender_info)
    sender_personal_domain = _sender_uses_personal_domain(sender_info)
    transactional_hits = _matching_patterns(combined_text, TRANSACTIONAL_HAM_MARKERS)
    family_hits = {
        "strong_terms": _matching_patterns(combined_text, JUNK_STRONG_MARKERS),
        "money_bait": _matching_patterns(combined_text, JUNK_MONEY_BAIT_MARKERS),
        "promotion_cta": _matching_patterns(combined_text, JUNK_PROMOTION_MARKERS),
        "urgency_pressure": _matching_patterns(combined_text, JUNK_PRESSURE_MARKERS),
        "account_bait": _matching_patterns(combined_text, JUNK_ACCOUNT_BAIT_MARKERS),
        "bulk_footer": _matching_patterns(combined_text, JUNK_BULK_FOOTER_MARKERS),
        "leadgen": _matching_patterns(combined_text, JUNK_LEADGEN_MARKERS),
    }
    url_count = len(re.findall(r"(?:https?://|www\.)", body_text.lower()))
    exclamation_count = f"{title_text} {body_text}".count("!")
    all_caps_word_count = len(re.findall(r"\b[A-Z0-9]{5,}\b", f"{title_text} {body_text}"))

    score = 0
    if family_hits["strong_terms"]:
        score += 5
    if family_hits["money_bait"]:
        score += 3
    if family_hits["promotion_cta"]:
        score += 2
    if family_hits["urgency_pressure"]:
        score += 2
    if family_hits["account_bait"]:
        score += 2
    if family_hits["bulk_footer"]:
        score += 1
    if family_hits["leadgen"]:
        score += 2
    if sender_automated:
        score += 1
    if bulk_signal:
        score += 1
    if url_count >= 2:
        score += 1
    if exclamation_count >= 3:
        score += 1
    if all_caps_word_count >= 2:
        score += 1
    if family_hits["leadgen"] and not sender_personal_domain:
        score += 1

    transactional_like = bool(transactional_hits) and not (
        family_hits["strong_terms"] or family_hits["money_bait"]
    )
    if transactional_like:
        score -= 2
    if sender_personal_domain:
        score -= 1
    score = max(0, score)

    strong = bool(family_hits["strong_terms"]) or (
        score >= 7
        and (
            family_hits["money_bait"]
            or family_hits["account_bait"]
            or family_hits["leadgen"]
        )
    ) or (
        family_hits["account_bait"]
        and family_hits["urgency_pressure"]
        and not sender_personal_domain
        and not transactional_like
    )
    soft = (
        not strong
        and score >= 3
        and (
            family_hits["money_bait"]
            or family_hits["promotion_cta"]
            or family_hits["urgency_pressure"]
            or family_hits["account_bait"]
            or family_hits["leadgen"]
        )
    )
    active_families = [name for name, hits in family_hits.items() if hits]
    if transactional_like:
        active_families.append("transactional_context")
    return {
        "level": "strong" if strong else ("soft" if soft else None),
        "score": score,
        "families": active_families,
        "bulk_signal": bulk_signal,
        "sender_automated": sender_automated,
        "sender_personal_domain": sender_personal_domain,
        "transactional_like": transactional_like,
        "url_count": url_count,
    }


def _junk_signal_block(email_data):
    """Build junk-signal hint block for the classification prompt."""
    assessment = _junk_signal_assessment(email_data)
    families = ", ".join(assessment.get("families") or []) or "(none)"
    return (
        "Junk signal summary:\n"
        f"- automated_sender: {'yes' if assessment.get('sender_automated') else 'no'}\n"
        f"- personal_sender_domain: {'yes' if assessment.get('sender_personal_domain') else 'no'}\n"
        f"- bulk_or_newsletter: {'yes' if assessment.get('bulk_signal') else 'no'}\n"
        f"- transactional_context: {'yes' if assessment.get('transactional_like') else 'no'}\n"
        f"- url_count: {assessment.get('url_count', 0)}\n"
        f"- junk_signal_score: {assessment.get('score', 0)}\n"
        f"- junk_signal_families: {families}\n"
    )


def _looks_probable_junk(email_data):
    """Looks probable junk.
    """
    # Keep this rule in one place so behavior stays consistent.
    return _junk_signal_assessment(email_data).get("level")


def _heuristic_classification(email_data):
    """Heuristic classification.
    """
    # Normalize to the fixed labels used by mailbox triage.
    # Heuristics provide deterministic fallback labels when model output is absent/uncertain.
    actionable = _looks_actionable(email_data)
    bulk_signal = _looks_bulk_or_newsletter(email_data)
    junk_assessment = _junk_signal_assessment(email_data)
    junk_signal = junk_assessment.get("level")
    if junk_signal == "strong":
        return {
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.92,
            "email_type": "junk",
        }

    if junk_signal == "soft":
        return {
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.68,
            "email_type": "junk-uncertain",
        }

    if bulk_signal and not actionable:
        return {
            "category": "informational",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.88,
            "email_type": "read-only",
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

    # Nudge uncertain model outputs toward safer mailbox behavior.
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

    # Enforce category/type consistency so downstream DB writes stay canonical.
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
    """Call Ollama chat API and return response content or None on failure."""
    # Send one non-streaming chat request to Ollama and return the model text payload.
    model_name = _model_name(task=task)
    api_urls = _api_url_candidates()
    request_timeout = _timeout_seconds(task=task)
    _log_action(
        task=task,
        status="call_start",
        email_id=email_id,
        detail=(
            f"ollama_chat model={model_name} timeout={int(request_timeout)}s "
            f"url={api_urls[0]}"
        ),
    )

    if not _endpoint_allowed():
        _log_action(
            task=task,
            status="error",
            email_id=email_id,
            detail=f"Blocked non-local Ollama endpoint: {_api_url()}",
        )
        return None

    payload = {
        "model": model_name,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
        },
    }
    body = json.dumps(payload).encode("utf-8")
    raw = None
    last_error = None
    for candidate_url in api_urls:
        request_obj = urllib.request.Request(
            candidate_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request_obj, timeout=request_timeout) as response:
                raw = response.read().decode("utf-8")
            if candidate_url != api_urls[0]:
                _log_action(
                    task=task,
                    status="fallback",
                    email_id=email_id,
                    detail=f"ollama_endpoint_fallback_success url={candidate_url}",
                )
            break
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            continue

    if raw is None:
        _log_action(task=task, status="error", email_id=email_id, detail=f"request_failed: {last_error}")
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

    # Ask for strict JSON, then combine with deterministic heuristics for stability.
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
        "Permission-based newsletters, digests, and transactional notifications are usually informational "
        "with needs_response=false unless the email explicitly asks the recipient to respond. "
        "But lean toward junk when the email shows several common spam patterns such as aggressive sales CTAs, "
        "urgency/scarcity bait, prize/refund/investment promises, account-verification fear bait from automated "
        "senders, bulk footer language paired with promotional copy, or unsolicited lead-gen outreach. "
        "Generalize from these patterns instead of requiring an exact match to a prior example. "
        "Mark junk only for spam/scam/promotional noise; use lower confidence for borderline cases so uncertain "
        "junk can be confirmed by the user. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Classify this email.\n\n"
        f"{_classification_few_shot_block()}\n\n"
        f"{_sender_hint_block(normalized_email)}\n"
        f"{_junk_signal_block(normalized_email)}"
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
    # Fall back cleanly when the model emits malformed/non-JSON output.
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
    deterministic_bulk_summary = _bulk_newsletter_summary(email_data)
    if deterministic_bulk_summary:
        return _rewrite_summary_for_second_person(deterministic_bulk_summary)
    profile = _summary_profile(email_data)
    title = _compact_text(email_data.get("title") or "")
    fallback_summary = _rewrite_summary_for_second_person(
        _extractive_summary_fallback(email_data)
    ) or None

    # Keep prompts tightly constrained, then post-filter for grounding/paraphrase quality.
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
        "When the email contains multiple sections, features, or story items, mention each major section explicitly. "
        "Ignore newsletter/footer boilerplate like subscriber IDs, preference-management links, "
        "privacy/cookie/legal notices, and utility links unless they are the main request. "
        "Ignore subscription prompts like sign-up or subscribe CTAs unless the email is explicitly about subscribing. "
        "Return plain text only as one compact paragraph (no markdown or bullet points). "
        f"Target {profile['prompt_target']} for long emails; keep shorter emails concise. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Generate an original condensed summary in your own words. "
        "Do not copy or closely quote the email body. "
        "Only include claims that are explicitly supported by the email content. "
        "For long emails, cover all major sections instead of summarizing only the opening lines. "
        "If the email has several distinct updates, features, or sections, include each one at least briefly. "
        "Focus on what happened, key facts, and what you need to do next.\n\n"
        f"{_email_context_block(
            email_data,
            body_max_chars=profile['context_chars'],
            body_max_sentences=profile['context_sentences'],
        )}"
    )
    response_text = _call_ollama(
        task="summarize",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.0,
        num_predict=profile["num_predict"],
    )
    # Prefer deterministic fallback whenever model output is missing or unusable.
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
    summary = _merge_summary_with_fallback_coverage(summary, email_data)
    if len(summary) > profile["char_limit"]:
        summary = f"{summary[: profile['char_limit'] - 3]}..."
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


def _reply_guidance_block(email_data):
    """Build compact reply guidance from the incoming email."""
    title = _compact_text(email_data.get("title") or "(No subject)")
    request_sentence = _first_request_sentence(email_data)
    key_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=4)
    detail_lines = []
    if request_sentence:
        detail_lines.append(request_sentence)
    for sentence in key_sentences:
        if request_sentence and _token_overlap_ratio(sentence, request_sentence) > 0.9:
            continue
        detail_lines.append(sentence)
        if len(detail_lines) >= 3:
            break
    details = "\n".join(f"- {detail}" for detail in detail_lines) or "- (none)"
    response_mode = "answer_or_confirm" if _looks_actionable(email_data) else "acknowledge_only"
    return (
        "Reply guidance:\n"
        f"- response_mode: {response_mode}\n"
        f"- subject_topic: {title}\n"
        f"- likely_sender_request: {request_sentence or '(none explicit)'}\n"
        f"- concrete_details_to_address:\n{details}\n"
    )


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
            _ensure_sentence_ending(
                f"I'm following up on {request_sentence or additional_detail or topic}"
            ),
            "I'll review the details and send you a specific response as soon as I can.",
        ]
        body_parts.append(
            "If there is a specific deadline or format you want, please let me know."
        )
        body_text = " ".join(body_parts)
    else:
        body_parts = [
            f"Thanks for sharing the update about {topic}.",
            "I appreciate the update.",
        ]
        body_parts.append("Let me know if you want any follow-up action from my side.")
        body_text = " ".join(body_parts)

    return f"{greeting}\n\n{body_text}\n\nBest regards,"


def _sanitize_reply_output(draft_text):
    """Remove placeholder signature artifacts from model-generated replies."""
    text = str(draft_text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return ""
    lines = text.split("\n")
    placeholder_patterns = (
        r"^\[?\s*your name\s*\]?$",
        r"^\[?\s*i would include your name here if applicable\s*\]?$",
        r"^\[?\s*insert your name\s*\]?$",
    )
    while lines and not lines[-1].strip():
        lines.pop()
    while lines and any(
        re.match(pattern, lines[-1].strip(), flags=re.IGNORECASE)
        for pattern in placeholder_patterns
    ):
        lines.pop()
        while lines and not lines[-1].strip():
            lines.pop()
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


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
    intro = lowered[:240]
    if any(marker in intro for marker in DRAFT_OBSERVER_MARKERS):
        return True
    if any(marker in lowered for marker in ("the user", "the recipient", "mailbox owner")):
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
            draft_tokens = set(_text_tokens(cleaned))
            source_tokens = set(_text_tokens(source))
            novel_tokens = draft_tokens - source_tokens
            if len(novel_tokens) <= 2:
                return True
    return False


def _draft_specificity_metrics(draft_text, email_data):
    """Return overlap metrics between a draft and email-specific details."""
    title = _compact_text(email_data.get("title") or "")
    request_sentence = _first_request_sentence(email_data) or ""
    detail_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=4)
    token_exclusions = {
        "thanks",
        "thank",
        "hello",
        "regards",
        "best",
        "dear",
        "hi",
        "please",
        "email",
        "message",
    }

    def _filtered_tokens(value):
        return {
            token
            for token in _content_tokens(value)
            if len(token) >= 4 and token not in token_exclusions
        }

    draft_tokens = _filtered_tokens(draft_text)
    title_tokens = _filtered_tokens(title)
    request_tokens = _filtered_tokens(request_sentence)
    detail_tokens = set()
    for sentence in detail_sentences:
        detail_tokens.update(_filtered_tokens(sentence))
    reference_tokens = set(title_tokens) | set(request_tokens) | set(detail_tokens)
    return {
        "draft_tokens": draft_tokens,
        "title_overlap": len(draft_tokens & title_tokens),
        "request_overlap": len(draft_tokens & request_tokens),
        "detail_overlap": len(draft_tokens & reference_tokens),
    }


def _looks_generic_draft(draft_text, email_data):
    """Return True when a draft is overly generic for the source email."""
    cleaned = _compact_text(draft_text)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    has_generic_marker = any(marker in lowered for marker in DRAFT_GENERIC_MARKERS)
    metrics = _draft_specificity_metrics(cleaned, email_data)
    actionable = _looks_actionable(email_data)

    if actionable:
        if has_generic_marker and metrics["request_overlap"] < 2:
            return True
        if any(
            marker in lowered
            for marker in (
                "follow up shortly",
                "get back to you",
                "send you a specific response",
                "send a complete response",
            )
        ) and metrics["request_overlap"] < 3:
            return True
        if len(cleaned) < 220 and metrics["detail_overlap"] < 2:
            return True
        return False

    if has_generic_marker and metrics["detail_overlap"] < 2:
        return True
    return False


def _rewrite_generic_draft(
    draft_text,
    email_data,
    to_value="",
    cc_value="",
    email_id=None,
):
    """Rewrite a weak draft into a more concrete, email-specific reply."""
    candidate = str(draft_text or "").strip()
    if not candidate:
        return None

    system_prompt = (
        "You rewrite weak email replies into strong, send-ready replies grounded in the incoming email. "
        "Write as the mailbox owner (use I/we), not as an observer. "
        "Keep only details supported by the email context. "
        "If the sender asks for something, address that request directly and mention concrete details from the email "
        "such as the topic, deliverable, date, deadline, question, or requested action. "
        "Avoid filler like 'thanks for the update' or 'I'll get back to you later' unless there is no better "
        "grounded response. "
        "If exact facts needed for a full answer are missing, give the most plausible professional next step and ask "
        "at most one focused clarifying question. "
        "Do not invent the mailbox owner's name, title, or signature. "
        "Return only the email body text with greeting and closing, no markdown and no subject."
    )
    user_prompt = (
        "Rewrite this weak draft so it directly responds to the email.\n\n"
        f"Weak draft:\n{candidate}\n\n"
        f"{_reply_guidance_block(email_data)}\n"
        f"{_email_context_block(email_data)}\n\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Make it specific, plausible, and ready to send."
    )
    response_text = _call_ollama(
        task="draft_rewrite",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=650,
    )
    if not response_text:
        return None

    cleaned = _sanitize_reply_output(response_text)
    if _looks_draft_failure(cleaned, email_data):
        return None
    if _looks_generic_draft(cleaned, email_data):
        return None
    return cleaned


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


def _brief_reply_intent(text):
    """Return coarse intent label for very short user-entered draft instructions."""
    normalized = _compact_text(text).lower()
    if not normalized:
        return None
    if any(
        marker in normalized
        for marker in ("wrong email", "wrong person", "wrong recipient", "remove me", "unsubscribe me")
    ):
        return "wrong_recipient"
    if normalized in {"no", "nope", "nah", "not interested"} or any(
        marker in normalized for marker in ("don't", "do not", "can't", "cannot", "won't", "no thanks")
    ):
        return "decline"
    if normalized in {"yes", "yep", "sure", "ok", "okay"} or any(
        marker in normalized for marker in ("sounds good", "works for me", "that works", "i can do", "confirmed")
    ):
        return "accept"
    if any(marker in normalized for marker in ("thanks", "thank you", "thx")):
        return "thanks"
    return None


def _is_brief_reply_instruction(text):
    """Return True when the current draft is really a short instruction/stub."""
    normalized = _compact_text(text)
    tokens = _text_tokens(normalized)
    return bool(normalized) and (len(tokens) <= 5 or len(normalized) <= 28)


def _draft_matches_brief_intent(original_text, revised_text):
    """Return True when a rewritten reply preserves a short user instruction's stance."""
    intent = _brief_reply_intent(original_text)
    lowered = _compact_text(revised_text).lower()
    if not intent or not lowered:
        return False
    if intent == "wrong_recipient":
        return any(
            marker in lowered
            for marker in ("wrong email", "wrong person", "wrong recipient", "remove me", "not the right contact")
        )
    if intent == "decline":
        return any(
            marker in lowered
            for marker in ("can't", "cannot", "won't", "no thanks", "not interested", "doesn't work")
        )
    if intent == "accept":
        return any(
            marker in lowered
            for marker in ("works for me", "sounds good", "confirmed", "happy to", "yes")
        )
    if intent == "thanks":
        return any(marker in lowered for marker in ("thanks", "thank you", "appreciate"))
    return False


def _extract_deadline_phrase(text):
    """Return a short deadline/timing phrase when one is clearly present."""
    normalized = _compact_text(text)
    if not normalized:
        return ""
    patterns = (
        r"\bby [^.?!,;]+",
        r"\bbefore [^.?!,;]+",
        r"\b(?:today|tomorrow|tonight|this week|next week|end of day|eod)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        phrase = _compact_text(match.group(0)).strip(" .,:;")
        phrase = re.sub(r"\s+(?:so|because|and)\b.*$", "", phrase, flags=re.IGNORECASE)
        if phrase:
            return phrase
    return ""


def _expand_brief_reply_instruction(email_data, current_draft_text):
    """Expand short reply instructions into a send-ready draft."""
    intent = _brief_reply_intent(current_draft_text)
    if not intent:
        return None

    sender_name = _sender_display_name(email_data.get("sender"))
    greeting = f"Hi {sender_name}," if sender_name else "Hi,"
    title = _summary_title_topic(email_data.get("title") or "")
    request_sentence = _compact_text(_first_request_sentence(email_data) or "")
    deadline = _extract_deadline_phrase(
        request_sentence or _body_for_context(email_data, max_chars=600, max_sentences=4)
    )
    actionable = _looks_actionable(email_data)

    if intent == "wrong_recipient":
        lines = [
            "I think this reached the wrong person.",
            "Please remove me from this thread.",
        ]
    elif intent == "decline":
        if actionable:
            if deadline:
                lines = [
                    f"That won't work for me. I won't be able to take care of this {deadline}.",
                    "Please plan without me for now.",
                ]
            elif title:
                lines = [
                    f"That won't work for me. I won't be able to take care of the request about {title}.",
                    "Please plan without me for now.",
                ]
            else:
                lines = [
                    "That won't work for me. I won't be able to help with that request.",
                    "Please plan without me for now.",
                ]
        else:
            lines = ["No thanks."]
            if _looks_bulk_or_newsletter(email_data):
                lines.append("Please remove me from this list.")
    elif intent == "accept":
        if actionable:
            if deadline:
                lines = [
                    f"Sounds good. I'll take care of this {deadline}.",
                    "I'll let you know right away if anything changes.",
                ]
            elif title:
                lines = [
                    f"Sounds good. I'll take care of the request about {title}.",
                    "I'll let you know right away if anything changes.",
                ]
            else:
                lines = [
                    "Sounds good. I'll take care of it.",
                    "I'll let you know right away if anything changes.",
                ]
        else:
            lines = [f"Sounds good. Thanks for the update about {title}." if title else "Sounds good. Thanks for the update."]
    elif intent == "thanks":
        lines = [f"Thanks for the update about {title}." if title else "Thanks for the update."]
        lines.append("I appreciate it.")
    else:
        return None

    body = " ".join(
        _ensure_sentence_ending(line)
        for line in lines
        if _compact_text(line)
    )
    if not body:
        return None
    return f"{greeting}\n\n{body}\n\nBest regards,"


def _draft_preserves_user_context(original_text, revised_text):
    """Return True when revised draft still reflects user-provided context."""
    # Keep revise behavior anchored to the user's current reply box content.
    original = _compact_text(original_text).lower()
    revised = _compact_text(revised_text).lower()
    if not original or not revised:
        return False
    if original in revised:
        return True

    original_tokens = set(_text_tokens(original))
    revised_tokens = set(_text_tokens(revised))
    if not original_tokens or not revised_tokens:
        return False
    overlap = len(original_tokens & revised_tokens) / float(len(original_tokens))

    # Short user drafts need lighter overlap checks; long drafts should retain more substance.
    if len(original_tokens) <= 12:
        return overlap >= 0.24
    return overlap >= 0.34


def can_generate_reply_draft(email_data, current_draft_text=""):
    """Return True when AI should generate or revise a reply draft."""
    if str(current_draft_text or "").strip():
        return True
    if not isinstance(email_data, dict):
        return False

    email_type = str(email_data.get("type") or "").strip().lower()
    if email_type == "response-needed":
        return True

    ai_needs_response = email_data.get("ai_needs_response")
    if ai_needs_response is not None:
        return bool(ai_needs_response)

    if str(email_data.get("ai_category") or "").strip():
        return False

    return _looks_actionable(email_data)


def draft_reply(email_data, to_value="", cc_value="", email_id=None):
    """Draft reply.
    """
    # Generate, revise, or validate draft reply used by reply and draft workflows.
    body = (email_data.get("body") or "").strip()
    title = _compact_text(email_data.get("title") or "(No subject)")
    if not body and not title:
        return None

    # Generate a complete send-ready draft; fallback template keeps UX functional on failure.
    system_prompt = (
        "You write high-quality professional email replies grounded in the incoming email context. "
        "Write as the mailbox owner (use I/we), not as an observer. "
        "Do not refer to the mailbox owner as 'the user'. "
        "Write the actual reply, not an analysis, summary, or explanation of the reply. "
        "Do not open with phrases like 'the main points I found were', 'here are the key points', "
        "'the sender is asking', or 'here is a draft'. "
        "Start directly with the email greeting and body as if it will be sent. "
        "Do not copy long spans from the original email. "
        "If the message is actionable, address the sender's request directly when possible. "
        "Use at least one concrete detail from the subject or body in the reply body, not only in the greeting. "
        "Avoid vague filler like 'thanks for the update' or 'I'll get back to you later' unless there is no better "
        "grounded response. "
        "If exact facts needed for a full answer are missing, give the most plausible professional next step and ask "
        "at most one focused clarifying question. "
        "If the message is informational/newsletter with no explicit ask, write a concise acknowledgment. "
        "Do not invent the mailbox owner's name, title, or signature. "
        "If no name is provided, end with a generic closing such as Best regards, without a personal name. "
        "Default to a complete response with greeting, substantive body, and closing sign-off. "
        "Return only the email body text, no markdown and no subject. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Draft a complete response email.\n\n"
        f"{_reply_guidance_block(email_data)}\n"
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

    cleaned = _sanitize_reply_output(response_text)
    if _looks_generic_draft(cleaned, email_data):
        rewritten = _rewrite_generic_draft(
            cleaned,
            email_data=email_data,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        if rewritten:
            return rewritten
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

    # Preserve user-provided intent while improving clarity/completeness against source context.
    system_prompt = (
        "You revise email drafts using the incoming email context. "
        "Write as the mailbox owner (use I/we), not as an observer. "
        "Never refer to the mailbox owner as 'the user'. "
        "Return the final send-ready email itself, not commentary about the draft. "
        "Do not add analysis phrases like 'the main points I found were', 'here are the key points', "
        "'the sender is asking', or 'here is a revised draft'. "
        "Treat the current draft as required user context: preserve its concrete points and intent. "
        "If the current draft is only a short instruction like 'nope', 'sounds good', 'thanks', or "
        "'wrong email', treat it as the user's intended stance and expand it into a complete reply. "
        "Do not discard user-provided constraints, commitments, or questions. "
        "Preserve the original intent, but expand vague or very short drafts into complete replies. "
        "Include concrete details and next steps when the original email asks for action. "
        "Use at least one concrete detail from the subject or body in the reply body. "
        "Avoid vague filler like 'thanks for the update' or 'I'll get back to you later' unless there is no better "
        "grounded response. "
        "Do not copy long spans from the original email. "
        "Do not invent the mailbox owner's name, title, or signature. "
        "If no name is provided, use a generic closing without adding a personal name. "
        "Return only the revised email body text with greeting and closing, no markdown and no subject. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Revise this draft response based on the original email.\n\n"
        f"{_reply_guidance_block(email_data)}\n"
        f"{_email_context_block(email_data)}\n\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Current draft (between markers):\n"
        "---BEGIN CURRENT DRAFT---\n"
        f"{current_draft_text}\n"
        "---END CURRENT DRAFT---\n\n"
        "Keep intent, improve clarity, and make it complete enough to send. "
        "When the current draft is very short, treat it as instructions for the reply rather than final wording."
    )
    response_text = _call_ollama(
        task="revise",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.1,
        num_predict=650,
    )
    if not response_text:
        if _is_brief_reply_instruction(current_draft_text):
            expanded = _expand_brief_reply_instruction(email_data, current_draft_text)
            if expanded:
                return expanded
        # Never discard user edits when the model fails or times out.
        _log_action(
            task="revise",
            status="fallback",
            email_id=email_id,
            detail="keeping_user_draft_no_model_response",
        )
        return current_draft_text
    cleaned = _sanitize_reply_output(response_text)
    if _looks_generic_draft(cleaned, email_data):
        rewritten = _rewrite_generic_draft(
            cleaned,
            email_data=email_data,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        if rewritten and not _drafts_too_similar(current_draft_text, rewritten):
            cleaned = rewritten
    if _looks_draft_failure(cleaned, email_data):
        # Recover from unusable revisions by forcing a fresh draft generation pass.
        regenerated = draft_reply(
            email_data=email_data,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        regenerated = str(regenerated or "").strip()
        if (
            regenerated
            and not _drafts_too_similar(current_draft_text, regenerated)
            and _draft_preserves_user_context(current_draft_text, regenerated)
            and not _looks_generic_draft(regenerated, email_data)
        ):
            return regenerated
        if _is_brief_reply_instruction(current_draft_text):
            expanded = _expand_brief_reply_instruction(email_data, current_draft_text)
            if expanded:
                return expanded
        _log_action(
            task="revise",
            status="fallback",
            email_id=email_id,
            detail="keeping_user_draft_unusable_model_output",
        )
        return current_draft_text
    if not _draft_preserves_user_context(current_draft_text, cleaned):
        if _is_brief_reply_instruction(current_draft_text) and _draft_matches_brief_intent(
            current_draft_text,
            cleaned,
        ):
            return cleaned or current_draft_text
        if _is_brief_reply_instruction(current_draft_text):
            expanded = _expand_brief_reply_instruction(email_data, current_draft_text)
            if expanded:
                return expanded
        # Never replace user-provided context with unrelated model content.
        return current_draft_text
    if _drafts_too_similar(current_draft_text, cleaned):
        # Near-identical revisions are still acceptable if they preserve context.
        return cleaned or current_draft_text
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
    if not can_generate_reply_draft(email_data, current_draft_text=current_draft_text):
        return current_draft_text
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
    raw_summary = str(email_data.get("summary") or "")
    summary = " ".join(raw_summary.split()).lower().strip()
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
    if _looks_utility_sentence(raw_summary):
        return True
    if _is_noise_fragment(raw_summary):
        return True
    if "\n" in raw_summary or "\r" in raw_summary:
        return True
    if _looks_summary_parrot(raw_summary, email_data):
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
# In-memory async task registry used by polling endpoints; kept bounded by cleanup.
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

        if not can_generate_reply_draft(email_data, current_draft_text=current_reply_text):
            raise ValueError("AI draft is only available for emails that need a response.")

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
