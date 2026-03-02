import ipaddress
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from .debug_logger import log_event

OLLAMA_API_URL_DEFAULT = "http://localhost:11434/api/chat"
OLLAMA_MODEL_DEFAULT = "qwen2.5:7b"
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
    "unsubscribe",
    "privacy policy",
    "terms of service",
    "terms of use",
    "all rights reserved",
    "you are receiving this email",
    "this email was sent to",
    "add us to your address book",
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


def _utc_now():
    """Utc now.
    """
    # Internal helper for utc now used by higher-level request and sync workflows.
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _action_log_path():
    """Action log path.
    """
    # Write action log path details in the app's structured log format for debugging and traceability.
    configured = (os.getenv("AI_ACTION_LOG_PATH") or "").strip()
    if configured:
        return Path(configured)
    return Path("instance/ai_actions.txt")


def _one_line(value):
    """One line.
    """
    # Internal helper for one line used by higher-level request and sync workflows.
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
    # Write log ai event details in the app's structured log format for debugging and traceability.
    _log_action(task=task, status=status, email_id=email_id, detail=detail)


def _api_url():
    """Api url.
    """
    # Resolve api url using configuration defaults and safe fallback behavior.
    value = (os.getenv("OLLAMA_API_URL") or OLLAMA_API_URL_DEFAULT).strip()
    return value or OLLAMA_API_URL_DEFAULT


def _model_name():
    """Model name.
    """
    # Internal helper for model name used by higher-level request and sync workflows.
    value = (os.getenv("OLLAMA_MODEL") or OLLAMA_MODEL_DEFAULT).strip()
    return value or OLLAMA_MODEL_DEFAULT


def _timeout_seconds():
    """Timeout seconds.
    """
    # Internal helper for timeout seconds used by higher-level request and sync workflows.
    raw = (os.getenv("OLLAMA_TIMEOUT_SECONDS") or "").strip()
    try:
        parsed = float(raw) if raw else float(OLLAMA_TIMEOUT_SECONDS_DEFAULT)
    except ValueError:
        parsed = float(OLLAMA_TIMEOUT_SECONDS_DEFAULT)
    return max(1.0, min(60.0, parsed))


def _summary_min_chars():
    """Summary min chars.
    """
    # Internal helper for summary min chars used by higher-level request and sync workflows.
    raw = (os.getenv("OLLAMA_SUMMARY_MIN_CHARS") or "").strip()
    try:
        parsed = int(raw) if raw else SUMMARY_MIN_CHARS_DEFAULT
    except ValueError:
        parsed = SUMMARY_MIN_CHARS_DEFAULT
    return max(50, min(5000, parsed))


def _is_loopback_host(hostname):
    """Return whether loopback host.
    """
    # Internal helper for is loopback host used by higher-level request and sync workflows.
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
    # Internal helper for endpoint allowed used by higher-level request and sync workflows.
    parsed = urlparse(_api_url())
    if parsed.scheme != "http":
        return False
    return _is_loopback_host(parsed.hostname)


def ai_enabled():
    """Ai enabled.
    """
    # Internal helper for ai enabled used by higher-level request and sync workflows.
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
    # Normalize classification to email type into constrained labels used by mailbox triage logic.
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
    # Internal helper for compact text used by higher-level request and sync workflows.
    return " ".join(str(value or "").split()).strip()


def _text_tokens(value):
    """Text tokens.
    """
    # Internal helper for text tokens used by higher-level request and sync workflows.
    text = str(value or "").lower()
    return [token for token in re.findall(r"[a-z0-9']+", text) if len(token) > 2]


def _token_overlap_ratio(left, right):
    """Token overlap ratio.
    """
    # Internal helper for token overlap ratio used by higher-level request and sync workflows.
    left_tokens = set(_text_tokens(left))
    right_tokens = set(_text_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / float(min(len(left_tokens), len(right_tokens)))


def _is_noise_fragment(text):
    """Return whether noise fragment.
    """
    # Internal helper for is noise fragment used by higher-level request and sync workflows.
    normalized = _compact_text(text).lower()
    if not normalized or normalized == "[link]":
        return True
    return any(marker in normalized for marker in SUMMARY_NOISE_MARKERS)


def _is_near_subject_copy(candidate_text, title_text):
    """Return whether near subject copy.
    """
    # Internal helper for is near subject copy used by higher-level request and sync workflows.
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
    # Sanitize clean body for prompt so downstream code receives safe, normalized text values.
    text = str(body or "").replace("\r\n", "\n").replace("\r", "\n")
    text = _strip_reply_chain(text)
    cleaned_lines = []
    for line in text.split("\n"):
        trimmed = line.strip()
        if not trimmed:
            cleaned_lines.append("")
            continue
        if trimmed.startswith(">"):
            continue
        normalized_line = re.sub(r"https?://\S+", "[link]", trimmed)
        cleaned_lines.append(normalized_line)

    cleaned = "\n".join(cleaned_lines)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()[:max_chars]


def _extract_key_sentences(body_text, max_sentences=8):
    """Extract key sentences.
    """
    # Extract key sentences from provider/user payloads while handling missing fields safely.
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
        parts = re.split(r";\s+|\.\s+", flattened)

    selected = []
    for part in parts:
        sentence = _compact_text(part).strip(" -:")
        if len(sentence) < 24:
            continue
        for marker in SUMMARY_NOISE_MARKERS:
            sentence = re.sub(re.escape(marker), " ", sentence, flags=re.IGNORECASE)
        sentence = _compact_text(sentence).strip(" -:")
        if len(sentence) < 24:
            continue
        lowered = sentence.lower()
        if lowered.startswith(("from:", "to:", "cc:", "bcc:", "sent:", "date:", "subject:")):
            continue
        if _is_noise_fragment(sentence):
            continue
        if any(_token_overlap_ratio(sentence, existing) > 0.94 for existing in selected):
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
    text = _compact_text(text)
    if len(text) > max_chars:
        return f"{text[: max_chars - 3].rstrip()}..."
    return text


def _looks_summary_failure(summary_text):
    """Looks summary failure.
    """
    # Keep this decision logic centralized for predictable control flow.
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


def _extractive_summary_fallback(email_data):
    """Extractive summary fallback.
    """
    # Internal helper for extractive summary fallback used by higher-level request and sync workflows.
    title = _compact_text(email_data.get("title") or "")
    candidate_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=4)
    if not candidate_sentences:
        fallback_body = _body_for_context(email_data, max_chars=2400)
        if not fallback_body:
            return title if title and title.lower() != "(no subject)" else None
        candidate_sentences = [fallback_body]

    selected = []
    for sentence in candidate_sentences:
        if _is_near_subject_copy(sentence, title):
            continue
        selected.append(sentence)
        if len(" ".join(selected)) >= 220 or len(selected) >= 2:
            break
    if not selected:
        selected = candidate_sentences[:2]

    summary = _compact_text(" ".join(selected))
    if _is_noise_fragment(summary):
        return title if title and title.lower() != "(no subject)" else None
    if len(summary) > 280:
        summary = f"{summary[:277]}..."
    return summary or None


def _normalized_email_for_classification(email_data):
    """Normalized email for classification.
    """
    # Normalize normalized email for classification into constrained labels used by mailbox triage logic.
    return {
        "title": _compact_text(email_data.get("title") or "(No subject)"),
        "sender": _compact_text(email_data.get("sender")),
        "recipients": _compact_text(email_data.get("recipients")),
        "cc": _compact_text(email_data.get("cc")),
        "body": _clean_body_for_prompt(email_data.get("body") or ""),
    }


def _profile_prompt_block(user_profile):
    """Profile prompt block.
    """
    # Build profile prompt block text that is passed into model prompts.
    if not isinstance(user_profile, dict):
        return ""
    name_value = _compact_text(user_profile.get("name"))
    occupation_value = _compact_text(user_profile.get("occupation"))
    lines = []
    if name_value:
        lines.append(f"User name: {name_value}")
    if occupation_value:
        lines.append(f"User occupation: {occupation_value}")
    if not lines:
        return ""
    return "User profile context:\n" + "\n".join(lines) + "\n\n"


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
    email_match = re.search(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})", raw)
    if email_match:
        email = email_match.group(1)
    else:
        bracket_match = re.search(r"<([^>]+)>", raw)
        if bracket_match:
            email = _compact_text(bracket_match.group(1)).lower()
    if not email:
        email = raw

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
    # Internal helper for sender address used by higher-level request and sync workflows.
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
    # Internal helper for sender looks automated used by higher-level request and sync workflows.
    if not isinstance(sender_info, dict):
        return False
    identity = sender_info.get("identity", "")
    return _has_any_pattern(identity, AUTOMATED_SENDER_MARKERS)


def _sender_uses_personal_domain(sender_info):
    """Sender uses personal domain.
    """
    # Internal helper for sender uses personal domain used by higher-level request and sync workflows.
    if not isinstance(sender_info, dict):
        return False
    domain = str(sender_info.get("domain") or "").strip().lower()
    if not domain:
        return False
    return domain in PERSONAL_EMAIL_DOMAINS


def _sender_hint_block(email_data):
    """Sender hint block.
    """
    # Internal helper for sender hint block used by higher-level request and sync workflows.
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
    # Normalize classification few shot block into constrained labels used by mailbox triage logic.
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
    # Keep this decision logic centralized for predictable control flow.
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
    # Keep this decision logic centralized for predictable control flow.
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
    # Normalize heuristic classification into constrained labels used by mailbox triage logic.
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
    # Extract json block from provider/user payloads while handling missing fields safely.
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
    # Parse raw bool input into validated values for downstream logic.
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
    # Normalize classification into a canonical value used across the app.
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


def classify_email(email_data, user_profile=None, email_id=None):
    """Classify email.
    """
    # Normalize classify email into constrained labels used by mailbox triage logic.
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
        "subject is third, user profile is fourth. "
        "If body and sender conflict weakly, give sender extra weight unless body has a direct response request. "
        "Infer whether the sender appears to be a brand/company/news source or an individual from sender "
        "name/domain using your general knowledge and context in this email. "
        "Do not rely on a predefined hardcoded brand list. "
        "Use strict policy: newsletters/digests/promotions/notifications are usually informational "
        "with needs_response=false unless the email explicitly asks the recipient to respond. "
        "Mark junk only for spam/scam/promotional noise; use lower confidence for borderline cases."
    )
    user_prompt = (
        "Classify this email.\n\n"
        f"{_profile_prompt_block(user_profile)}"
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


def summarize_email(email_data, user_profile=None, email_id=None):
    """Summarize email.
    """
    # Transform summarize email data between provider payloads and local mailbox records.
    if not should_summarize_email(email_data):
        return None
    title = _compact_text(email_data.get("title") or "")
    fallback_summary = _extractive_summary_fallback(email_data)

    system_prompt = (
        "Summarize emails. Return plain text only. "
        "Maximum 2 sentences and under 280 characters."
    )
    user_prompt = (
        "Summarize this email.\n\n"
        f"{_profile_prompt_block(user_profile)}"
        f"{_email_context_block(email_data)}"
    )
    response_text = _call_ollama(
        task="summarize",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.1,
        num_predict=200,
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

    summary = _compact_text(response_text)
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
    if len(summary) > 280:
        summary = f"{summary[:277]}..."
    return summary or fallback_summary


def _sender_display_name(sender_text):
    """Extract a readable sender name from the raw sender field."""
    # Internal helper for sender display name used by higher-level request and sync workflows.
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
    # Internal helper for first request sentence used by higher-level request and sync workflows.
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
        key_sentences = _extract_key_sentences(email_data.get("body") or "", max_sentences=1)
        topic = key_sentences[0] if key_sentences else "your message"
    if len(topic) > 110:
        topic = f"{topic[:107].rstrip()}..."

    request_sentence = _first_request_sentence(email_data)
    if request_sentence and len(request_sentence) > 150:
        request_sentence = f"{request_sentence[:147].rstrip()}..."

    if _looks_actionable(email_data):
        if request_sentence:
            body_text = (
                f"Thanks for your email about {topic}. "
                f"I reviewed your request: {request_sentence} "
                "I will follow up shortly with next steps."
            )
        else:
            body_text = (
                f"Thanks for your email about {topic}. "
                "I reviewed the details and will follow up shortly with next steps."
            )
    else:
        body_text = (
            f"Thanks for sharing the update about {topic}. "
            "I reviewed it and appreciate the context."
        )

    return f"{greeting}\n\n{body_text}\n\nBest regards,"


def _looks_draft_failure(draft_text, email_data):
    """Detect placeholder or low-quality drafts that should be replaced."""
    # Keep this decision logic centralized for predictable control flow.
    cleaned = _compact_text(draft_text)
    if not cleaned:
        return True
    lowered = cleaned.lower()
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


def draft_reply(email_data, to_value="", cc_value="", user_profile=None, email_id=None):
    """Draft reply.
    """
    # Generate, revise, or validate draft reply used by reply and draft workflows.
    body = (email_data.get("body") or "").strip()
    title = _compact_text(email_data.get("title") or "(No subject)")
    if not body and not title:
        return None

    system_prompt = (
        "You write concise, professional email replies grounded in the incoming email context. "
        "Do not copy long spans from the original email. "
        "If the message is informational/newsletter with no explicit ask, write a short acknowledgment. "
        "Return only the email body text, no markdown and no subject."
    )
    user_prompt = (
        "Draft a response email.\n\n"
        f"{_profile_prompt_block(user_profile)}"
        f"{_email_context_block(email_data)}\n\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Keep it clear and actionable."
    )
    response_text = _call_ollama(
        task="draft",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.35,
        num_predict=420,
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
    user_profile=None,
    email_id=None,
):
    """Revise reply.
    """
    # Generate, revise, or validate revise reply used by reply and draft workflows.
    current_draft_text = str(current_draft_text or "").strip()
    if not current_draft_text:
        return None

    system_prompt = (
        "You improve email drafts using the incoming email context. "
        "Do not copy long spans from the original email. "
        "Return only the revised email body text, no markdown and no subject."
    )
    user_prompt = (
        "Revise this draft response based on the original email.\n\n"
        f"{_profile_prompt_block(user_profile)}"
        f"{_email_context_block(email_data)}\n\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Current draft:\n"
        f"{current_draft_text}\n\n"
        "Keep intent, improve clarity, and keep it concise."
    )
    response_text = _call_ollama(
        task="revise",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=420,
    )
    if not response_text:
        return current_draft_text
    cleaned = str(response_text or "").strip()
    if _looks_draft_failure(cleaned, email_data):
        return current_draft_text
    return cleaned or current_draft_text
