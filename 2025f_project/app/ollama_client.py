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


def _utc_now():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _action_log_path():
    configured = (os.getenv("AI_ACTION_LOG_PATH") or "").strip()
    if configured:
        return Path(configured)
    return Path("instance/ai_actions.txt")


def _one_line(value):
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return text[:500]


def _log_action(task, status, email_id=None, detail=""):
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
    _log_action(task=task, status=status, email_id=email_id, detail=detail)


def _api_url():
    value = (os.getenv("OLLAMA_API_URL") or OLLAMA_API_URL_DEFAULT).strip()
    return value or OLLAMA_API_URL_DEFAULT


def _model_name():
    value = (os.getenv("OLLAMA_MODEL") or OLLAMA_MODEL_DEFAULT).strip()
    return value or OLLAMA_MODEL_DEFAULT


def _timeout_seconds():
    raw = (os.getenv("OLLAMA_TIMEOUT_SECONDS") or "").strip()
    try:
        parsed = float(raw) if raw else float(OLLAMA_TIMEOUT_SECONDS_DEFAULT)
    except ValueError:
        parsed = float(OLLAMA_TIMEOUT_SECONDS_DEFAULT)
    return max(1.0, min(60.0, parsed))


def _summary_min_chars():
    raw = (os.getenv("OLLAMA_SUMMARY_MIN_CHARS") or "").strip()
    try:
        parsed = int(raw) if raw else SUMMARY_MIN_CHARS_DEFAULT
    except ValueError:
        parsed = SUMMARY_MIN_CHARS_DEFAULT
    return max(50, min(5000, parsed))


def _is_loopback_host(hostname):
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
    parsed = urlparse(_api_url())
    if parsed.scheme != "http":
        return False
    return _is_loopback_host(parsed.hostname)


def ai_enabled():
    return _endpoint_allowed()


def should_summarize_email(email_data):
    body = (email_data.get("body") or "").strip()
    return len(body) >= _summary_min_chars()


def classification_to_email_type(classification):
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
    return " ".join(str(value or "").split()).strip()


def _strip_reply_chain(text):
    content = str(text or "")
    cut_positions = []
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


def _normalized_email_for_classification(email_data):
    return {
        "title": _compact_text(email_data.get("title") or "(No subject)"),
        "sender": _compact_text(email_data.get("sender")),
        "recipients": _compact_text(email_data.get("recipients")),
        "cc": _compact_text(email_data.get("cc")),
        "body": _clean_body_for_prompt(email_data.get("body") or ""),
    }


def _profile_prompt_block(user_profile):
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
    title = _compact_text(email_data.get("title") or "(No subject)")
    sender = _compact_text(email_data.get("sender"))
    recipients = _compact_text(email_data.get("recipients"))
    cc = _compact_text(email_data.get("cc"))
    body = (email_data.get("body") or "").strip()[:8000]
    return (
        f"Subject: {title}\n"
        f"From: {sender}\n"
        f"To: {recipients}\n"
        f"Cc: {cc}\n"
        f"Body:\n{body}"
    )


def _sender_parts(sender_text):
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
    return _sender_parts(sender_text).get("email", "")


def _has_any_pattern(text, patterns):
    value = str(text or "").lower()
    return any(pattern in value for pattern in patterns)


def _sender_looks_automated(sender_info):
    if not isinstance(sender_info, dict):
        return False
    identity = sender_info.get("identity", "")
    return _has_any_pattern(identity, AUTOMATED_SENDER_MARKERS)


def _sender_uses_personal_domain(sender_info):
    if not isinstance(sender_info, dict):
        return False
    domain = str(sender_info.get("domain") or "").strip().lower()
    if not domain:
        return False
    return domain in PERSONAL_EMAIL_DOMAINS


def _sender_hint_block(email_data):
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
    if _looks_bulk_or_newsletter(email_data) and not _has_any_pattern(
        combined, response_markers
    ):
        return False
    if explicit_question and _has_any_pattern(combined, direct_question_markers):
        return True
    return _has_any_pattern(combined, response_markers)


def _looks_probable_junk(email_data):
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
        if not model_is_junk and model_confidence < 0.85:
            merged = dict(heuristic_classification)

    if str(merged.get("category") or "").strip().lower() == "junk":
        if model_confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
            merged["email_type"] = "junk-uncertain"
        else:
            merged["email_type"] = "junk"

    return merged


def _extract_json_block(text):
    stripped = str(text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    fenced_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", stripped)
    if fenced_match:
        return fenced_match.group(1).strip()
    match = re.search(r"\{[\s\S]*\}", stripped)
    return match.group(0).strip() if match else None


def _parse_bool(value):
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
    if not should_summarize_email(email_data):
        return None

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
        return None

    summary = _compact_text(response_text)
    if len(summary) > 280:
        summary = f"{summary[:277]}..."
    return summary or None


def draft_reply(email_data, to_value="", cc_value="", user_profile=None, email_id=None):
    body = (email_data.get("body") or "").strip()
    title = _compact_text(email_data.get("title") or "(No subject)")
    if not body and not title:
        return None

    system_prompt = (
        "You write concise, professional email replies. "
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
    cleaned = str(response_text or "").strip()
    return cleaned or None


def revise_reply(
    email_data,
    current_draft_text,
    to_value="",
    cc_value="",
    user_profile=None,
    email_id=None,
):
    current_draft_text = str(current_draft_text or "").strip()
    if not current_draft_text:
        return None

    system_prompt = (
        "You improve email drafts. Return only the revised email body text, "
        "no markdown and no subject."
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
    cleaned = str(response_text or "").strip()
    return cleaned or None
