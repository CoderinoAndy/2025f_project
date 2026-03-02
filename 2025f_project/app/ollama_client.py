import ipaddress
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

OLLAMA_API_URL_DEFAULT = "http://localhost:11434/api/chat"
OLLAMA_MODEL_DEFAULT = "qwen2.5:7b"
OLLAMA_TIMEOUT_SECONDS_DEFAULT = 12
SUMMARY_MIN_CHARS_DEFAULT = 200
VALID_CATEGORIES = {"urgent", "informational", "junk"}
LOCALHOST_NAMES = {"localhost"}


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
    category = str(classification.get("category") or "").strip().lower()
    needs_response = bool(classification.get("needs_response"))
    if category == "junk":
        return "junk"
    if needs_response:
        return "response-needed"
    return "read-only"


def _compact_text(value):
    return " ".join(str(value or "").split()).strip()


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

    if category not in VALID_CATEGORIES:
        if needs_response or priority >= 3:
            category = "urgent"
        else:
            category = "informational"

    if category == "junk":
        needs_response = False
        if priority > 1:
            priority = 1

    return {
        "category": category,
        "needs_response": bool(needs_response),
        "priority": priority,
        "confidence": confidence,
    }


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
    body = (email_data.get("body") or "").strip()
    title = (email_data.get("title") or "").strip()
    if not body and not title:
        return None

    system_prompt = (
        "You classify emails. Return valid JSON only with exactly these keys: "
        "category, needs_response, priority, confidence. "
        "category must be one of: urgent, informational, junk. "
        "needs_response must be true or false. "
        "priority must be an integer 1 to 3. "
        "confidence must be a float 0 to 1."
    )
    user_prompt = (
        "Classify this email.\n\n"
        f"{_profile_prompt_block(user_profile)}"
        f"{_email_context_block(email_data)}\n\n"
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
        num_predict=220,
    )
    if not response_text:
        return None

    json_block = _extract_json_block(response_text)
    if not json_block:
        _log_action(task="classify", status="error", email_id=email_id, detail="missing_json_block")
        return None

    try:
        parsed = json.loads(json_block)
    except json.JSONDecodeError as exc:
        _log_action(task="classify", status="error", email_id=email_id, detail=f"invalid_json: {exc}")
        return None

    return _normalize_classification(parsed)


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
