import json
import ipaddress
import os
import re
import urllib.error
import urllib.request
from urllib.parse import urlparse
from .debug_logger import log_event, log_exception

MODEL_DEFAULT = "qwen2.5-7b-instruct"
BASE_URL_DEFAULT = "http://127.0.0.1:8080/v1"
TIMEOUT_SECONDS_DEFAULT = 25
VALID_TYPES = {"read-only", "junk-uncertain", "junk", "response-needed"}
LOCALHOST_NAMES = {"localhost"}


def _api_key():
    return (
        os.getenv("QWEN_API_KEY")
        or os.getenv("HF_TOKEN")
        or os.getenv("HUGGINGFACEHUB_API_TOKEN")
    )


def ai_enabled():
    return _endpoint_allowed()


def _base_url():
    return (os.getenv("QWEN_API_BASE_URL") or BASE_URL_DEFAULT).strip().rstrip("/")


def _allow_remote():
    return str(os.getenv("QWEN_ALLOW_REMOTE", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _is_local_hostname(hostname):
    if not hostname:
        return False
    lower = hostname.lower()
    if lower in LOCALHOST_NAMES:
        return True
    try:
        return ipaddress.ip_address(lower).is_loopback
    except ValueError:
        return False


def _endpoint_allowed():
    base_url = _base_url()
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        log_event(
            action_type="qwen_endpoint",
            action="endpoint_validate",
            status="error",
            level="WARNING",
            component="qwen_client",
            details="Invalid QWEN_API_BASE_URL.",
            base_url=base_url,
        )
        return False
    if _allow_remote():
        return True
    if _is_local_hostname(parsed.hostname):
        return True
    log_event(
        action_type="qwen_endpoint",
        action="endpoint_validate",
        status="blocked",
        level="WARNING",
        component="qwen_client",
        details=(
            "Qwen blocked: external endpoint is not allowed while "
            "QWEN_ALLOW_REMOTE is off."
        ),
        base_url=base_url,
    )
    return False


def _request_headers():
    headers = {"Content-Type": "application/json"}
    api_key = _api_key()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _chat_completion(messages, temperature=0.1, max_tokens=600):
    if not _endpoint_allowed():
        return None

    base_url = _base_url()
    endpoint = f"{base_url}/chat/completions"
    model_name = os.getenv("QWEN_MODEL", MODEL_DEFAULT).strip() or MODEL_DEFAULT
    timeout_seconds = float(
        os.getenv("QWEN_TIMEOUT_SECONDS", str(TIMEOUT_SECONDS_DEFAULT)).strip()
        or TIMEOUT_SECONDS_DEFAULT
    )

    payload = {
        "model": model_name,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    body = json.dumps(payload).encode("utf-8")
    request_obj = urllib.request.Request(
        endpoint,
        data=body,
        headers=_request_headers(),
        method="POST",
    )

    try:
        with urllib.request.urlopen(request_obj, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        log_exception(
            action_type="qwen_api",
            action="chat_completion",
            error=exc,
            component="qwen_client",
            details="Qwen request failed.",
            endpoint=endpoint,
        )
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    choices = data.get("choices") or []
    if not choices:
        return None
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks = []
        for part in content:
            if isinstance(part, dict):
                text_piece = part.get("text")
                if text_piece:
                    chunks.append(str(text_piece))
        return "".join(chunks).strip() if chunks else None
    return None


def _extract_json_block(text):
    if not text:
        return None
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    match = re.search(r"\{[\s\S]*\}", stripped)
    return match.group(0).strip() if match else None


def _clean_summary(raw_summary):
    summary = " ".join(str(raw_summary or "").split())
    if not summary:
        return None
    if len(summary) > 280:
        return f"{summary[:277]}..."
    return summary


def _profile_prompt_block(user_profile):
    if not isinstance(user_profile, dict):
        return ""

    name_value = " ".join(str(user_profile.get("name") or "").split())
    occupation_value = " ".join(str(user_profile.get("occupation") or "").split())
    profile_bits = []
    if name_value:
        profile_bits.append(f"User name: {name_value}")
    if occupation_value:
        profile_bits.append(f"User occupation: {occupation_value}")
    if not profile_bits:
        return ""
    return "User profile context:\n" + "\n".join(profile_bits) + "\n\n"


def analyze_email(email_data, user_profile=None):
    body = (email_data.get("body") or "").strip()
    title = (email_data.get("title") or "(No subject)").strip()
    sender = (email_data.get("sender") or "").strip()
    recipients = (email_data.get("recipients") or "").strip()
    cc = (email_data.get("cc") or "").strip()

    if not body:
        return None

    trimmed_body = body[:8000]
    system_prompt = (
        "You are an email triage assistant. Return JSON only with keys: "
        "summary, type, priority. "
        "type must be one of: read-only, junk-uncertain, junk, response-needed. "
        "priority must be an integer 1 to 3. "
        "Use priority 3 for urgent/time-sensitive messages requiring action, "
        "2 for important but not urgent, 1 for low urgency. "
        "summary must be one concise sentence under 35 words."
    )
    user_prompt = (
        "Classify this email.\n\n"
        f"{_profile_prompt_block(user_profile)}"
        f"Subject: {title}\n"
        f"From: {sender}\n"
        f"To: {recipients}\n"
        f"Cc: {cc}\n"
        f"Body:\n{trimmed_body}\n\n"
        "Return strictly valid JSON."
    )
    response_text = _chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
        max_tokens=260,
    )
    if not response_text:
        return None

    json_block = _extract_json_block(response_text)
    if not json_block:
        return None

    try:
        parsed = json.loads(json_block)
    except json.JSONDecodeError:
        return None

    predicted_type = str(parsed.get("type") or "").strip().lower()
    if predicted_type not in VALID_TYPES:
        predicted_type = "read-only"

    try:
        priority = int(parsed.get("priority"))
    except (TypeError, ValueError):
        priority = 1
    priority = max(1, min(3, priority))

    summary = _clean_summary(parsed.get("summary"))
    if not summary:
        summary = _clean_summary(email_data.get("summary")) or "No summary generated."

    return {
        "type": predicted_type,
        "priority": priority,
        "summary": summary,
    }


def generate_reply_draft(
    email_data,
    to_value="",
    cc_value="",
    current_draft_text="",
    user_profile=None,
):
    body = (email_data.get("body") or "").strip()
    title = (email_data.get("title") or "(No subject)").strip()
    sender = (email_data.get("sender") or "").strip()
    current_draft_text = (current_draft_text or "").strip()

    if not body and not title:
        return None

    trimmed_body = body[:8000]
    system_prompt = (
        "You write concise, natural email replies. "
        "Return only the reply body text, no markdown, no subject line."
    )
    user_prompt = (
        "Write a reply draft for this email thread.\n\n"
        f"{_profile_prompt_block(user_profile)}"
        f"Original subject: {title}\n"
        f"Sender: {sender}\n"
        f"Planned To: {to_value}\n"
        f"Planned Cc: {cc_value}\n"
        f"Original message body:\n{trimmed_body}\n\n"
    )
    if current_draft_text:
        user_prompt += (
            "If useful, improve this existing draft while preserving intent:\n"
            f"{current_draft_text}\n\n"
        )
    user_prompt += "Keep it professional, clear, and actionable."

    response_text = _chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.35,
        max_tokens=420,
    )
    if not response_text:
        return None

    cleaned = response_text.strip()
    return cleaned or None
