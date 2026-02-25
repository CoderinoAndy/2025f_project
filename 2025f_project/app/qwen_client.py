import json
import os
import re
import urllib.error
import urllib.request

MODEL_DEFAULT = "Qwen/Qwen2.5-14B-Instruct"
BASE_URL_DEFAULT = "https://router.huggingface.co/v1"
TIMEOUT_SECONDS_DEFAULT = 25
VALID_TYPES = {"read-only", "junk-uncertain", "junk", "response-needed"}


def _api_key():
    return (
        os.getenv("QWEN_API_KEY")
        or os.getenv("HF_TOKEN")
        or os.getenv("HUGGINGFACEHUB_API_TOKEN")
    )


def ai_enabled():
    return bool(_api_key())


def _chat_completion(messages, temperature=0.1, max_tokens=600):
    api_key = _api_key()
    if not api_key:
        return None

    base_url = os.getenv("QWEN_API_BASE_URL", BASE_URL_DEFAULT).strip().rstrip("/")
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
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request_obj, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        print(f"Qwen request failed: {exc}")
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


def analyze_email(email_data):
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


def generate_reply_draft(email_data, to_value="", cc_value="", current_draft_text=""):
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
