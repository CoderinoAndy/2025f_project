import threading
import time
from uuid import uuid4

from ..db import fetch_email_by_id, update_draft, update_email_ai_fields
from ..ollama_client import (
    ai_enabled,
    classify_email,
    classification_to_email_type,
    draft_reply,
    log_ai_event,
    revise_reply,
    should_summarize_email,
    summarize_email,
)

AI_TASK_MAX_ITEMS = 200
AI_TASK_ACTIVE_STATUSES = {"queued", "running"}


class _AiTaskState:
    """In-memory task state for async AI endpoints."""

    def __init__(self):
        self.tasks = {}
        self.task_index = {}
        self.lock = threading.Lock()


AI_TASK_STATE = _AiTaskState()


def contextual_reply_fallback(email_data):
    """Generate a safe fallback reply when model output is unavailable."""
    sender_raw = str(email_data.get("sender") or "").strip()
    sender_label = sender_raw.split("<", 1)[0].strip().strip('"')
    if "@" in sender_label and " " not in sender_label:
        sender_label = sender_label.split("@", 1)[0]
    sender_label = (
        sender_label.replace(".", " ")
        .replace("_", " ")
        .replace("-", " ")
        .strip()
    )
    sender_label = " ".join(sender_label.split())

    title = str(email_data.get("title") or "").strip()
    body = str(email_data.get("body") or "")
    normalized_body = " ".join(body.replace("\r\n", "\n").replace("\r", "\n").split())
    sentence_chunks = (
        normalized_body.replace("?", "?. ").replace("!", "!. ").split(". ")
        if normalized_body
        else []
    )
    noise_markers = (
        "view in browser",
        "is this email difficult to read",
        "unsubscribe",
        "manage preferences",
        "privacy policy",
        "terms of service",
        "all rights reserved",
    )
    request_markers = (
        "please reply",
        "please confirm",
        "can you",
        "could you",
        "would you",
        "let me know",
        "respond by",
        "action required",
        "approval needed",
        "deadline",
        "asap",
    )

    filtered_sentences = []
    request_sentence = ""
    for chunk in sentence_chunks:
        sentence = " ".join(str(chunk).split()).strip(" .")
        if len(sentence) < 24:
            continue
        lowered = sentence.lower()
        if any(marker in lowered for marker in noise_markers):
            continue
        filtered_sentences.append(sentence)
        if not request_sentence and (
            "?" in sentence or any(marker in lowered for marker in request_markers)
        ):
            request_sentence = sentence
        if len(filtered_sentences) >= 4:
            break

    topic = ""
    if title and title != "(No subject)":
        topic = title
    elif filtered_sentences:
        topic = filtered_sentences[0]
    elif normalized_body:
        topic = normalized_body[:120].rstrip() + ("..." if len(normalized_body) > 120 else "")

    if len(topic) > 110:
        topic = topic[:107].rstrip() + "..."
    if len(request_sentence) > 150:
        request_sentence = request_sentence[:147].rstrip() + "..."

    greeting = f"Hi {sender_label}," if sender_label else "Hi,"
    if request_sentence:
        message_line = (
            f"Thanks for your email about {topic}. "
            f"I reviewed your request: {request_sentence} "
            "I will follow up shortly with next steps."
        )
    elif topic:
        message_line = (
            f"Thanks for sharing the update about {topic}. "
            "I reviewed it and appreciate the context."
        )
    else:
        message_line = (
            "Thanks for your email. I reviewed your message and will follow up shortly."
        )

    return f"{greeting}\n\n{message_line}\n\nBest regards,"


def summary_looks_unusable(email_data):
    """Return whether stored summary should be replaced."""
    summary = " ".join(str(email_data.get("summary") or "").split()).strip().lower()
    if not summary:
        return False

    title = " ".join(str(email_data.get("title") or "").split()).strip().lower()
    if title and title != "(no subject)" and summary.startswith(title):
        remainder = summary[len(title):].strip(" .:-")
        if len(remainder) < 40:
            return True

    unusable_markers = (
        "is this email difficult to read",
        "view in browser",
        "summary unavailable",
        "summary generation failed",
        "unable to summarize",
        "unable to interpret",
    )
    return any(marker in summary for marker in unusable_markers)


def should_auto_analyze_email(email_data, non_main_types=frozenset({"sent", "draft"})):
    """Return whether an email should trigger auto-analysis."""
    if not ai_enabled():
        return False
    if not email_data:
        return False
    if bool(email_data.get("is_archived")):
        return False
    if email_data.get("type") in non_main_types:
        return False
    body = (email_data.get("body") or "").strip()
    if not body:
        return False
    classification_missing = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )
    summary_needed = should_summarize_email(email_data) and (
        not (email_data.get("summary") or "").strip()
        or summary_looks_unusable(email_data)
    )
    return classification_missing or summary_needed


def run_ai_analysis(email_data, force=False):
    """Run classification and summary generation, then persist fields."""
    updated = False
    classification_missing = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )

    if force or classification_missing:
        classification = classify_email(
            email_data,
            email_id=email_data.get("id"),
        )
        if classification:
            update_email_ai_fields(
                email_id=email_data["id"],
                email_type=classification_to_email_type(classification),
                priority=classification.get("priority"),
                ai_category=classification.get("category"),
                ai_needs_response=classification.get("needs_response"),
                ai_confidence=classification.get("confidence"),
            )
            updated = True

    summary_missing = not (email_data.get("summary") or "").strip()
    summary_unusable = summary_looks_unusable(email_data)
    should_generate_summary = should_summarize_email(email_data) and (
        summary_missing or summary_unusable or force
    )
    if should_generate_summary:
        summary = summarize_email(
            email_data,
            email_id=email_data.get("id"),
        )
        if summary:
            update_email_ai_fields(
                email_id=email_data["id"],
                summary=summary,
            )
            updated = True

    return updated


def generate_reply_draft(
    *,
    email_data,
    to_value="",
    cc_value="",
    current_reply_text="",
    email_id=None,
):
    """Generate or revise a draft reply text with fallback."""
    if current_reply_text:
        draft_text = revise_reply(
            email_data=email_data,
            current_draft_text=current_reply_text,
            to_value=to_value or "",
            cc_value=cc_value or "",
            email_id=email_id,
        )
    else:
        draft_text = draft_reply(
            email_data=email_data,
            to_value=to_value or "",
            cc_value=cc_value or "",
            email_id=email_id,
        )
    if not draft_text:
        draft_text = contextual_reply_fallback(email_data)
    return draft_text


def _prune_ai_tasks_locked():
    tasks = AI_TASK_STATE.tasks
    task_index = AI_TASK_STATE.task_index
    if len(tasks) <= AI_TASK_MAX_ITEMS:
        return
    removable = [
        task for task in tasks.values() if task["status"] not in AI_TASK_ACTIVE_STATUSES
    ]
    removable.sort(key=lambda task: float(task.get("created_at") or 0))
    while len(tasks) > AI_TASK_MAX_ITEMS and removable:
        task = removable.pop(0)
        task_id = task["id"]
        key = (task["type"], task["email_id"])
        mapped_id = task_index.get(key)
        if mapped_id == task_id:
            task_index.pop(key, None)
        tasks.pop(task_id, None)


def _create_or_get_ai_task(task_type, email_id):
    tasks = AI_TASK_STATE.tasks
    task_index = AI_TASK_STATE.task_index
    key = (task_type, int(email_id))
    with AI_TASK_STATE.lock:
        existing_id = task_index.get(key)
        existing = tasks.get(existing_id) if existing_id else None
        if existing and existing["status"] in AI_TASK_ACTIVE_STATUSES:
            return dict(existing), False

        task_id = uuid4().hex
        now = time.time()
        task = {
            "id": task_id,
            "type": task_type,
            "email_id": int(email_id),
            "status": "queued",
            "result": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }
        tasks[task_id] = task
        task_index[key] = task_id
        _prune_ai_tasks_locked()
        return dict(task), True


def _set_ai_task_status(task_id, status, result=None, error=None):
    tasks = AI_TASK_STATE.tasks
    task_index = AI_TASK_STATE.task_index
    with AI_TASK_STATE.lock:
        task = tasks.get(task_id)
        if not task:
            return
        task["status"] = status
        task["updated_at"] = time.time()
        if result is not None:
            task["result"] = result
        if error is not None:
            task["error"] = error
        if status not in AI_TASK_ACTIVE_STATUSES:
            key = (task["type"], task["email_id"])
            mapped_id = task_index.get(key)
            if mapped_id == task_id:
                task_index.pop(key, None)


def get_ai_task(task_id):
    """Return one tracked task snapshot by id."""
    with AI_TASK_STATE.lock:
        task = AI_TASK_STATE.tasks.get(task_id)
        return dict(task) if task else None


def serialize_ai_task(task):
    """Return API-safe task payload."""
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
    _set_ai_task_status(task_id, "running")
    try:
        email_data = fetch_email_by_id(email_id)
        if not email_data:
            raise ValueError("Email not found.")
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
    _set_ai_task_status(task_id, "running")
    try:
        email_data = fetch_email_by_id(email_id)
        if not email_data:
            raise ValueError("Email not found.")

        if ai_enabled() and not str(email_data.get("ai_category") or "").strip():
            run_ai_analysis(email_data, force=True)
            email_data = fetch_email_by_id(email_id) or email_data

        draft_text = generate_reply_draft(
            email_data=email_data,
            to_value=to_value or "",
            cc_value=cc_value or "",
            current_reply_text=current_reply_text or "",
            email_id=email_id,
        )

        update_draft(email_id, draft_text)
        _set_ai_task_status(
            task_id,
            "completed",
            result={
                "draft": draft_text,
            },
        )
    except Exception as exc:
        log_ai_event(
            task="draft",
            status="error",
            email_id=email_id,
            detail=f"task_exception: {exc}",
        )
        _set_ai_task_status(task_id, "error", error=str(exc))


def start_analysis_task(email_id):
    """Create/start analysis task and return task payload."""
    task, created = _create_or_get_ai_task("analyze", email_id)
    if created:
        threading.Thread(
            target=_analysis_task_worker,
            args=(task["id"], int(email_id)),
            daemon=True,
        ).start()
    return task


def start_draft_task(email_id, to_value, cc_value, current_reply_text):
    """Create/start draft task and return task payload."""
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
