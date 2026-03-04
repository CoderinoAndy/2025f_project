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
AI_TASKS = {}
AI_TASK_INDEX = {}
AI_TASK_LOCK = threading.Lock()


def contextual_reply_fallback(email_data):
    """Return a short fallback draft when model output is empty."""
    sender = str(email_data.get("sender") or "").split("<", 1)[0].strip().strip('"')
    topic = str(email_data.get("title") or "").strip() or "your message"
    greeting = f"Hi {sender}," if sender else "Hi,"
    return f"{greeting}\n\nThanks for your email about {topic}. I will follow up soon.\n\nBest regards,"


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


def generate_reply_draft(
    *,
    email_data,
    to_value="",
    cc_value="",
    current_reply_text="",
    email_id=None,
):
    """Generate a new draft or revise existing text."""
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
    return draft_text or contextual_reply_fallback(email_data)


def _cleanup_tasks_locked():
    """Remove older completed tasks if in-memory cache grows too large."""
    if len(AI_TASKS) <= AI_TASK_MAX_ITEMS:
        return

    done_tasks = [
        task for task in AI_TASKS.values() if task.get("status") not in AI_TASK_ACTIVE_STATUSES
    ]
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

        draft_text = generate_reply_draft(
            email_data=email_data,
            to_value=to_value or "",
            cc_value=cc_value or "",
            current_reply_text=current_reply_text or "",
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
            args=(task["id"], int(email_id), to_value or "", cc_value or "", current_reply_text or ""),
            daemon=True,
        ).start()
    return task
