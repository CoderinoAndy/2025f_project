from flask import Blueprint, render_template, request, redirect, url_for, abort, Response, jsonify
from datetime import datetime
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from uuid import uuid4
from werkzeug.utils import secure_filename
from .db import (
    get_user_profile,
    fetch_emails,
    fetch_email_by_id,
    fetch_email_by_provider_draft_id,
    fetch_thread_emails,
    set_email_type as db_set_email_type,
    set_email_archived,
    delete_email as db_delete_email,
    mark_read,
    update_draft,
    create_reply_email,
    save_local_draft,
    save_user_profile,
    create_local_sent_email,
    update_email_ai_fields,
)
from .gmail_service import (
    delete_draft_message,
    fetch_draft_attachment_metadata,
    fetch_draft_attachments,
    fetch_message_attachment_metadata,
    fetch_message_attachments,
    send_reply_message,
    send_compose_message,
    set_message_read_state,
    set_message_type,
    sync_drafts_from_gmail,
    trash_message,
    trigger_background_sync,
    upsert_gmail_draft,
)
from .ollama_client import (
    ai_enabled,
    classify_email,
    summarize_email,
    draft_reply,
    revise_reply,
    should_summarize_email,
    classification_to_email_type,
    log_ai_event,
)

main = Blueprint("main", __name__)
LOCAL_USER_EMAIL = "you@example.com"
PROFILE_UPLOAD_DIR = Path(__file__).resolve().parent / "static" / "uploads" / "profiles"
ALLOWED_PROFILE_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
OCCUPATION_OPTIONS = [
    "Student",
    "Unemployed",
    "Software Engineer",
    "Data Scientist",
    "Machine Learning Engineer",
    "DevOps Engineer",
    "Product Manager",
    "Project Manager",
    "UX Designer",
    "UI Designer",
    "Graphic Designer",
    "Marketing Specialist",
    "Sales Representative",
    "Accountant",
    "Financial Analyst",
    "Teacher",
    "Professor",
    "Researcher",
    "Doctor",
    "Nurse",
    "Pharmacist",
    "Dentist",
    "Therapist",
    "Social Worker",
    "Lawyer",
    "Paralegal",
    "HR Specialist",
    "Recruiter",
    "Operations Manager",
    "Business Analyst",
    "Consultant",
    "Architect",
    "Civil Engineer",
    "Mechanical Engineer",
    "Electrical Engineer",
    "Construction Manager",
    "Real Estate Agent",
    "Customer Support Specialist",
    "Administrative Assistant",
    "Executive Assistant",
    "Writer",
    "Editor",
    "Journalist",
    "Photographer",
    "Video Editor",
    "Chef",
    "Restaurant Manager",
    "Barista",
    "Retail Associate",
    "Warehouse Associate",
    "Truck Driver",
    "Pilot",
    "Flight Attendant",
    "Police Officer",
    "Firefighter",
    "Military Service Member",
    "Electrician",
    "Plumber",
    "Carpenter",
    "Mechanic",
    "Scientist",
    "Entrepreneur",
    "Freelancer",
    "Homemaker",
    "Retired",
    "Other",
]

VALID_SORTS = {
    "date_desc",
    "date_asc",
    "priority_desc",
    "priority_asc",
    "unread_first",
    "read_first",
}

NON_MAIN_TYPES = {"sent", "draft"}
HIDDEN_FROM_MAIN_LIST_TYPES = {"sent", "draft"}
ALLOWED_EMAIL_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}
LIVE_LIST_CONFIGS = {
    "all": {
        "exclude_types": HIDDEN_FROM_MAIN_LIST_TYPES,
        "empty_message": "Your All Emails tab is empty.",
    },
    "read-only": {
        "email_type": "read-only",
        "empty_message": "Your Read Only tab is empty.",
    },
    "response-needed": {
        "email_type": "response-needed",
        "empty_message": "Your Response Needed tab is empty.",
    },
    "junk": {
        "email_type": "junk",
        "empty_message": "Your Junk tab is empty.",
    },
    "junk-uncertain": {
        "email_type": "junk-uncertain",
        "empty_message": "You have no Junk Mail to confirm.",
    },
    "archived": {
        "archived_only": True,
        "exclude_types": HIDDEN_FROM_MAIN_LIST_TYPES,
        "empty_message": "Your Archive is empty.",
    },
}

AI_TASKS = {}
AI_TASK_INDEX = {}
AI_TASK_LOCK = threading.Lock()
AI_TASK_MAX_ITEMS = 200
AI_TASK_ACTIVE_STATUSES = {"queued", "running"}

def _normalize_addresses(raw_value):
    if raw_value is None:
        return None
    text = str(raw_value).replace(";", ",")
    cleaned = [part.strip() for part in text.split(",") if part.strip()]
    return ", ".join(cleaned) if cleaned else None


def _parse_optional_int(raw_value):
    try:
        return int(raw_value) if raw_value else None
    except (TypeError, ValueError):
        return None


def _collect_compose_fields():
    return {
        "to": _normalize_addresses(request.form.get("to")) or "",
        "cc": _normalize_addresses(request.form.get("cc")) or "",
        "subject": (request.form.get("subject") or "").strip(),
        "body": (request.form.get("body") or "").strip(),
        "local_draft_id": _parse_optional_int(request.form.get("local_draft_id")),
        "provider_draft_id": (request.form.get("provider_draft_id") or "").strip() or None,
        "thread_id": (request.form.get("thread_id") or "").strip() or None,
    }


def _has_compose_content(fields):
    return bool(
        fields.get("to")
        or fields.get("cc")
        or fields.get("subject")
        or fields.get("body")
    )


def _collect_attachment_payloads():
    payloads = []
    for item in request.files.getlist("attachments"):
        if item is None:
            continue
        filename = (item.filename or "").strip()
        if not filename:
            continue
        content = item.read()
        if content is None:
            continue
        payloads.append(
            {
                "filename": filename,
                "content": content,
                "content_type": item.mimetype or item.content_type or "",
            }
        )
    return payloads


def _save_profile_photo(file_obj):
    if file_obj is None:
        return None
    original_name = (file_obj.filename or "").strip()
    if not original_name:
        return None
    safe_name = secure_filename(original_name)
    extension = Path(safe_name).suffix.lower()
    if extension not in ALLOWED_PROFILE_IMAGE_EXTENSIONS:
        abort(400)
    if file_obj.mimetype and not file_obj.mimetype.startswith("image/"):
        abort(400)

    PROFILE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    unique_name = f"{uuid4().hex}{extension}"
    target_path = PROFILE_UPLOAD_DIR / unique_name
    file_obj.save(target_path)
    return f"uploads/profiles/{unique_name}"


def _collect_reply_fields(email_data):
    to_value = _normalize_addresses(request.form.get("to"))
    if not to_value:
        sender = _normalize_addresses(email_data.get("sender"))
        recipients = _normalize_addresses(email_data.get("recipients"))
        if sender and sender.lower() == LOCAL_USER_EMAIL:
            to_value = recipients or sender
        else:
            to_value = sender or recipients
    cc_value = _normalize_addresses(request.form.get("cc"))
    reply_text = (request.form.get("reply_text") or "").strip()
    return to_value, cc_value, reply_text


def _should_auto_analyze_email(email_data):
    if not ai_enabled():
        return False
    if not email_data:
        return False
    if bool(email_data.get("is_archived")):
        return False
    if email_data.get("type") in NON_MAIN_TYPES:
        return False
    body = (email_data.get("body") or "").strip()
    if not body:
        return False
    classification_missing = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )
    summary_needed = should_summarize_email(email_data) and not (
        email_data.get("summary") or ""
    ).strip()
    return classification_missing or summary_needed


def _run_ai_analysis(email_data, force=False):
    profile = get_user_profile()
    updated = False
    classification_missing = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )

    if force or classification_missing:
        classification = classify_email(
            email_data,
            user_profile=profile,
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
    if summary_missing and should_summarize_email(email_data):
        summary = summarize_email(
            email_data,
            user_profile=profile,
            email_id=email_data.get("id"),
        )
        if summary:
            update_email_ai_fields(
                email_id=email_data["id"],
                summary=summary,
            )
            updated = True

    return updated


def _safe_next_url(raw_next):
    """Return a local list-style URL and collapse nested /email/... next chains."""
    fallback = url_for("main.allemails")
    candidate = (raw_next or "").strip()
    if not candidate:
        return fallback

    seen = set()
    while candidate and candidate not in seen:
        seen.add(candidate)
        if not candidate.startswith("/"):
            return fallback

        parsed = urlsplit(candidate)
        if parsed.scheme or parsed.netloc:
            return fallback

        path = parsed.path or "/"
        query = parsed.query
        if path.startswith("/email/"):
            nested = parse_qs(query).get("next", [None])[0]
            if nested:
                candidate = nested
                continue
            return fallback

        return f"{path}?{query}" if query else path

    return fallback

def _parse_dt(value):
    """Parse your existing date strings safely."""
    if value is None:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def _dt_sort_value(dt):
    """
    Turn datetime into a comparable number without using timestamp()
    (timestamp can be annoying for very old dates on some systems).
    """
    if dt is None:
        return -1
    return dt.toordinal() * 86400 + dt.hour * 3600 + dt.minute * 60 + dt.second

def sort_emails(emails, sort_code):
    sort_code = sort_code if sort_code in VALID_SORTS else "date_desc"

    def dt_val(e):
        return _dt_sort_value(_parse_dt(e.get("date")))

    def pr_val(e):
        return int(e.get("priority") or 0)

    # Default: newest first
    if sort_code == "date_desc":
        return sorted(emails, key=dt_val, reverse=True)

    if sort_code == "date_asc":
        return sorted(emails, key=dt_val)

    # Priority: bigger number = more stars = more urgent
    if sort_code == "priority_desc":
        return sorted(emails, key=lambda e: (pr_val(e), dt_val(e)), reverse=True)

    if sort_code == "priority_asc":
        # low priority first, but keep newest first inside each priority group
        return sorted(emails, key=lambda e: (pr_val(e), -dt_val(e)))

    # Unread/read grouping, then newest first inside the group
    if sort_code == "unread_first":
        return sorted(emails, key=lambda e: (bool(e.get("is_read")), -dt_val(e)))

    if sort_code == "read_first":
        return sorted(emails, key=lambda e: (not bool(e.get("is_read")), -dt_val(e)))

    return sorted(emails, key=dt_val, reverse=True)


def _emails_fingerprint(emails):
    rows = []
    for email in emails:
        rows.append(
            ":".join(
                [
                    str(email.get("id") or ""),
                    str(int(bool(email.get("is_read")))),
                    str(email.get("type") or ""),
                    str(email.get("date") or ""),
                    str(int(email.get("priority") or 0)),
                    str(email.get("title") or ""),
                ]
            )
        )
    return "|".join(rows)


def _prune_ai_tasks_locked():
    if len(AI_TASKS) <= AI_TASK_MAX_ITEMS:
        return
    removable = [
        task for task in AI_TASKS.values() if task["status"] not in AI_TASK_ACTIVE_STATUSES
    ]
    removable.sort(key=lambda task: float(task.get("created_at") or 0))
    while len(AI_TASKS) > AI_TASK_MAX_ITEMS and removable:
        task = removable.pop(0)
        task_id = task["id"]
        key = (task["type"], task["email_id"])
        mapped_id = AI_TASK_INDEX.get(key)
        if mapped_id == task_id:
            AI_TASK_INDEX.pop(key, None)
        AI_TASKS.pop(task_id, None)


def _create_or_get_ai_task(task_type, email_id):
    key = (task_type, int(email_id))
    with AI_TASK_LOCK:
        existing_id = AI_TASK_INDEX.get(key)
        existing = AI_TASKS.get(existing_id) if existing_id else None
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
        AI_TASKS[task_id] = task
        AI_TASK_INDEX[key] = task_id
        _prune_ai_tasks_locked()
        return dict(task), True


def _set_ai_task_status(task_id, status, result=None, error=None):
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
            mapped_id = AI_TASK_INDEX.get(key)
            if mapped_id == task_id:
                AI_TASK_INDEX.pop(key, None)


def _get_ai_task(task_id):
    with AI_TASK_LOCK:
        task = AI_TASKS.get(task_id)
        return dict(task) if task else None


def _serialize_ai_task(task):
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
        _run_ai_analysis(email_data, force=True)
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

        if ai_enabled() and (
            not str(email_data.get("ai_category") or "").strip()
            or email_data.get("ai_needs_response") is None
        ):
            _run_ai_analysis(email_data, force=True)
            email_data = fetch_email_by_id(email_id) or email_data

        needs_response = email_data.get("ai_needs_response")
        if needs_response is None:
            needs_response = email_data.get("type") == "response-needed"
        if not bool(needs_response):
            _set_ai_task_status(
                task_id,
                "completed",
                result={
                    "needs_response": False,
                    "draft": current_reply_text or "",
                },
            )
            return

        profile = get_user_profile()
        if current_reply_text:
            draft_text = revise_reply(
                email_data=email_data,
                current_draft_text=current_reply_text,
                to_value=to_value or "",
                cc_value=cc_value or "",
                user_profile=profile,
                email_id=email_id,
            )
        else:
            draft_text = draft_reply(
                email_data=email_data,
                to_value=to_value or "",
                cc_value=cc_value or "",
                user_profile=profile,
                email_id=email_id,
            )
        if not draft_text:
            title = email_data.get("title") or ""
            draft_text = (
                f"Hi,\n\n"
                f"Thanks for your message about \"{title}\". "
                f"I've received it and will get back to you shortly.\n\n"
                f"Best regards,"
            )

        update_draft(email_id, draft_text)
        _set_ai_task_status(
            task_id,
            "completed",
            result={
                "needs_response": True,
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


def _start_analysis_task(email_id):
    task, created = _create_or_get_ai_task("analyze", email_id)
    if created:
        threading.Thread(
            target=_analysis_task_worker,
            args=(task["id"], int(email_id)),
            daemon=True,
        ).start()
    return task


def _start_draft_task(email_id, to_value, cc_value, current_reply_text):
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


def _set_message_read_state_async(external_id, read=True):
    if not external_id:
        return
    threading.Thread(
        target=set_message_read_state,
        args=(external_id, read),
        daemon=True,
    ).start()


def _fetch_live_list_emails(list_view):
    config = LIVE_LIST_CONFIGS.get(list_view)
    if not config:
        return None, None
    email_type = config.get("email_type")
    exclude_types = config.get("exclude_types")
    archived_only = bool(config.get("archived_only"))
    if email_type:
        emails = fetch_emails(
            email_type=email_type,
            archived_only=archived_only,
        )
    else:
        emails = fetch_emails(
            exclude_types=exclude_types,
            archived_only=archived_only,
        )
    return emails, config["empty_message"]


def _filter_emails_by_query(emails, query_text):
    query = (query_text or "").strip()
    if not query:
        return emails

    query_lc = query.lower()
    filtered = []
    for email in emails:
        haystack = " ".join(
            [
                email.get("title") or "",
                email.get("sender") or "",
                email.get("recipients") or "",
                email.get("cc") or "",
                email.get("body") or "",
            ]
        ).lower()
        if query_lc in haystack:
            filtered.append(email)
    return filtered


def _current_list_url():
    return request.full_path[:-1] if request.full_path.endswith("?") else request.full_path


def _list_query_state():
    sort_code = request.args.get("sort", "date_desc")
    search_query = (request.args.get("q") or "").strip()
    return sort_code, search_query, _current_list_url()


def _next_url_from_request():
    return _safe_next_url(request.form.get("next") or request.args.get("next"))


def _render_mailbox_page(
    template_name,
    *,
    email_type=None,
    exclude_types=None,
    archived_only=False,
    include_fingerprint=True,
    sync_drafts=False,
):
    sort_code, search_query, current_list_url = _list_query_state()
    if sync_drafts:
        sync_drafts_from_gmail(max_results=40)
    emails = fetch_emails(
        email_type=email_type,
        exclude_types=exclude_types,
        archived_only=archived_only,
    )
    emails = _filter_emails_by_query(emails, search_query)
    emails_sorted = sort_emails(emails, sort_code)

    context = {
        "emails": emails_sorted,
        "sort": sort_code,
        "current_list_url": current_list_url,
        "search_query": search_query,
    }
    if include_fingerprint:
        context["list_fingerprint"] = _emails_fingerprint(emails_sorted)
    return render_template(template_name, **context)


def _persist_compose_draft(fields, attachments=None):
    draft_info = upsert_gmail_draft(
        to_value=fields["to"],
        cc_value=fields["cc"],
        subject=fields["subject"],
        body_text=fields["body"],
        draft_id=fields["provider_draft_id"],
        attachments=attachments,
        thread_id=fields["thread_id"],
    )
    if draft_info:
        fields["provider_draft_id"] = (
            draft_info.get("provider_draft_id") or fields["provider_draft_id"]
        )
        fields["thread_id"] = draft_info.get("thread_id") or fields["thread_id"]

    return save_local_draft(
        title=fields["subject"],
        body=fields["body"],
        recipients=fields["to"],
        cc=fields["cc"],
        email_id=fields["local_draft_id"],
        provider_draft_id=fields["provider_draft_id"],
        thread_id=fields["thread_id"],
    )


def _set_email_type_with_fallback(email_id, email_data, new_type):
    if email_data.get("type") == new_type:
        return
    external_id = email_data.get("external_id")
    if external_id:
        if not set_message_type(external_id, new_type):
            db_set_email_type(email_id, new_type)
        return
    db_set_email_type(email_id, new_type)


def _set_read_state_with_fallback(email_id, email_data, target_read_state):
    external_id = email_data.get("external_id")
    if external_id:
        if not set_message_read_state(external_id, read=target_read_state):
            mark_read(email_id, target_read_state)
        return
    mark_read(email_id, target_read_state)


def _parse_bulk_email_ids(raw_ids):
    seen = set()
    parsed = []
    for token in str(raw_ids or "").split(","):
        value = token.strip()
        if not value:
            continue
        try:
            email_id = int(value)
        except (TypeError, ValueError):
            continue
        if email_id <= 0 or email_id in seen:
            continue
        seen.add(email_id)
        parsed.append(email_id)
    return parsed


@main.route("/api/list-emails")
def list_emails_api():
    list_view = (request.args.get("view") or "").strip()
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = _safe_next_url(request.args.get("next"))

    emails, empty_message = _fetch_live_list_emails(list_view)
    if emails is None:
        abort(400)

    emails_sorted = sort_emails(emails, sort_code)
    rows_html = render_template(
        "_live_email_rows.html",
        emails=emails_sorted,
        current_list_url=current_list_url,
        empty_message=empty_message,
    )
    return jsonify(
        {
            "html": rows_html,
            "fingerprint": _emails_fingerprint(emails_sorted),
            "count": len(emails_sorted),
        }
    )


@main.route("/api/email/<int:id>/ai/analyze", methods=["POST"])
def start_ai_analyze(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if bool(email_data.get("is_archived")):
        abort(400)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)
    task = _start_analysis_task(id)
    return jsonify(_serialize_ai_task(task))


@main.route("/api/email/<int:id>/ai/draft", methods=["POST"])
def start_ai_draft(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if bool(email_data.get("is_archived")):
        abort(400)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    payload = request.get_json(silent=True) or {}
    to_value = _normalize_addresses(payload.get("to")) or ""
    cc_value = _normalize_addresses(payload.get("cc")) or ""
    current_reply_text = str(payload.get("reply_text") or "").strip()
    task = _start_draft_task(id, to_value, cc_value, current_reply_text)
    return jsonify(_serialize_ai_task(task))


@main.route("/api/ai-task/<task_id>", methods=["GET"])
def ai_task_status(task_id):
    task = _get_ai_task(task_id)
    if not task:
        abort(404)
    return jsonify(_serialize_ai_task(task))


@main.before_app_request
def sync_from_gmail():
    if request.endpoint == "static":
        return
    if request.method != "GET":
        return
    if request.endpoint in {"main.about", "main.compose", "main.profile"}:
        return
    trigger_background_sync(max_results=15)


@main.route("/")
def index():
    return redirect(url_for("main.about"))

@main.route("/about")
def about():
    return render_template("about.html")


@main.route("/profile", methods=["GET", "POST"])
def profile():
    profile_data = get_user_profile()
    if request.method == "POST":
        name_value = (request.form.get("name") or "").strip()
        occupation_value = (request.form.get("occupation") or "").strip()
        if occupation_value and occupation_value not in OCCUPATION_OPTIONS:
            abort(400)

        photo_path = profile_data.get("photo_path") or ""
        uploaded = request.files.get("picture")
        if uploaded and (uploaded.filename or "").strip():
            photo_path = _save_profile_photo(uploaded)

        save_user_profile(
            name=name_value,
            occupation=occupation_value,
            photo_path=photo_path,
        )
        return redirect(url_for("main.profile"))

    return render_template(
        "profile.html",
        profile=profile_data,
        occupation_options=OCCUPATION_OPTIONS,
    )


@main.route("/allemails")
def allemails():
    return _render_mailbox_page(
        "allemails.html",
        exclude_types=HIDDEN_FROM_MAIN_LIST_TYPES,
    )

@main.route("/readonly")
def readonly():
    return _render_mailbox_page(
        "readonly.html",
        email_type="read-only",
    )

@main.route("/responseneeded")
def responseneeded():
    return _render_mailbox_page(
        "responseneeded.html",
        email_type="response-needed",
    )

@main.route("/junkmailconfirm")
def junkmailconfirm():
    return _render_mailbox_page(
        "junkmailconfirm.html",
        email_type="junk-uncertain",
    )

@main.route("/junk")
def junk():
    return _render_mailbox_page(
        "junk.html",
        email_type="junk",
    )


@main.route("/sent")
def sent():
    return _render_mailbox_page(
        "sent.html",
        email_type="sent",
        include_fingerprint=False,
    )


@main.route("/drafts")
def drafts():
    return _render_mailbox_page(
        "drafts.html",
        email_type="draft",
        include_fingerprint=False,
        sync_drafts=True,
    )


@main.route("/archive")
def archive():
    return _render_mailbox_page(
        "archive.html",
        exclude_types=HIDDEN_FROM_MAIN_LIST_TYPES,
        archived_only=True,
    )

@main.route("/email/<int:id>")
def email(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404
    if email_data.get("type") not in NON_MAIN_TYPES and not bool(email_data.get("is_archived")):
        external_id = email_data.get("external_id")
        if external_id:
            _set_message_read_state_async(external_id, read=True)
        mark_read(id, True)
        email_data = fetch_email_by_id(id) or email_data
        email_data["is_read"] = True

    ai_analysis_needed = False
    ai_analysis_task_id = None
    ai_summary_expected = should_summarize_email(email_data)
    if _should_auto_analyze_email(email_data):
        ai_analysis_needed = True
        task = _start_analysis_task(id)
        ai_analysis_task_id = task["id"]

    thread_emails = fetch_thread_emails(email_data.get("thread_id"))
    if email_data.get("type") not in NON_MAIN_TYPES and not bool(email_data.get("is_archived")):
        thread_emails = [
            item
            for item in thread_emails
            if item.get("type") not in NON_MAIN_TYPES and not bool(item.get("is_archived"))
        ]
    next_url = _safe_next_url(request.args.get("next"))

    return render_template(
        "email.html",
        email=email_data,
        next_url=next_url,
        thread_emails=thread_emails,
        thread_count=len(thread_emails),
        current_user_email=LOCAL_USER_EMAIL,
        ai_enabled=ai_enabled(),
        ai_analysis_needed=ai_analysis_needed,
        ai_analysis_task_id=ai_analysis_task_id,
        ai_summary_expected=ai_summary_expected,
    )

@main.route("/email/<int:id>/set-type", methods=["POST"])
def set_email_type(id):
    """Change an email's category.

    Note:
    - Right now, emails live in the in-memory EMAILS list.
    - Later, you can swap the implementation inside this function to update a SQLite row
      (or your Outlook integration) without changing the UI.
    """

    new_type = (request.form.get("new_type") or "").strip()

    if new_type not in ALLOWED_EMAIL_TYPES:
        abort(400)

    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    _set_email_type_with_fallback(id, email_data, new_type)
    if bool(email_data.get("is_archived")):
        set_email_archived(id, archived=False)

    # Return the user back to wherever they were.
    next_url = _next_url_from_request()
    return redirect(next_url)


@main.route("/emails/bulk-action", methods=["POST"])
def bulk_email_action():
    action = (request.form.get("action") or "").strip()
    new_type = (request.form.get("new_type") or "").strip()
    email_ids = _parse_bulk_email_ids(request.form.get("ids"))
    next_url = _next_url_from_request()

    allowed_actions = {
        "archive",
        "unarchive",
        "delete",
        "mark-read",
        "mark-unread",
        "set-type",
    }
    if action not in allowed_actions:
        abort(400)
    if action == "set-type" and new_type not in ALLOWED_EMAIL_TYPES:
        abort(400)
    if not email_ids:
        return redirect(next_url)

    for email_id in email_ids:
        email_data = fetch_email_by_id(email_id)
        if email_data is None:
            continue

        email_type = email_data.get("type")
        is_archived = bool(email_data.get("is_archived"))
        external_id = email_data.get("external_id")

        if action == "archive":
            if email_type == "draft" or is_archived:
                continue
            set_email_archived(email_id, archived=True)
            continue

        if action == "unarchive":
            if not is_archived:
                continue
            set_email_archived(email_id, archived=False)
            continue

        if action == "delete":
            if email_type == "draft" and email_data.get("provider_draft_id"):
                delete_draft_message(email_data["provider_draft_id"])
            elif external_id:
                trash_message(external_id)
            db_delete_email(email_id)
            continue

        if action in {"mark-read", "mark-unread"}:
            if email_type in NON_MAIN_TYPES:
                continue
            target_read_state = action == "mark-read"
            if bool(email_data.get("is_read")) == target_read_state:
                continue
            _set_read_state_with_fallback(email_id, email_data, target_read_state)
            continue

        if action == "set-type":
            if email_type in NON_MAIN_TYPES:
                continue
            _set_email_type_with_fallback(email_id, email_data, new_type)
            if is_archived:
                set_email_archived(email_id, archived=False)

    return redirect(next_url)


@main.route("/email/<int:id>/archive", methods=["POST"])
def archive_email(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if email_data.get("type") == "draft":
        abort(400)
    set_email_archived(id, archived=True)
    next_url = _next_url_from_request()
    return redirect(next_url)


@main.route("/email/<int:id>/unarchive", methods=["POST"])
def unarchive_email(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    set_email_archived(id, archived=False)
    next_url = _next_url_from_request()
    return redirect(next_url)


@main.route("/search")
def search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return redirect(url_for("main.allemails"))
    return redirect(url_for("main.allemails", q=q))


@main.route("/email/<int:id>/analyze", methods=["POST"])
def analyze_email_route(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if bool(email_data.get("is_archived")):
        abort(400)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    _start_analysis_task(id)
    next_url = _next_url_from_request()
    return redirect(url_for("main.email", id=id, next=next_url))

@main.route("/send_reply/<int:id>", methods=["POST"])
def send_reply(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    to_value, cc_value, reply_text = _collect_reply_fields(email_data)
    attachments = _collect_attachment_payloads()
    if reply_text:
        update_draft(id, reply_text)
        sent_id = None
        if email_data.get("external_id"):
            sent_id = send_reply_message(
                email_data,
                reply_text,
                to_value,
                cc_value,
                attachments=attachments,
            )
        if not sent_id:
            create_reply_email(id, reply_text, to_value, cc_value)
        trigger_background_sync(force=True, max_results=20)

    next_url = _next_url_from_request()
    return redirect(url_for("main.email", id=id, next=next_url))


@main.route("/compose")
def compose():
    next_candidate = request.args.get("next")
    if not next_candidate:
        referrer = request.referrer or ""
        parsed_referrer = urlsplit(referrer)
        if parsed_referrer.path.startswith("/"):
            next_candidate = (
                f"{parsed_referrer.path}?{parsed_referrer.query}"
                if parsed_referrer.query
                else parsed_referrer.path
            )
    next_url = _safe_next_url(next_candidate)
    local_draft_id = _parse_optional_int(request.args.get("draft_id"))
    provider_draft_id = (request.args.get("provider_draft_id") or "").strip() or None

    draft_email = None
    if local_draft_id:
        draft_email = fetch_email_by_id(local_draft_id)
    if draft_email is None and provider_draft_id:
        sync_drafts_from_gmail(max_results=40)
        draft_email = fetch_email_by_provider_draft_id(provider_draft_id)

    if draft_email and draft_email.get("type") != "draft":
        draft_email = None

    draft_attachments = []
    if draft_email:
        provider_draft_id = (draft_email.get("provider_draft_id") or "").strip()
        external_id = (draft_email.get("external_id") or "").strip()
        if not provider_draft_id and external_id:
            # Backfill provider draft id for legacy local rows.
            sync_drafts_from_gmail(max_results=80)
            refreshed = fetch_email_by_id(draft_email.get("id"))
            if refreshed and refreshed.get("type") == "draft":
                draft_email = refreshed
                provider_draft_id = (refreshed.get("provider_draft_id") or "").strip()
                external_id = (refreshed.get("external_id") or "").strip()

        if provider_draft_id:
            draft_attachments = fetch_draft_attachment_metadata(provider_draft_id)
        elif external_id:
            draft_attachments = fetch_message_attachment_metadata(external_id)

    return render_template(
        "compose.html",
        draft_email=draft_email or {},
        draft_attachments=draft_attachments,
        next_url=next_url,
    )


@main.route("/compose/save", methods=["POST"])
def compose_save():
    next_url = _next_url_from_request()
    fields = _collect_compose_fields()
    attachments = _collect_attachment_payloads()
    if not _has_compose_content(fields) and not attachments:
        return redirect(url_for("main.compose", next=next_url))

    draft_email_id = _persist_compose_draft(fields, attachments=attachments)
    return redirect(url_for("main.compose", draft_id=draft_email_id, next=next_url))


@main.route("/compose/autosave", methods=["POST"])
def compose_autosave():
    fields = _collect_compose_fields()
    if not _has_compose_content(fields):
        return Response(status=204)

    _persist_compose_draft(fields)
    return Response(status=204)


@main.route("/compose/send", methods=["POST"])
def compose_send():
    fields = _collect_compose_fields()
    attachments = _collect_attachment_payloads()
    if fields["provider_draft_id"]:
        attachments = fetch_draft_attachments(fields["provider_draft_id"]) + attachments
    elif fields["local_draft_id"]:
        draft_email = fetch_email_by_id(fields["local_draft_id"])
        external_id = (draft_email or {}).get("external_id")
        if external_id:
            attachments = fetch_message_attachments(external_id) + attachments
    if not fields["to"]:
        abort(400)

    sent_external_id = send_compose_message(
        to_value=fields["to"],
        cc_value=fields["cc"],
        subject=fields["subject"],
        body_text=fields["body"],
        attachments=attachments,
        thread_id=fields["thread_id"],
    )
    if not sent_external_id:
        create_local_sent_email(
            title=fields["subject"],
            body=fields["body"],
            recipients=fields["to"],
            cc=fields["cc"],
            thread_id=fields["thread_id"],
        )

    if fields["provider_draft_id"]:
        delete_draft_message(fields["provider_draft_id"])
        existing_draft = fetch_email_by_provider_draft_id(fields["provider_draft_id"])
        if existing_draft:
            db_delete_email(existing_draft["id"])
    if fields["local_draft_id"]:
        db_delete_email(fields["local_draft_id"])

    trigger_background_sync(force=True, max_results=20)
    return redirect(url_for("main.sent"))

@main.route("/generate_draft/<int:id>", methods=["POST"])
def generate_draft(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    if ai_enabled() and (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
    ):
        _run_ai_analysis(email_data, force=True)
        email_data = fetch_email_by_id(id) or email_data

    needs_response = email_data.get("ai_needs_response")
    if needs_response is None:
        needs_response = email_data.get("type") == "response-needed"
    if not bool(needs_response):
        next_url = _next_url_from_request()
        return redirect(url_for("main.email", id=id, next=next_url))

    to_value, cc_value, current_reply_text = _collect_reply_fields(email_data)
    profile = get_user_profile()
    if current_reply_text:
        draft = revise_reply(
            email_data=email_data,
            current_draft_text=current_reply_text,
            to_value=to_value or "",
            cc_value=cc_value or "",
            user_profile=profile,
            email_id=id,
        )
    else:
        draft = draft_reply(
            email_data=email_data,
            to_value=to_value or "",
            cc_value=cc_value or "",
            user_profile=profile,
            email_id=id,
        )
    if not draft:
        title = email_data.get("title") or ""
        draft = (
            f"Hi,\n\n"
            f"Thanks for your message about \"{title}\". "
            f"I've received it and will get back to you shortly.\n\n"
            f"Best regards,"
        )

    update_draft(id, draft)
    next_url = _next_url_from_request()
    return redirect(url_for("main.email", id=id, next=next_url))

@main.route("/email/<int:id>/delete", methods=["POST"])
def delete_email(id):
    email_data = fetch_email_by_id(id)
    if email_data:
        if email_data.get("type") == "draft" and email_data.get("provider_draft_id"):
            delete_draft_message(email_data["provider_draft_id"])
        elif email_data.get("external_id"):
            trash_message(email_data["external_id"])
    next_url = _next_url_from_request()
    db_delete_email(id)
    return redirect(next_url)

@main.route("/email/<int:id>/toggle-read", methods=["POST"])
def toggle_read(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    new_read_state = not bool(email_data.get("is_read"))
    _set_read_state_with_fallback(id, email_data, new_read_state)
    next_url = _next_url_from_request()
    return redirect(next_url)

