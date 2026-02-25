from flask import Blueprint, render_template, request, redirect, url_for, abort, Response, jsonify
from datetime import datetime
from urllib.parse import urlsplit, parse_qs
from .db import (
    fetch_emails,
    fetch_email_by_id,
    fetch_email_by_external_id,
    fetch_email_by_provider_draft_id,
    fetch_thread_emails,
    set_email_type as db_set_email_type,
    toggle_read_state,
    delete_email as db_delete_email,
    mark_read,
    update_draft,
    create_reply_email,
    save_local_draft,
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
from .qwen_client import ai_enabled, analyze_email as analyze_email_with_qwen, generate_reply_draft

main = Blueprint("main", __name__)
LOCAL_USER_EMAIL = "you@example.com"

VALID_SORTS = {
    "date_desc",
    "date_asc",
    "priority_desc",
    "priority_asc",
    "unread_first",
    "read_first",
}

NON_MAIN_TYPES = {"sent", "draft"}
LIVE_LIST_CONFIGS = {
    "all": {
        "exclude_types": NON_MAIN_TYPES,
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
}

def _normalize_addresses(raw_value):
    if raw_value is None:
        return None
    text = str(raw_value).replace(";", ",")
    cleaned = [part.strip() for part in text.split(",") if part.strip()]
    return ", ".join(cleaned) if cleaned else None


def _normalize_subject(raw_value):
    text = (raw_value or "").strip()
    return text or "(No subject)"


def _parse_optional_int(raw_value):
    try:
        return int(raw_value) if raw_value else None
    except (TypeError, ValueError):
        return None


def _collect_compose_fields():
    return {
        "to": _normalize_addresses(request.form.get("to")) or "",
        "cc": _normalize_addresses(request.form.get("cc")) or "",
        "subject": _normalize_subject(request.form.get("subject")),
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
    if email_data.get("type") in NON_MAIN_TYPES:
        return False
    if (email_data.get("summary") or "").strip():
        return False
    return bool((email_data.get("body") or "").strip())


def _run_ai_analysis(email_data):
    result = analyze_email_with_qwen(email_data)
    if not result:
        return False

    update_email_ai_fields(
        email_id=email_data["id"],
        summary=result.get("summary"),
        email_type=result.get("type"),
        priority=result.get("priority"),
    )
    return True


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


def _fetch_live_list_emails(list_view):
    config = LIVE_LIST_CONFIGS.get(list_view)
    if not config:
        return None, None
    email_type = config.get("email_type")
    exclude_types = config.get("exclude_types")
    if email_type:
        emails = fetch_emails(email_type=email_type)
    else:
        emails = fetch_emails(exclude_types=exclude_types)
    return emails, config["empty_message"]


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


@main.before_app_request
def sync_from_gmail():
    if request.endpoint == "static":
        return
    if request.method != "GET":
        return
    if request.endpoint in {"main.about", "main.compose"}:
        return
    trigger_background_sync(max_results=15)


@main.route("/")
def index():
    return redirect(url_for("main.about"))

@main.route("/about")
def about():
    return render_template("about.html")

@main.route("/allemails")
def allemails():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    emails = fetch_emails(exclude_types=NON_MAIN_TYPES)
    emails_sorted = sort_emails(emails, sort_code)
    return render_template(
        "allemails.html",
        emails=emails_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
        list_fingerprint=_emails_fingerprint(emails_sorted),
    )

@main.route("/readonly")
def readonly():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    filtered_sorted = sort_emails(fetch_emails(email_type="read-only"), sort_code)
    return render_template(
        "readonly.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
        list_fingerprint=_emails_fingerprint(filtered_sorted),
    )

@main.route("/responseneeded")
def responseneeded():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    filtered_sorted = sort_emails(fetch_emails(email_type="response-needed"), sort_code)
    return render_template(
        "responseneeded.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
        list_fingerprint=_emails_fingerprint(filtered_sorted),
    )

@main.route("/junkmailconfirm")
def junkmailconfirm():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    filtered_sorted = sort_emails(fetch_emails(email_type="junk-uncertain"), sort_code)
    return render_template(
        "junkmailconfirm.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
        list_fingerprint=_emails_fingerprint(filtered_sorted),
    )

@main.route("/junk")
def junk():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    filtered_sorted = sort_emails(fetch_emails(email_type="junk"), sort_code)
    return render_template(
        "junk.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
        list_fingerprint=_emails_fingerprint(filtered_sorted),
    )


@main.route("/sent")
def sent():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    filtered_sorted = sort_emails(fetch_emails(email_type="sent"), sort_code)
    return render_template(
        "sent.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
    )


@main.route("/drafts")
def drafts():
    sort_code = request.args.get("sort", "date_desc")
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    sync_drafts_from_gmail(max_results=40)
    filtered_sorted = sort_emails(fetch_emails(email_type="draft"), sort_code)
    return render_template(
        "drafts.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
    )

@main.route("/email/<int:id>")
def email(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404
    if email_data.get("type") not in NON_MAIN_TYPES:
        external_id = email_data.get("external_id")
        if external_id:
            set_message_read_state(external_id, read=True)
        mark_read(id, True)
        email_data = fetch_email_by_id(id) or email_data
        email_data["is_read"] = True

    if _should_auto_analyze_email(email_data):
        if _run_ai_analysis(email_data):
            email_data = fetch_email_by_id(id) or email_data

    thread_emails = fetch_thread_emails(email_data.get("thread_id"))
    if email_data.get("type") not in NON_MAIN_TYPES:
        thread_emails = [
            item for item in thread_emails if item.get("type") not in NON_MAIN_TYPES
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

    # Allow all user-facing triage buckets.
    allowed = {"response-needed", "read-only", "junk", "junk-uncertain"}
    if new_type not in allowed:
        abort(400)

    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    if email_data.get("type") != new_type:
        external_id = email_data.get("external_id")
        if external_id:
            if not set_message_type(external_id, new_type):
                db_set_email_type(id, new_type)
        else:
            db_set_email_type(id, new_type)

    # Return the user back to wherever they were.
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    return redirect(next_url)


@main.route("/search")
def search():
    current_list_url = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return render_template("search.html", query="", emails=None, current_list_url=current_list_url)

    results = []
    for e in fetch_emails(exclude_types=NON_MAIN_TYPES):
        haystack = " ".join(
            [
                e.get("title") or "",
                e.get("sender") or "",
                e.get("recipients") or "",
                e.get("cc") or "",
                e.get("body") or "",
            ]
        ).lower()
        if q in haystack:
            results.append(e)

    return render_template(
        "search.html",
        query=q,
        emails=results,
        current_list_url=current_list_url,
    )


@main.route("/email/<int:id>/analyze", methods=["POST"])
def analyze_email_route(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    _run_ai_analysis(email_data)
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
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

    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
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
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    fields = _collect_compose_fields()
    attachments = _collect_attachment_payloads()
    if not _has_compose_content(fields) and not attachments:
        return redirect(url_for("main.compose", next=next_url))

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
        fields["provider_draft_id"] = draft_info.get("provider_draft_id") or fields["provider_draft_id"]
        fields["thread_id"] = draft_info.get("thread_id") or fields["thread_id"]

    draft_email_id = save_local_draft(
        title=fields["subject"],
        body=fields["body"],
        recipients=fields["to"],
        cc=fields["cc"],
        email_id=fields["local_draft_id"],
        provider_draft_id=fields["provider_draft_id"],
        thread_id=fields["thread_id"],
    )
    return redirect(url_for("main.compose", draft_id=draft_email_id, next=next_url))


@main.route("/compose/autosave", methods=["POST"])
def compose_autosave():
    fields = _collect_compose_fields()
    if not _has_compose_content(fields):
        return Response(status=204)

    draft_info = upsert_gmail_draft(
        to_value=fields["to"],
        cc_value=fields["cc"],
        subject=fields["subject"],
        body_text=fields["body"],
        draft_id=fields["provider_draft_id"],
        attachments=None,
        thread_id=fields["thread_id"],
    )
    if draft_info:
        fields["provider_draft_id"] = draft_info.get("provider_draft_id") or fields["provider_draft_id"]
        fields["thread_id"] = draft_info.get("thread_id") or fields["thread_id"]

    save_local_draft(
        title=fields["subject"],
        body=fields["body"],
        recipients=fields["to"],
        cc=fields["cc"],
        email_id=fields["local_draft_id"],
        provider_draft_id=fields["provider_draft_id"],
        thread_id=fields["thread_id"],
    )
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

@main.route("/revise_draft/<int:id>", methods=["POST"])
def revise_draft(id):
    return redirect(url_for("main.email", id=id))

@main.route("/generate_draft/<int:id>", methods=["POST"])
def generate_draft(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    to_value, cc_value, current_reply_text = _collect_reply_fields(email_data)
    draft = generate_reply_draft(
        email_data=email_data,
        to_value=to_value or "",
        cc_value=cc_value or "",
        current_draft_text=current_reply_text,
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
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    return redirect(url_for("main.email", id=id, next=next_url))

@main.route("/email/<int:id>/delete", methods=["POST"])
def delete_email(id):
    email_data = fetch_email_by_id(id)
    if email_data:
        if email_data.get("type") == "draft" and email_data.get("provider_draft_id"):
            delete_draft_message(email_data["provider_draft_id"])
        elif email_data.get("external_id"):
            trash_message(email_data["external_id"])
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    db_delete_email(id)
    return redirect(next_url)

@main.route("/email/<int:id>/toggle-read", methods=["POST"])
def toggle_read(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    new_read_state = not bool(email_data.get("is_read"))
    external_id = email_data.get("external_id")
    if external_id:
        if not set_message_read_state(external_id, read=new_read_state):
            toggle_read_state(id)
    else:
        toggle_read_state(id)
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    return redirect(next_url)

