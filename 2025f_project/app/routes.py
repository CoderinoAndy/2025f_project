# Controller layer.
# Flask routes for mailbox pages, compose flows, and lightweight JSON action APIs.
from flask import Blueprint, render_template, request, redirect, url_for, abort, Response, jsonify
import os
import threading
from urllib.parse import urlsplit
from .db import (
    fetch_email_by_id,
    fetch_emails_by_ids,
    fetch_email_by_provider_draft_id,
    fetch_thread_emails,
    get_user_display_name,
    set_email_type as db_set_email_type,
    set_email_archived,
    set_user_display_name,
    delete_email as db_delete_email,
    mark_read,
    update_draft,
    create_reply_email,
    save_local_draft,
    create_local_sent_email,
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
    sync_message_by_external_id,
    trash_message,
    trigger_background_sync,
    upsert_gmail_draft,
)
from .email_content import prepare_html_email_document
from .ollama_client import (
    ai_enabled,
    can_generate_reply_draft as _can_generate_reply_draft,
    generate_reply_draft as _generate_reply_draft,
    get_ai_task as _get_ai_task,
    serialize_ai_task as _serialize_ai_task,
    should_auto_analyze_email as _should_auto_analyze_email,
    should_summarize_email,
    start_analysis_task as _start_analysis_task,
    start_draft_task as _start_draft_task,
)
from .mailbox import (
    LIVE_EMAIL_POLL_INTERVAL_MS,
    MAILBOX_PAGE_SIZE,
    build_mailbox_pagination,
    build_mailbox_context,
    emails_fingerprint as _emails_fingerprint,
    fetch_live_list_email_ids as _fetch_live_list_email_ids,
    fetch_live_list_emails as _fetch_live_list_emails,
    mailbox_live_polling_enabled,
    maybe_get_live_sync_max_results,
)

main = Blueprint("main", __name__)
LOCAL_USER_EMAIL = (os.getenv("LOCAL_USER_EMAIL") or "you@example.com").strip() or "you@example.com"

# Mailbox type rules shared by several routes.
NON_MAIN_TYPES = {"sent", "draft"}
ALLOWED_EMAIL_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}


@main.app_context_processor
def inject_user_display_name():
    """Expose the mailbox-owner display name to shared templates."""
    return {"user_display_name": get_user_display_name() or ""}

def _normalize_addresses(raw_value):
    """Normalize addresses.
    """
    # Keep addresses in a consistent format across the app.
    if raw_value is None:
        return None
    text = str(raw_value).replace(";", ",")
    cleaned = [part.strip() for part in text.split(",") if part.strip()]
    return ", ".join(cleaned) if cleaned else None


def _parse_optional_int(raw_value):
    """Parse optional int.
    """
    # Validate this before we trust it.
    try:
        return int(raw_value) if raw_value else None
    except (TypeError, ValueError):
        return None


def _collect_compose_fields():
    """Collect compose fields.
    """
    # Pull compose fields out of the request/context and normalize them.
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
    # Make sure there is compose content before we do heavier work.
    return bool(
        fields.get("to")
        or fields.get("cc")
        or fields.get("subject")
        or fields.get("body")
    )


def _collect_attachment_payloads():
    """Collect attachment payloads.
    """
    # Pull attachment payloads out of the request/context and normalize them.
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
    """Collect reply fields.
    """
    # Pull reply fields out of the request/context and normalize them.
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


def _set_message_read_state_async(external_id, read=True):
    """Set message read state async.
    """
    # Update read state asynchronously and keep local and provider state aligned when we can.
    if not external_id:
        return
    threading.Thread(
        target=set_message_read_state,
        args=(external_id, read),
        daemon=True,
    ).start()


def _current_list_url():
    """Current list url.
    """
    # Resolve the current list URL, with a safe fallback if config is missing.
    return request.full_path[:-1] if request.full_path.endswith("?") else request.full_path


def _list_query_state():
    """List query state.
    """
    # Apply the active list-query rules before rendering this mailbox view.
    sort_code = request.args.get("sort", "date_desc")
    search_query = (request.args.get("q") or "").strip()
    page = max(1, _parse_optional_int(request.args.get("page")) or 1)
    return sort_code, search_query, page, _current_list_url()


def _safe_next_url(raw_next):
    """Return a local in-app URL for redirects."""
    fallback = url_for("main.allemails")
    candidate = (raw_next or "").strip()
    if not candidate:
        return fallback
    if not candidate.startswith("/"):
        return fallback

    # Parse this once and reject external or absolute targets.
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc:
        return fallback
    if parsed.path.startswith("/email/"):
        # Keep people on list-style pages after POST actions.
        return fallback
    return f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path


def _next_url_from_request():
    """Next url from request.
    """
    # Resolve the next URL from the request, with a safe fallback if needed.
    return _safe_next_url(request.form.get("next") or request.args.get("next"))


def _render_mailbox_page(
    template_name,
    *,
    list_view,
    include_fingerprint=True,
):
    """Render mailbox page.
    """
    # Shared mailbox renderer for /allemails, /junk, /archive, and friends.
    sort_code, search_query, page, current_list_url = _list_query_state()
    emails, empty_message, total_count, current_page = _fetch_live_list_emails(
        list_view,
        search_query=search_query,
        sort_code=sort_code,
        page=page,
        page_size=MAILBOX_PAGE_SIZE,
    )
    if emails is None:
        abort(404)
    pagination = build_mailbox_pagination(
        current_list_url,
        page=current_page,
        page_size=MAILBOX_PAGE_SIZE,
        total_count=total_count,
    )
    context = build_mailbox_context(
        emails,
        sort_code=sort_code,
        current_list_url=pagination["current_url"],
        search_query=search_query,
        live_poll_interval_ms=LIVE_EMAIL_POLL_INTERVAL_MS,
        include_fingerprint=include_fingerprint,
        presorted=True,
        empty_message=empty_message,
        pagination=pagination,
        current_page=current_page,
        live_polling_enabled=mailbox_live_polling_enabled(
            list_view,
            search_query=search_query,
            page=current_page,
        ),
    )
    return render_template(template_name, **context)


def _persist_compose_draft(fields, attachments=None):
    """Persist compose draft.
    """
    # Save the compose form to the provider and local DB, then return the local draft id.
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
    """Set email type with fallback.
    """
    if email_data.get("type") == new_type:
        return
    external_id = email_data.get("external_id")
    if external_id:
        # Update provider labels first, then save the user's explicit mailbox move
        # locally because provider sync preserves existing triage on older rows.
        set_message_type(external_id, new_type)
    db_set_email_type(email_id, new_type)


def _set_read_state_with_fallback(email_id, email_data, target_read_state):
    """Set read state with fallback.
    """
    external_id = email_data.get("external_id")
    if external_id:
        # Fall back to the local DB if the provider update fails so the UI stays responsive.
        if not set_message_read_state(external_id, read=target_read_state):
            mark_read(email_id, target_read_state)
        return
    mark_read(email_id, target_read_state)


def _parse_bulk_email_ids(raw_ids):
    """Parse bulk email IDs.
    """
    # Validate this before we trust it.
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


def _resolve_bulk_email_ids_from_request():
    """Resolve explicit or whole-view bulk selection IDs from the current request."""
    selection_scope = (request.form.get("selection_scope") or "").strip().lower()
    if selection_scope != "all":
        return _parse_bulk_email_ids(request.form.get("ids"))

    list_view = (request.form.get("list_view") or "").strip()
    search_query = (request.form.get("search_query") or "").strip()
    email_ids = _fetch_live_list_email_ids(list_view, search_query=search_query)
    if email_ids is None:
        abort(400)
    return email_ids


@main.route("/api/list-emails")
def list_emails_api():
    """List emails api.
    """
    # The live list polls this endpoint every few seconds.
    list_view = (request.args.get("view") or "").strip()
    sort_code = request.args.get("sort", "date_desc")
    search_query = (request.args.get("q") or "").strip()
    page = max(1, _parse_optional_int(request.args.get("page")) or 1)
    sync_requested = (request.args.get("sync") or "1").strip() != "0"
    current_list_url = _safe_next_url(request.args.get("next"))

    # Keep mailbox views fresh without letting provider sync run wild.
    sync_max_results = maybe_get_live_sync_max_results(sync_requested)
    if sync_max_results is not None:
        trigger_background_sync(max_results=sync_max_results)

    emails, empty_message, total_count, current_page = _fetch_live_list_emails(
        list_view,
        search_query=search_query,
        sort_code=sort_code,
        page=page,
        page_size=MAILBOX_PAGE_SIZE,
    )
    if emails is None:
        abort(400)

    pagination = build_mailbox_pagination(
        current_list_url,
        page=current_page,
        page_size=MAILBOX_PAGE_SIZE,
        total_count=total_count,
    )
    rows_html = render_template(
        "_mailbox_list_content.html",
        emails=emails,
        current_list_url=pagination["current_url"],
        empty_message=empty_message,
        pagination=pagination,
    )
    return jsonify(
        {
            "html": rows_html,
            "fingerprint": _emails_fingerprint(
                emails,
                total_count=total_count,
                page=current_page,
            ),
            "count": len(emails),
            "page": current_page,
            "current_list_url": pagination["current_url"],
        }
    )


@main.route("/api/email/<int:id>/ai/analyze", methods=["POST"])
def start_ai_analyze(id):
    """Start AI analyze.
    """
    # Start AI analysis in the background and return metadata for polling.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if bool(email_data.get("is_archived")):
        abort(400)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)
    task = _start_analysis_task(id, force=True)
    return jsonify(_serialize_ai_task(task))


@main.route("/api/email/<int:id>/ai/draft", methods=["POST"])
def start_ai_draft(id):
    """Start AI draft.
    """
    # Start AI draft generation in the background and return metadata for polling.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if bool(email_data.get("is_archived")):
        abort(400)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    # The request body is optional so older callers still work.
    payload = request.get_json(silent=True) or {}
    to_value = _normalize_addresses(payload.get("to")) or ""
    cc_value = _normalize_addresses(payload.get("cc")) or ""
    current_reply_text = str(payload.get("reply_text") or "").strip()
    if not _can_generate_reply_draft(email_data, current_draft_text=current_reply_text):
        return jsonify({"error": "AI draft is only available for emails that need a response."}), 400
    task = _start_draft_task(id, to_value, cc_value, current_reply_text)
    return jsonify(_serialize_ai_task(task))


@main.route("/api/ai-task/<task_id>", methods=["GET"])
def ai_task_status(task_id):
    """Ai task status.
    """
    # Keep the AI task lifecycle tidy so async UI polling stays consistent.
    task = _get_ai_task(task_id)
    if not task:
        abort(404)
    return jsonify(_serialize_ai_task(task))


@main.route("/api/settings/display-name", methods=["POST"])
def save_display_name():
    """Persist the mailbox-owner display name for shared AI/UI context."""
    payload = request.get_json(silent=True)
    raw_name = None
    if isinstance(payload, dict):
        raw_name = payload.get("display_name")
    if raw_name is None:
        raw_name = request.form.get("display_name")
    saved_name = set_user_display_name(raw_name)
    return jsonify({"display_name": saved_name or ""})


@main.before_app_request
def sync_from_gmail():
    """Sync from Gmail.
    """
    # Sync Gmail state into the local database.
    if request.endpoint == "static":
        return
    if request.method != "GET":
        return
    if request.endpoint in {
        "main.about",
        "main.compose",
        "main.list_emails_api",
        "main.ai_task_status",
    }:
        return
    trigger_background_sync(max_results=30)


@main.route("/")
def index():
    """Index.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return redirect(url_for("main.about"))

@main.route("/about")
def about():
    """About.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return render_template("about.html")


@main.route("/allemails")
def allemails():
    """Allemails.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "allemails.html",
        list_view="all",
    )

@main.route("/readonly")
def readonly():
    """Readonly.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "readonly.html",
        list_view="read-only",
    )

@main.route("/responseneeded")
def responseneeded():
    """Responseneeded.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "responseneeded.html",
        list_view="response-needed",
    )

@main.route("/junkmailconfirm")
def junkmailconfirm():
    """Junkmailconfirm.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "junkmailconfirm.html",
        list_view="junk-uncertain",
    )

@main.route("/junk")
def junk():
    """Junk.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "junk.html",
        list_view="junk",
    )


@main.route("/sent")
def sent():
    """Sent.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "sent.html",
        list_view="sent",
    )


@main.route("/drafts")
def drafts():
    """Drafts.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "drafts.html",
        list_view="draft",
    )


@main.route("/archive")
def archive():
    """Archive.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    return _render_mailbox_page(
        "archive.html",
        list_view="archived",
    )

@main.route("/email/<int:id>")
def email(id):
    """Email.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404
    external_id = email_data.get("external_id")
    # Backfill missing HTML for older rows that were synced before MIME decoding was fixed.
    if not email_data.get("body_html") and external_id:
        refreshed = sync_message_by_external_id(external_id)
        if refreshed:
            email_data = fetch_email_by_id(id) or email_data
    # Opening a normal inbox email marks it read both locally and at the provider.
    if email_data.get("type") not in NON_MAIN_TYPES and not bool(email_data.get("is_archived")):
        if external_id:
            _set_message_read_state_async(external_id, read=True)
        mark_read(id, True)
        email_data = fetch_email_by_id(id) or email_data
        email_data["is_read"] = True

    if email_data.get("body_html"):
        email_data["render_html_document"] = prepare_html_email_document(
            email_data.get("body_html")
        )
    else:
        email_data["render_html_document"] = ""

    ai_analysis_needed = False
    ai_analysis_task_id = None
    ai_summary_expected = should_summarize_email(email_data)
    ai_draft_available = _can_generate_reply_draft(email_data)
    # Start analysis asynchronously so the page still renders quickly.
    if _should_auto_analyze_email(email_data, non_main_types=NON_MAIN_TYPES):
        ai_analysis_needed = True
        task = _start_analysis_task(id, force=False)
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
        ai_draft_available=ai_draft_available,
    )

@main.route("/email/<int:id>/set-type", methods=["POST"])
def set_email_type(id):
    """Change an email's category.
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

    # Send the user back to where they came from.
    next_url = _next_url_from_request()
    return redirect(next_url)


@main.route("/emails/bulk-action", methods=["POST"])
def bulk_email_action():
    """Bulk email action.
    """
    # Convert API data into the mailbox shape the app uses locally.
    action = (request.form.get("action") or "").strip()
    new_type = (request.form.get("new_type") or "").strip()
    email_ids = _resolve_bulk_email_ids_from_request()
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

    emails_by_id = {
        email_data["id"]: email_data for email_data in fetch_emails_by_ids(email_ids)
    }

    # Process each selected row on its own so one failure does not block the rest.
    for email_id in email_ids:
        email_data = emails_by_id.get(email_id)
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
    """Archive email.
    """
    # Convert API data into the mailbox shape the app uses locally.
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
    """Unarchive email.
    """
    # Convert API data into the mailbox shape the app uses locally.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    set_email_archived(id, archived=False)
    next_url = _next_url_from_request()
    return redirect(next_url)


@main.route("/search")
def search():
    """Search.
    """
    # Thin route wrapper that validates input, then renders or redirects.
    q = (request.args.get("q") or "").strip()
    if not q:
        return redirect(url_for("main.allemails"))
    return redirect(url_for("main.allemails", q=q))


@main.route("/email/<int:id>/analyze", methods=["POST"])
def analyze_email_route(id):
    """Analyze email route.
    """
    # Convert API data into the mailbox shape the app uses locally.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    if bool(email_data.get("is_archived")):
        abort(400)
    if email_data.get("type") in NON_MAIN_TYPES:
        abort(400)

    _start_analysis_task(id, force=True)
    next_url = _next_url_from_request()
    return redirect(url_for("main.email", id=id, next=next_url))

@main.route("/send_reply/<int:id>", methods=["POST"])
def send_reply(id):
    """Send reply.
    """
    # Generate, revise, or validate send reply used by reply and draft workflows.
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
    """Compose.
    """
    next_candidate = request.args.get("next")
    # If the caller did not send next=..., fall back to the referring list page.
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
            # Backfill the provider draft id on older local rows.
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
    """Compose save.
    """
    # Shared helper for this file.
    next_url = _next_url_from_request()
    fields = _collect_compose_fields()
    attachments = _collect_attachment_payloads()
    if not _has_compose_content(fields) and not attachments:
        return redirect(url_for("main.compose", next=next_url))

    draft_email_id = _persist_compose_draft(fields, attachments=attachments)
    return redirect(url_for("main.compose", draft_id=draft_email_id, next=next_url))


@main.route("/compose/autosave", methods=["POST"])
def compose_autosave():
    """Compose autosave.
    """
    # Shared helper for this file.
    fields = _collect_compose_fields()
    if not _has_compose_content(fields):
        return Response(status=204)

    _persist_compose_draft(fields)
    return Response(status=204)


@main.route("/compose/send", methods=["POST"])
def compose_send():
    """Compose send.
    """
    fields = _collect_compose_fields()
    attachments = _collect_attachment_payloads()
    # Reattach files that already exist on the provider draft or local sent message before sending.
    # Rehydrate existing draft attachments before sending the final message.
    if fields["provider_draft_id"]:
        attachments = fetch_draft_attachments(fields["provider_draft_id"]) + attachments
    elif fields["local_draft_id"]:
        draft_email = fetch_email_by_id(fields["local_draft_id"])
        external_id = (draft_email or {}).get("external_id")
        if external_id:
            attachments = fetch_message_attachments(external_id) + attachments
    if not fields["to"]:
        abort(400)

    # Try the provider send first; the local fallback keeps the app usable offline.
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
    """Generate draft.
    """
    # Shared generate-draft helper for the reply and draft flow.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    to_value, cc_value, current_reply_text = _collect_reply_fields(email_data)
    if not _can_generate_reply_draft(email_data, current_draft_text=current_reply_text or ""):
        next_url = _next_url_from_request()
        return redirect(url_for("main.email", id=id, next=next_url))
    draft = _generate_reply_draft(
        email_data=email_data,
        to_value=to_value or "",
        cc_value=cc_value or "",
        current_reply_text=current_reply_text or "",
        email_id=id,
    )

    update_draft(id, draft)
    next_url = _next_url_from_request()
    return redirect(url_for("main.email", id=id, next=next_url))

@main.route("/email/<int:id>/delete", methods=["POST"])
def delete_email(id):
    """Delete email.
    """
    # Delete the email and clean up anything that depends on it.
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
    """Toggle read.
    """
    # Shared helper for this file.
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    new_read_state = not bool(email_data.get("is_read"))
    _set_read_state_with_fallback(id, email_data, new_read_state)
    next_url = _next_url_from_request()
    return redirect(next_url)
