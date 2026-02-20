from flask import Blueprint, render_template, request, redirect, url_for, abort
from datetime import datetime
from urllib.parse import urlsplit, parse_qs
from .db import (
    fetch_emails,
    fetch_email_by_id,
    fetch_thread_emails,
    set_email_type as db_set_email_type,
    toggle_read_state,
    delete_email as db_delete_email,
    mark_read,
    update_draft,
    create_reply_email,
)

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

def _normalize_addresses(raw_value):
    if raw_value is None:
        return None
    text = str(raw_value).replace(";", ",")
    cleaned = [part.strip() for part in text.split(",") if part.strip()]
    return ", ".join(cleaned) if cleaned else None

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
    emails = fetch_emails()
    emails_sorted = sort_emails(emails, sort_code)
    return render_template(
        "allemails.html",
        emails=emails_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
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
    )

@main.route("/email/<int:id>")
def email(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404
    mark_read(id, True)
    email_data["is_read"] = True
    thread_emails = fetch_thread_emails(email_data.get("thread_id"))
    next_url = _safe_next_url(request.args.get("next"))

    return render_template(
        "email.html",
        email=email_data,
        next_url=next_url,
        thread_emails=thread_emails,
        thread_count=len(thread_emails),
        current_user_email=LOCAL_USER_EMAIL,
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

    # Only allow the 3 user-facing categories to be set from the UI.
    allowed = {"response-needed", "read-only", "junk"}
    if new_type not in allowed:
        abort(400)

    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)

    if email_data.get("type") != new_type:
        db_set_email_type(id, new_type)

    # Return the user back to wherever they were.
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    return redirect(next_url)


@main.route("/search")
def search():
    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return render_template("search.html", query="", emails=None)

    results = []
    for e in fetch_emails():
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

    return render_template("search.html", query=q, emails=results)

@main.route("/send_reply/<int:id>", methods=["POST"])
def send_reply(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    to_value, cc_value, reply_text = _collect_reply_fields(email_data)
    if reply_text:
        update_draft(id, reply_text)
        create_reply_email(id, reply_text, to_value, cc_value)

    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    return redirect(url_for("main.email", id=id, next=next_url))

@main.route("/revise_draft/<int:id>", methods=["POST"])
def revise_draft(id):
    return redirect(url_for("main.email", id=id))

@main.route("/generate_draft/<int:id>", methods=["POST"])
def generate_draft(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    # TO BE REPLACED BY LLM
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
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    db_delete_email(id)
    return redirect(next_url)

@main.route("/email/<int:id>/toggle-read", methods=["POST"])
def toggle_read(id):
    email_data = fetch_email_by_id(id)
    if email_data is None:
        abort(404)
    toggle_read_state(id)
    next_url = _safe_next_url(request.form.get("next") or request.args.get("next"))
    return redirect(next_url)

