from flask import Blueprint, render_template, request, redirect, url_for, abort
from datetime import datetime

main = Blueprint("main", __name__)

EMAILS = [
    {
        "id": 1,
        "title": "Meeting follow-up",
        "sender": "teacher@school.org",
        "date": "2026-01-10 22:58",
        "type": "response-needed",
        "priority": 3,
        "body": "Can you send your draft by Friday?",
        "summary": "Teacher asking for draft by Friday.",
        "draft": "Hi, thanks for the reminder! I will send it by Friday.",
        "is_read": False,

    },
    {
        "id": 2,
        "title": "Newsletter",
        "sender": "news@service.com",
        "date": "2026-01-09 14:12",
        "type": "read-only",
        "priority": 1,
        "body": "This is an informational newsletter.",
        "summary": None,
        "draft": None,
        "is_read": False,
    },
]

VALID_SORTS = {
    "date_desc",
    "date_asc",
    "priority_desc",
    "priority_asc",
    "unread_first",
    "read_first",
}

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


def get_email_by_id(email_id: int):
    for e in EMAILS:
        if e["id"] == email_id:
            return e
    return None

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
    emails_sorted = sort_emails(EMAILS, sort_code)
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
    filtered = [e for e in EMAILS if e["type"] == "read-only"]
    filtered_sorted = sort_emails(filtered, sort_code)
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
    filtered = [e for e in EMAILS if e["type"] == "response-needed"]
    filtered_sorted = sort_emails(filtered, sort_code)
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
    filtered = [e for e in EMAILS if e["type"] == "junk-uncertain"]
    filtered_sorted = sort_emails(filtered, sort_code)
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
    filtered = [e for e in EMAILS if e["type"] == "junk"]
    filtered_sorted = sort_emails(filtered, sort_code)
    return render_template(
        "junk.html",
        emails=filtered_sorted,
        sort=sort_code,
        current_list_url=current_list_url,
    )

@main.route("/email/<int:id>")
def email(id):
    email_data = get_email_by_id(id)
    if email_data is None:
        return "Email not found", 404
    email_data["is_read"] = True
    next_url = request.args.get("next")

    # Safety + sanity: only allow local paths
    if not next_url or not next_url.startswith("/"):
        next_url = url_for("main.allemails")

    return render_template("email.html", email=email_data, next_url=next_url)

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

    email_data = get_email_by_id(id)
    if email_data is None:
        abort(404)

    if email_data.get("type") != new_type:
        email_data["type"] = new_type

    # Return the user back to wherever they were.
    next_url = request.form.get("next") or request.args.get("next")
    if next_url and next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("main.allemails"))


@main.route("/search")
def search():
    q = (request.args.get("q") or "").strip().lower()
    if not q:
        return render_template("search.html", query="", emails=None)

    results = []
    for e in EMAILS:
        haystack = f"{e['title']} {e['sender']} {e['body']}".lower()
        if q in haystack:
            results.append(e)

    return render_template("search.html", query=q, emails=results)

@main.route("/send_reply/<int:id>", methods=["POST"])
def send_reply(id):
    next_url = request.form.get("next") or request.args.get("next")
    if next_url and next_url.startswith("/"):
        return redirect(url_for("main.email", id=id, next=next_url))
    return redirect(url_for("main.email", id=id))

@main.route("/revise_draft/<int:id>", methods=["POST"])
def revise_draft(id):
    return redirect(url_for("main.email", id=id))

@main.route("/generate_draft/<int:id>", methods=["POST"])
def generate_draft(id):
    email_data = get_email_by_id(id)
    if email_data is None:
        return "Email not found", 404

    # TO BE REPLACED BY LLM
    title = email_data.get("title") or ""
    draft = (
        f"Hi,\n\n"
        f"Thanks for your message about \"{title}\". "
        f"I’ve received it and will get back to you shortly.\n\n"
        f"Best regards,"
    )

    email_data["draft"] = draft
    next_url = request.form.get("next") or request.args.get("next")
    if next_url and next_url.startswith("/"):
        return redirect(url_for("main.email", id=id, next=next_url))
    return redirect(url_for("main.email", id=id))

@main.route("/email/<int:id>/delete", methods=["POST"])
def delete_email(id):
    global EMAILS
    next_url = request.form.get("next") or request.args.get("next")
    EMAILS = [e for e in EMAILS if e["id"] != id]
    if next_url and next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("main.allemails"))

@main.route("/email/<int:id>/toggle-read", methods=["POST"])
def toggle_read(id):
    email_data = get_email_by_id(id)
    if email_data is None:
        abort(404)
    email_data["is_read"] = not bool(email_data.get("is_read"))
    next_url = request.form.get("next") or request.args.get("next")
    if next_url and next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("main.allemails"))

