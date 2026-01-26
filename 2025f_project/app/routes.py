from flask import Blueprint, render_template, request, redirect, url_for, abort

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
    },
]

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
    return render_template("allemails.html", emails=EMAILS)

@main.route("/readonly")
def readonly():
    filtered = [e for e in EMAILS if e["type"] == "read-only"]
    return render_template("readonly.html", emails=filtered)

@main.route("/responseneeded")
def responseneeded():
    filtered = [e for e in EMAILS if e["type"] == "response-needed"]
    return render_template("responseneeded.html", emails=filtered)

@main.route("/junkmailconfirm")
def junkmailconfirm():
    filtered = [e for e in EMAILS if e["type"] == "junk-uncertain"]
    return render_template("junkmailconfirm.html", emails=filtered)

@main.route("/junk")
def junk():
    filtered = [e for e in EMAILS if e["type"] == "junk"]
    return render_template("junk.html", emails=filtered)

@main.route("/email/<int:id>")
def email(id):
    email_data = get_email_by_id(id)
    if email_data is None:
        return "Email not found", 404
    return render_template("email.html", email=email_data)

@main.route("/email/<int:id>/set-type", methods=["POST"])
def set_email_type(id):
    new_type = request.form.get("new_type", "").strip()

    allowed = {"response-needed", "read-only", "junk"}
    if new_type not in allowed:
        abort(400)

    email = Email.query.get_or_404(id)
    if email.type != new_type:
        email.type = new_type
        db.session.commit()

    return redirect(url_for("main.email", id=id))

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

@main.route("/confirm_junk/<int:id>", methods=["POST"])
def confirm_junk(id):
    email_data = get_email_by_id(id)
    if email_data is not None:
        email_data["type"] = "junk"
    return redirect(request.referrer or url_for("main.allemails"))

@main.route("/reject_junk/<int:id>", methods=["POST"])
def reject_junk(id):
    email_data = get_email_by_id(id)
    if email_data is not None:
        email_data["type"] = "read-only"
    return redirect(request.referrer or url_for("main.allemails"))

@main.route("/send_reply/<int:id>", methods=["POST"])
def send_reply(id):
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
    return redirect(url_for("main.email", id=id))


