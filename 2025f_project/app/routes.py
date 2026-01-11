from flask import Blueprint, render_template, request

main = Blueprint("main", __name__)

@main.route("/")
def index():
    return "Welcome to our flask web application!"

@main.route("/about")
def about():
    emails = []
    return render_template("about.html")

@main.route("/allemails")
def allemails():
    emails = []
    return render_template("allemails.html", emails=emails)

@main.route("/readonly")
def readonly():
    emails = []
    return render_template("readonly.html", emails=emails)

@main.route("/responseneeded")
def responseneeded():
    emails = []
    return render_template("responseneeded.html", emails=emails)

@main.route("/junkmailconfirm")
def junkmailconfirm():
    emails = []
    return render_template("junkmailconfirm.html", emails=emails)

@main.route("/email/<int:id>")
def email(id):
    emails = []
    # later: fetch the email by id from the database
    return render_template("email.html", email=email)

@main.route("/search")
def search():
    q = (request.args.get("q") or "").strip()   # reads ?q=...
    emails = []  # later replace with real DB search results
    return render_template("search.html", query=q, emails=emails)


