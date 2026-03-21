"""Expose the Flask app for WSGI and keep local-dev startup in one place."""

from app import create_app

app = create_app()

# Importing this module should be enough for production servers; only the direct
# execution path below should boot Flask's dev server.
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True, use_reloader=False)
