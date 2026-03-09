# MVC: Controller
"""Application entrypoint used by both WSGI servers and local development.

Create the Flask app once at import time, and only start Flask's built-in
debug server when this module is executed directly.
"""

from app import create_app

app = create_app()

# Keep dev-only server startup isolated from production WSGI imports.
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True, use_reloader=False)
