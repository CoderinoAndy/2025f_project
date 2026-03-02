from app import create_app  # Import the app factory that wires config, DB, and routes.

app = create_app()  # Build one Flask app instance for both CLI and WSGI use.

if __name__ == "__main__":  # Run dev server only when executing `python run.py` directly.
    app.run(host="127.0.0.1", port=5050, debug=True, use_reloader=False)  # Bind local dev host/port with debug on.
    
