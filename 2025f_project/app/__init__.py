from flask import Flask
from datetime import datetime
from .db import init_db

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    init_db()
    
    @app.template_filter("fmt_dt")

    def fmt_dt(value):
        """
        Accepts:
        - "YYYY-MM-DD"
        - "YYYY-MM-DD HH:MM"
        - "YYYY-MM-DD HH:MM:SS"
        Returns:
        - "DD/MM/YYYY HH:MM" (24-hour time)
        """
        if value is None:
            return ""

        s = str(value).strip()
        dt = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue

        if dt is None:
            return s  # fallback: show original text if parsing fails

        return dt.strftime("%d/%m/%Y %H:%M")
        
    from .routes import main
    app.register_blueprint(main)
    return app
