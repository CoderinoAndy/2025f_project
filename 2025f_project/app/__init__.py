from flask import Flask, g, request
from werkzeug.exceptions import HTTPException
from datetime import datetime
from time import perf_counter
from .db import init_db
from .debug_logger import (
    configure_debug_logger,
    get_debug_log_path,
    log_event,
    log_exception,
)
from .gmail_service import gmail_available

def create_app():
    """Create app.
    """
    app = Flask(__name__, instance_relative_config=True)
    # Initialize structured logging before startup work so boot failures are recorded.
    configure_debug_logger()
    try:
        init_db()
        log_event(
            action_type="database",
            action="init_db",
            status="ok",
            component="startup",
            details="Database initialization completed.",
        )
    except Exception as exc:
        log_exception(
            action_type="database",
            action="init_db",
            error=exc,
            component="startup",
            details="Database initialization failed.",
        )
        raise

    if not gmail_available():
        log_event(
            action_type="gmail_sync",
            action="gmail_unavailable",
            status="skipped",
            level="WARNING",
            component="startup",
            details=(
                "Gmail integration is unavailable (missing Google API dependencies "
                "or credentials). New Gmail messages will not sync."
            ),
        )

    @app.before_request
    def _log_request_start():
        """Capture request start timing and write a start event to the debug log."""
        # Write log request start details in the app's structured log format for debugging and traceability.
        g.request_started_at = perf_counter()
        log_event(
            action_type="http_request",
            action="request_started",
            status="start",
            component="flask",
            method=request.method,
            path=request.path,
            endpoint=request.endpoint or "",
            remote_addr=request.remote_addr or "",
        )

    @app.after_request
    def _log_request_complete(response):
        """Log request completion details (status + duration) before sending the response."""
        started = getattr(g, "request_started_at", None)
        # Use monotonic timer values from before/after hooks to avoid wall-clock drift.
        duration_ms = int((perf_counter() - started) * 1000) if started is not None else -1
        status_code = int(response.status_code)
        if status_code >= 500:
            level = "ERROR"
            status = "error"
        elif status_code >= 400:
            level = "WARNING"
            status = "error"
        else:
            level = "INFO"
            status = "ok"

        log_event(
            action_type="http_request",
            action="request_completed",
            status=status,
            level=level,
            component="flask",
            method=request.method,
            path=request.path,
            endpoint=request.endpoint or "",
            status_code=status_code,
            duration_ms=duration_ms,
        )
        return response

    @app.teardown_request
    def _log_request_exception(error):
        """Log unexpected request exceptions that were not handled by Flask HTTP errors."""
        # Write log request exception details in the app's structured log format for debugging and traceability.
        if error is None or isinstance(error, HTTPException):
            return
        log_exception(
            action_type="http_request",
            action="request_exception",
            error=error,
            component="flask",
            method=request.method,
            path=request.path,
            endpoint=request.endpoint or "",
        )
    
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
        # Accept both date-only and date-time values because legacy rows are mixed.
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
    log_event(
        action_type="system",
        action="app_ready",
        status="ok",
        component="startup",
        log_path=get_debug_log_path(),
        details="Application startup completed.",
    )
    return app
