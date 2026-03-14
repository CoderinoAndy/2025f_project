# Controller layer.
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
from markupsafe import Markup, escape
import re


PLAIN_TEXT_URL_RE = re.compile(
    r"(?P<url>(?:https?://|www\.)[^\s<]+|mailto:[^\s<]+|tel:[^\s<]+)",
    re.IGNORECASE,
)


# Build the Flask app, run startup setup, and then attach routes.
def create_app():
    """Create app.
    """
    app = Flask(__name__, instance_relative_config=True)
    # Set up structured logging first so startup failures get captured.
    configure_debug_logger()
    try:
        init_db()
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
        # Log this here so it is easier to trace later.
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
        # Use monotonic timing here so wall-clock drift does not skew request timing.
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
        # Log this here so it is easier to trace later.
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
    
    # Register this template filter once when the app is created.
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
        # Older rows mix date-only and date-time values, so accept both.
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue

        if dt is None:
            return s  # If parsing fails, just show the original text.

        return dt.strftime("%d/%m/%Y %H:%M")

    @app.template_filter("linkify_email_text")
    def linkify_email_text(value):
        """Render plain-text email content with clickable external links."""
        if value is None:
            return Markup("")

        def _linkify_line(line):
            rendered = Markup("")
            last_index = 0
            for match in PLAIN_TEXT_URL_RE.finditer(line):
                start, end = match.span()
                rendered += escape(line[last_index:start])

                raw_url = match.group("url")
                suffix = ""
                while raw_url and raw_url[-1] in ".,!?;:":
                    suffix = raw_url[-1] + suffix
                    raw_url = raw_url[:-1]

                href = raw_url
                if raw_url.lower().startswith("www."):
                    href = f"https://{raw_url}"

                rendered += Markup(
                    f'<a href="{escape(href)}" target="_blank" rel="noopener noreferrer">{escape(raw_url)}</a>'
                )
                if suffix:
                    rendered += escape(suffix)
                last_index = end

            rendered += escape(line[last_index:])
            return rendered

        return Markup("\n").join(_linkify_line(line) for line in str(value).split("\n"))

    # Import routes late so the startup utilities above are ready first.
    from .routes import main
    app.register_blueprint(main) # Hook the blueprint routes into the app.
    log_event(
        action_type="system",
        action="app_ready",
        status="ok",
        component="startup",
        log_path=get_debug_log_path(),
        details="Application startup completed.",
    )
    return app
