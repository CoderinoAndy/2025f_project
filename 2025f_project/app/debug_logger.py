"""Tiny structured logger used across startup, sync, and AI flows."""

import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Lock

LOGGER_NAME = "app.debug"
DEFAULT_LOG_PATH = "instance/debug_log.txt"
MAX_FIELD_LENGTH = 1000
_CONFIG_LOCK = Lock()


def _clean_value(value):
    """Keep log values single-line and bounded."""
    text = str(value if value is not None else "")
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    if len(text) > MAX_FIELD_LENGTH:
        return f"{text[:MAX_FIELD_LENGTH]}..."
    return text


def _clean_key(value):
    """Normalize metadata keys into a log-friendly token."""
    raw = _clean_value(value).lower()
    if not raw:
        return "meta"
    safe = "".join(char if char.isalnum() else "_" for char in raw)
    safe = safe.strip("_")
    return safe or "meta"


def _log_path():
    """Resolve the log file path, honoring an override env var when present."""
    configured = (os.getenv("APP_DEBUG_LOG_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(DEFAULT_LOG_PATH)


def get_debug_log_path():
    """Return the current debug log path as text for UI/debug output."""
    return str(_log_path())


def configure_debug_logger():
    """Create the rotating debug logger once and reuse it thereafter."""
    logger = logging.getLogger(LOGGER_NAME)
    target_path = _log_path()

    with _CONFIG_LOCK:  # Serialize logger setup so we do not add duplicate handlers.
        target_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_target = str(target_path.resolve())
        for handler in logger.handlers:
            existing = getattr(handler, "baseFilename", None)
            if existing and str(Path(existing).resolve()) == resolved_target:
                return logger

        handler = RotatingFileHandler(
            target_path,
            maxBytes=2_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.addHandler(handler)
        return logger


def _logger():
    """Return the configured app logger."""
    return configure_debug_logger()


def log_event(
    action_type,
    action,
    status="ok",
    *,
    level="INFO",
    component="app",
    details="",
    **metadata,
):
    """Write one structured event line to the debug log."""
    # Convert a string level into the numeric level the stdlib logger expects.
    level_name = str(level or "INFO").upper()
    level_number = getattr(logging, level_name, logging.INFO)
    logger = _logger()

    # Base fields are always present so log parsing stays consistent.
    payload = {
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "level": logging.getLevelName(level_number),
        "action_type": _clean_value(action_type or "app"),
        "action": _clean_value(action or "unknown"),
        "status": _clean_value(status or "ok"),
        "component": _clean_value(component or "app"),
    }
    if details:
        payload["details"] = _clean_value(details)

    # Optional metadata gets appended as extra key=value fields.
    for key, value in metadata.items():
        if value is None:
            continue
        cleaned_key = _clean_key(key)
        # Keep the base keys stable and tuck colliding metadata under a prefixed field.
        if cleaned_key in payload:
            cleaned_key = f"meta_{cleaned_key}"
        payload[cleaned_key] = _clean_value(value)

    line = "\t".join(f"{key}={value}" for key, value in payload.items())
    logger.log(level_number, line)


def log_exception(
    action_type,
    action,
    error,
    *,
    component="app",
    details="",
    status="error",
    **metadata,
):
    """Log an exception as a structured event with shared metadata fields."""
    error_name = type(error).__name__
    fallback_details = details or str(error)
    log_event(  # Log the exception as one normalized structured event.
        action_type=action_type,
        action=action,
        status=status,
        level="ERROR",
        component=component,
        details=fallback_details,
        error_type=error_name,
        **metadata,
    )
