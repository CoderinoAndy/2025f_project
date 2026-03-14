# Model layer.
import atexit
import base64
import hashlib
import io
import ipaddress
import json
import os
import re
import textwrap
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:  # Optional until the extra requirements are installed.
    Image = None
    ImageDraw = None
    ImageFont = None
try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:  # Optional dependency for real HTML screenshots.
    PlaywrightError = RuntimeError
    PlaywrightTimeoutError = RuntimeError
    sync_playwright = None
from .db import (
    fetch_email_by_id,
    get_user_display_name,
    update_draft,
    update_email_ai_fields,
)
from .debug_logger import log_event
from .email_content import contains_common_mojibake, html_to_text, repair_body_text, repair_header_text

# This module handles both model calls and the async AI task plumbing.
# Shared runtime defaults and label sets for classification and summary work.
OLLAMA_API_URL_DEFAULT = "http://127.0.0.1:11434/api/chat"
OLLAMA_CLASSIFY_MODEL_DEFAULT = "qwen2.5:7b-instruct"
OLLAMA_SUMMARY_MODEL_DEFAULT = "mistral-small3.2:24b"
OLLAMA_DRAFT_MODEL_DEFAULT = OLLAMA_SUMMARY_MODEL_DEFAULT
OLLAMA_MODEL_DEFAULT = OLLAMA_SUMMARY_MODEL_DEFAULT
OLLAMA_TIMEOUT_SECONDS_DEFAULT = 45
OLLAMA_LONG_TASK_TIMEOUT_SECONDS_DEFAULT = 180
OLLAMA_KEEP_ALIVE_DEFAULT = "15m"
SUMMARY_MIN_CHARS_DEFAULT = 200
CLASSIFY_NUM_PREDICT_DEFAULT = 96
VISION_RENDER_MAX_CHARS_DEFAULT = 6000
VISION_RENDER_MAX_PAGES_DEFAULT = 2
VISION_RENDER_WRAP_WIDTH_DEFAULT = 92
VISION_RENDER_MAX_LINES_DEFAULT = 44
VISION_BROWSER_TIMEOUT_SECONDS_DEFAULT = 12.0
VISION_BROWSER_WAIT_MS_DEFAULT = 750
VISION_RENDER_VIEWPORT_WIDTH_DEFAULT = 1365
VISION_RENDER_PAGE_HEIGHT_DEFAULT = 1800
VISION_RENDER_CACHE_MAX_ITEMS = 128
VALID_CATEGORIES = {"urgent", "informational", "junk"}
VALID_EMAIL_TYPES = {"response-needed", "read-only", "junk", "junk-uncertain"}
LONG_OLLAMA_TASKS = {
    "draft",
    "revise",
    "draft_rewrite",
    "summarize",
    "summarize_rewrite",
}
TASK_MODEL_ENV_MAP = {
    "classify": "OLLAMA_CLASSIFY_MODEL",
    "draft": "OLLAMA_DRAFT_MODEL",
    "draft_plan": "OLLAMA_DRAFT_MODEL",
    "revise": "OLLAMA_DRAFT_MODEL",
    "draft_rewrite": "OLLAMA_DRAFT_MODEL",
    "summarize": "OLLAMA_SUMMARY_MODEL",
    "summarize_rewrite": "OLLAMA_SUMMARY_MODEL",
}
TASK_MODEL_DEFAULTS = {
    "classify": OLLAMA_CLASSIFY_MODEL_DEFAULT,
    "draft": OLLAMA_DRAFT_MODEL_DEFAULT,
    "draft_plan": OLLAMA_DRAFT_MODEL_DEFAULT,
    "revise": OLLAMA_DRAFT_MODEL_DEFAULT,
    "draft_rewrite": OLLAMA_DRAFT_MODEL_DEFAULT,
    "summarize": OLLAMA_SUMMARY_MODEL_DEFAULT,
    "summarize_rewrite": OLLAMA_SUMMARY_MODEL_DEFAULT,
}
TASK_NUM_PREDICT_ENV_MAP = {
    "classify": "OLLAMA_CLASSIFY_NUM_PREDICT",
    "draft": "OLLAMA_DRAFT_NUM_PREDICT",
    "draft_plan": "OLLAMA_DRAFT_NUM_PREDICT",
    "revise": "OLLAMA_DRAFT_NUM_PREDICT",
    "draft_rewrite": "OLLAMA_DRAFT_NUM_PREDICT",
    "summarize": "OLLAMA_SUMMARY_NUM_PREDICT",
    "summarize_rewrite": "OLLAMA_SUMMARY_NUM_PREDICT",
}
LOCALHOST_NAMES = {"localhost"}
OLLAMA_TAGS_CACHE_TTL_SECONDS = 300.0
OLLAMA_TAGS_CACHE = {"fetched_at": 0.0, "models": ()}
OLLAMA_TAGS_LOCK = threading.Lock()
VISION_RENDER_CACHE = {}
VISION_RENDER_CACHE_LOCK = threading.Lock()
VISION_BROWSER_STATE = {
    "playwright": None,
    "browser": None,
    "launch_options_key": None,
}
VISION_BROWSER_STATE_LOCK = threading.Lock()
VISION_BROWSER_RENDER_LOCK = threading.Lock()
VISION_BROWSER_CHANNEL_CANDIDATES = ("msedge", "chrome")
VISUAL_SUMMARY_TEXT_FAILURE_CHARS_DEFAULT = 120
VISUAL_SUMMARY_MIN_HTML_CHARS_DEFAULT = 180
VISUAL_SUMMARY_COMPLEXITY_THRESHOLD_DEFAULT = 2
VISUAL_SUMMARY_TEXT_OVERLAP_THRESHOLD_DEFAULT = 0.72
JUNK_LOW_CONFIDENCE_THRESHOLD = 0.78
LOCAL_USER_EMAIL = (os.getenv("LOCAL_USER_EMAIL") or "you@example.com").strip() or "you@example.com"
MAILBOX_OWNER_NAME_ENV = "LOCAL_USER_NAME"
# Sender/domain heuristics used to separate person-to-person mail from automated mail.
PERSONAL_EMAIL_DOMAINS = (
    "gmail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "protonmail.com",
    "proton.me",
)
AUTOMATED_SENDER_MARKERS = (
    "no-reply",
    "noreply",
    "do-not-reply",
    "donotreply",
    "newsletter",
    "digest",
    "notification",
    "notifications",
    "alerts",
    "updates",
    "announcements",
    "marketing",
    "promotions",
)
REPLY_CHAIN_PATTERNS = (
    r"\n-----\s*Original Message\s*-----",
    r"\nOn\s.+?wrote:",
    r"\nFrom:\s.+\nSent:\s.+\nTo:\s.+\nSubject:\s.+",
)
# Marker sets for filtering unusable summaries and stripping newsletter/footer boilerplate.
SUMMARY_FAILURE_MARKERS = (
    "summary unavailable",
    "summary generation failed",
    "unable to summarize",
    "unable to provide a summary",
    "unable to interpret",
    "unable to understand",
    "i cannot summarize",
    "i can't summarize",
    "cannot provide a summary",
    "can't provide a summary",
    "insufficient information",
    "not enough information",
    "no content provided",
    "no email content",
)
# Phrases frequently found in footer/utility text that should not dominate summaries.
SUMMARY_NOISE_MARKERS = (
    "is this email difficult to read",
    "view in browser",
    "open in browser",
    "read online",
    "sign up here",
    "subscribe here",
    "subscribe now",
    "if you're not subscribed",
    "if you are not subscribed",
    "manage preferences",
    "manage email preferences",
    "email preferences",
    "manage subscription",
    "unsubscribe",
    "privacy policy",
    "privacy notice",
    "cookie notice",
    "terms of service",
    "terms of use",
    "subscriber id",
    "subscription id",
    "all rights reserved",
    "you are receiving this email",
    "this email was sent to",
    "add us to your address book",
    "view online",
    "presented by",
    "together with",
    "notification emails",
    "remove your email",
    "intended for",
    "want to keep receiving",
    "clicking on newsletter links",
    "stop receiving this newsletter",
    "here's what these numbers mean",
    "data is provided by",
)
SUMMARY_UTILITY_MARKERS = (
    "read more",
    "alerts center",
    "contact us",
    "customer service",
    "for further assistance",
    "you are currently subscribed",
    "if you're not subscribed",
    "if you are not subscribed",
    "sign up here",
    "support@",
    "privacy policy",
    "cookie policy",
    "copyright",
    "dow jones",
    "route 1",
    "monmouth junction",
    "sponsored by",
    "is this email difficult to read",
    "view online",
    "presented by",
    "together with",
    "notification emails",
    "remove your email",
    "intended for",
    "view job",
    "see jobs",
    "top applicant",
    "try premium",
    "here's what these numbers mean",
    "data is provided by",
    "want to keep receiving",
    "clicking on newsletter links",
)
# Common model-inserted lines that are not grounded in the source email.
SUMMARY_HALLUCINATION_MARKERS = (
    "if you're not subscribed",
    "if you are not subscribed",
    "sign up here",
    "subscribe here",
    "subscribe now",
    "click here to subscribe",
)
# Stopwords removed before token-overlap grounding checks.
SUMMARY_STOPWORDS = {
    "the",
    "and",
    "for",
    "that",
    "with",
    "this",
    "from",
    "your",
    "you",
    "are",
    "was",
    "were",
    "been",
    "have",
    "has",
    "had",
    "into",
    "about",
    "their",
    "there",
    "will",
    "would",
    "could",
    "should",
    "they",
    "them",
    "then",
    "than",
    "also",
    "just",
    "more",
    "most",
    "very",
    "what",
    "when",
    "where",
    "which",
    "while",
    "after",
    "before",
    "over",
    "under",
    "between",
    "because",
    "through",
    "these",
    "those",
    "within",
    "without",
    "about",
    "into",
    "onto",
    "upon",
}
# Regex-level footer patterns used for sentence/fragment noise filtering.
FOOTER_NOISE_REGEX_PATTERNS = (
    r"\bmanage\s+(?:email\s+)?preferences\b",
    r"\bemail\s+preferences\b",
    r"\bmanage\s+subscription\b",
    r"\b(?:sign\s+up|subscribe)(?:\s+(?:here|now))?\b",
    r"\bif\s+you(?:'re| are)\s+not\s+subscribed\b",
    r"\bunsubscribe\b",
    r"\bprivacy\s+(?:policy|notice)\b",
    r"\bcookie\s+notice\b",
    r"\bterms\s+of\s+(?:service|use)\b",
    r"\bview\s+in\s+browser\b",
    r"\bopen\s+in\s+browser\b",
    r"\bread\s+online\b",
    r"\byou\s+are\s+receiving\s+this\s+email\b",
    r"\bthis\s+email\s+was\s+sent\s+to\b",
    r"\badd\s+us\s+to\s+your\s+address\s+book\b",
    r"\bview\s+online\b",
    r"\bpresented\s+by\b",
    r"\btogether\s+with\b",
    r"\bnotification\s+emails\b",
    r"\bremove\s+your\s+email\b",
    r"\bintended\s+for\b",
    r"\bwant\s+to\s+keep\s+receiving\b",
    r"\bclicking\s+on\s+newsletter\s+links\b",
    r"\bstop\s+receiving\s+this\s+newsletter\b",
    r"\bhere'?s\s+what\s+these\s+numbers\s+mean\b",
    r"\bdata\s+is\s+provided\s+by\b",
    r"\ball\s+rights\s+reserved\b",
    r"\b(?:subscriber|subscription|member|customer)\s*id\s*[:#]?\s*[a-z0-9\-]{5,}\b",
    r"\bis\s+this\s+email\s+difficult\s+to\s+read\??\b",
    r"\bread\s+more\b",
    r"\balerts?\s+center\b",
    r"\bcontact\s+us\b",
    r"\bcustomer\s+service\b",
    r"\bfor\s+further\s+assistance\b",
    r"\byou\s+are\s+currently\s+subscribed(?:\s+as)?\b",
    r"\bsponsored\s+by\b",
)
# Draft-quality failure markers plus request-language cues for actionable detection.
DRAFT_FAILURE_MARKERS = (
    "as an ai",
    "unable to draft",
    "unable to write",
    "cannot draft",
    "can't draft",
    "insufficient information",
    "not enough information",
)
DRAFT_OBSERVER_MARKERS = (
    "the main points i found were",
    "the main point i found was",
    "here are the main points",
    "the key points are",
    "the main detail i noted is",
    "i reviewed the information and captured the key points",
    "the sender is asking",
    "this email is about",
    "here's a draft",
)
DRAFT_GENERIC_MARKERS = (
    "thanks for your email about",
    "thanks for sharing the update about",
    "i appreciate the update",
    "i have the details and will send a complete response",
    "i will send a complete response",
    "i will follow up shortly",
    "i'll follow up shortly",
    "i will get back to you",
    "i'll get back to you",
    "let me know if you want any follow-up action from my side",
)
DIRECT_REPLY_MARKERS = (
    "i can",
    "i can't",
    "i cannot",
    "i will",
    "i'll",
    "i am",
    "i'm",
    "we can",
    "we will",
    "works for me",
    "sounds good",
    "confirmed",
    "yes",
    "no",
    "happy to",
    "available",
    "unavailable",
    "please send",
    "please share",
)
REPLY_PLAN_RESPONSE_MODES = {"answer_or_confirm", "clarify", "acknowledge_only"}
REPLY_PLAN_TONES = {"professional", "friendly", "urgent", "informational", "apologetic"}
REQUEST_SENTENCE_MARKERS = (
    "please reply",
    "please confirm",
    "can you",
    "could you",
    "would you",
    "let me know",
    "respond by",
    "action required",
    "rsvp",
    "approval needed",
    "deadline",
    "asap",
)
SUMMARY_PLEASANTRY_MARKERS = (
    "hope you're well",
    "hope you are well",
    "hope you're doing well",
    "hope you are doing well",
    "i hope this email finds you well",
    "trust you're well",
    "trust you are well",
)
SUMMARY_CLOSING_MARKERS = (
    "thanks again",
    "thank you again",
    "best,",
    "best regards",
    "regards,",
    "sincerely,",
    "cheers,",
)
SUMMARY_TASK_MARKERS = (
    "please ",
    "need everyone to",
    "need you to",
    "make sure",
    "let me know",
    "confirm",
    "reply",
    "respond",
    "submit",
    "send",
    "share",
    "review",
    "complete",
    "enter",
    "follow up",
    "stay focused",
    "approve",
)
SUMMARY_OVERVIEW_MARKERS = (
    "thank",
    "appreciate",
    "update",
    "meeting",
    "deliverable",
    "progress",
    "project",
    "timeline",
    "status",
)
JUNK_STRONG_MARKERS = (
    "viagra",
    "casino bonus",
    "adult",
    "xxx",
    "lottery",
    "wire transfer",
    "gift card",
    "inheritance",
    "beneficiary",
    "sugar daddy",
)
JUNK_MONEY_BAIT_MARKERS = (
    "crypto giveaway",
    "crypto presale",
    "guaranteed income",
    "guaranteed returns",
    "claim your prize",
    "winner selected",
    "cash prize",
    "bonus reward",
    "refund due",
    "debt relief",
    "passive income",
    "investment opportunity",
    "double your money",
    "earn from home",
    "work from home",
)
JUNK_PROMOTION_MARKERS = (
    "% off",
    "percent off",
    "limited time offer",
    "exclusive deal",
    "special promotion",
    "special offer",
    "discount",
    "promo code",
    "coupon",
    "coupon code",
    "free shipping",
    "flash sale",
    "sale ends",
    "deal of the day",
    "member exclusive",
    "members only",
    "recommended for you",
    "new arrivals",
    "new collection",
    "latest collection",
    "fresh styles",
    "featured picks",
    "featured products",
    "best sellers",
    "gift guide",
    "buy one get one",
    "bogo",
    "bundle and save",
    "save up to",
    "save big",
    "earn up to",
    "discover",
    "shop the collection",
    "shop the sale",
    "shop our",
    "refer friends",
    "trending products",
    "top products",
    "presale access",
    "pre-sale access",
    "premium upgrade",
    "upgrade today",
    "buy now",
    "shop now",
    "order now",
    "claim now",
    "redeem now",
    "unlock savings",
    "free trial",
    "free gift",
    "no obligation",
    "lowest price",
    "clearance",
    "price drop",
)
WEAK_PROMOTION_MARKERS = {
    "discover",
    "featured picks",
    "featured products",
    "new arrivals",
    "new collection",
    "latest collection",
    "fresh styles",
    "best sellers",
    "shop our",
}
JUNK_EDITORIAL_MARKERS = (
    "top stories",
    "latest news",
    "morning briefing",
    "daily briefing",
    "weekly briefing",
    "daily digest",
    "weekly digest",
    "newsletter",
    "roundup",
    "analysis",
    "week in review",
    "today in",
    "read more",
    "podcast",
    "headline",
    "opinion",
    "coverage",
)
JUNK_PRESSURE_MARKERS = (
    "act now",
    "last chance",
    "don't miss out",
    "ends soon",
    "ends tonight",
    "expires soon",
    "today only",
    "only a few left",
    "urgent action",
    "immediate action",
    "final notice",
    "before it's gone",
)
JUNK_ACCOUNT_BAIT_MARKERS = (
    "verify your account",
    "confirm your account",
    "account suspended",
    "account on hold",
    "unusual activity",
    "secure your account",
    "payment failed",
    "update your payment",
    "billing issue",
    "verify your identity",
    "password expires",
)
JUNK_BULK_FOOTER_MARKERS = (
    "unsubscribe",
    "manage preferences",
    "manage subscription",
    "view in browser",
    "open in browser",
    "why am i getting this",
    "advertisement",
    "sponsored",
    "opt out",
)
JUNK_LEADGEN_MARKERS = (
    "quick question",
    "following up",
    "circle back",
    "book a demo",
    "schedule a demo",
    "increase your revenue",
    "generate more leads",
    "grow your pipeline",
    "sales team",
    "calendar link",
)
TRANSACTIONAL_HAM_MARKERS = (
    "receipt",
    "order confirmation",
    "order shipped",
    "delivery update",
    "statement is ready",
    "appointment",
    "meeting",
    "interview",
    "class schedule",
    "verification code",
    "one-time code",
    "payment receipt",
)
EMAIL_ADDRESS_PATTERN = re.compile(r"([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})")
SUMMARY_LIST_MARKER_PATTERN = re.compile(r"^(?:[-*]|\d+[.)])\s+")
SUMMARY_MAX_CHARS = 720
DRAFT_MIN_CHARS = 20
DRAFT_MIN_ACTIONABLE_CHARS = 60
READ_TIME_PREFIX_PATTERN = re.compile(
    r"^\d+\s*(?:-\s*\d+)?(?:\s*-\s*|\s+)(?:min(?:ute)?s?)\s+read\b[:\-]?\s*",
    re.IGNORECASE,
)
SENTENCE_ABBREVIATION_ENDINGS = (
    "mr.",
    "mrs.",
    "ms.",
    "dr.",
    "prof.",
    "sr.",
    "jr.",
    "st.",
    "vs.",
    "etc.",
    "e.g.",
    "i.e.",
    "u.s.",
    "u.k.",
    "fig.",
    "no.",
    "dept.",
)
HTML_VISUAL_COMPLEXITY_PATTERN = re.compile(
    r"<\s*(?:img|picture|svg|canvas|video|audio|table|tr|td|th|iframe|form|input|button|style)\b"
    r"|cid:"
    r"|data:image/"
    r"|background(?:-image)?\s*:"
    r"|display\s*:\s*(?:grid|flex|table)",
    re.IGNORECASE,
)


def _utc_now():
    """Utc now.
    """
    # Shared helper for this file.
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _action_log_path():
    """Action log path.
    """
    # Log this here so it is easier to trace later.
    configured = (os.getenv("AI_ACTION_LOG_PATH") or "").strip()
    if configured:
        return Path(configured)
    return Path("instance/ai_actions.txt")


def _one_line(value):
    """One line.
    """
    # Shared helper for this file.
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return text[:500]


def _log_action(task, status, email_id=None, detail=""):
    """Log action.
    """
    try:
        log_event(
            action_type="ai_task",
            action=task,
            status=status,
            level="ERROR" if str(status).lower() == "error" else "INFO",
            component="ollama_client",
            details=detail,
            email_id=email_id if email_id is not None else "",
        )
    except Exception:
        pass

    line = (
        f"{_utc_now()}\ttask={task}\tstatus={status}"
        f"\temail_id={email_id if email_id is not None else '-'}"
        f"\tdetail={_one_line(detail)}"
    )
    try:
        target = _action_log_path()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")
    except OSError:
        # If logging fails, do not break the request path.
        pass


def log_ai_event(task, status, email_id=None, detail=""):
    """Log AI event.
    """
    # Log this here so it is easier to trace later.
    _log_action(task=task, status=status, email_id=email_id, detail=detail)


def _log_performance(action, duration_ms, *, email_id=None, **metadata):
    """Write a structured timing event for the current AI pipeline step."""
    try:
        log_event(
            action_type="performance",
            action=action,
            status="ok",
            component="ollama_client",
            email_id=email_id if email_id is not None else "",
            duration_ms=max(0, int(duration_ms)),
            **metadata,
        )
    except Exception:
        pass


def _api_url():
    """Api url.
    """
    # Resolve the API URL, with a safe fallback if config is missing.
    value = (os.getenv("OLLAMA_API_URL") or OLLAMA_API_URL_DEFAULT).strip()
    return value or OLLAMA_API_URL_DEFAULT


def _env_flag(env_names, default=False):
    """Return the first boolean-like env var value from the provided names."""
    truthy = {"1", "true", "yes", "on"}
    falsy = {"0", "false", "no", "off"}
    for env_name in env_names:
        raw = (os.getenv(env_name) or "").strip().lower()
        if not raw:
            continue
        if raw in truthy:
            return True
        if raw in falsy:
            return False
    return bool(default)


def _env_int(name, default, *, minimum=None, maximum=None):
    """Return an integer env var with clamped bounds."""
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except (TypeError, ValueError):
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def _env_float(name, default, *, minimum=None, maximum=None):
    """Return a float env var with clamped bounds."""
    raw = (os.getenv(name) or "").strip()
    try:
        value = float(raw) if raw else float(default)
    except (TypeError, ValueError):
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def _api_url_candidates():
    """Return loopback-safe Ollama endpoint candidates."""
    primary = _api_url()
    parsed = urlparse(primary)
    candidates = [primary]
    if not _is_loopback_host(parsed.hostname):
        return candidates

    netloc = parsed.netloc
    suffix = ""
    if "@" in netloc:
        suffix = netloc.split("@", 1)[0] + "@"
        netloc = netloc.split("@", 1)[1]
    port = f":{parsed.port}" if parsed.port else ""
    host_variants = ("127.0.0.1", "localhost")
    for host in host_variants:
        if parsed.hostname == host:
            continue
        candidate = parsed._replace(netloc=f"{suffix}{host}{port}").geturl()
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _tags_url_candidates(api_urls=None):
    """Return candidate Ollama `/api/tags` URLs matching the configured chat endpoint."""
    candidates = []
    for api_url in api_urls or _api_url_candidates():
        parsed = urlparse(api_url)
        candidate = parsed._replace(path="/api/tags", params="", query="", fragment="").geturl()
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _available_ollama_models(api_urls=None, force_refresh=False):
    """Return locally installed Ollama model names from `/api/tags`."""
    now = time.time()
    with OLLAMA_TAGS_LOCK:
        cached_models = tuple(OLLAMA_TAGS_CACHE.get("models") or ())
        fetched_at = float(OLLAMA_TAGS_CACHE.get("fetched_at") or 0.0)
        if cached_models and not force_refresh and now - fetched_at < OLLAMA_TAGS_CACHE_TTL_SECONDS:
            return cached_models

    models = ()
    tags_timeout = _tags_timeout_seconds()
    for tags_url in _tags_url_candidates(api_urls=api_urls):
        request_obj = urllib.request.Request(tags_url, method="GET")
        try:
            with urllib.request.urlopen(request_obj, timeout=tags_timeout) as response:
                parsed = json.loads(response.read().decode("utf-8"))
            models = tuple(
                model.get("name")
                for model in (parsed.get("models") or [])
                if str(model.get("name") or "").strip()
            )
            if models:
                break
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            continue

    with OLLAMA_TAGS_LOCK:
        OLLAMA_TAGS_CACHE["fetched_at"] = now
        OLLAMA_TAGS_CACHE["models"] = models
    return models


def _strict_model_resolution(task=None):
    """Return True when missing requested models should fail instead of silently substituting."""
    task_name = str(task or "").strip().lower()
    env_names = []
    if task_name:
        env_names.append(f"OLLAMA_{task_name.upper()}_STRICT_MODEL_RESOLUTION")
    env_names.append("OLLAMA_STRICT_MODEL_RESOLUTION")
    return _env_flag(env_names, default=False)


def _resolve_model_selection(task=None, api_urls=None):
    """Resolve the requested model and describe any substitution decision."""
    requested_model = _model_name(task=task)
    available_models = _available_ollama_models(api_urls=api_urls)
    strict_resolution = _strict_model_resolution(task=task)
    selection = {
        "requested_model": requested_model,
        "resolved_model": requested_model,
        "available_models": tuple(available_models or ()),
        "substituted": False,
        "reason": "",
        "strict": strict_resolution,
    }
    if not available_models or requested_model in available_models:
        return selection

    requested_family = requested_model.split(":", 1)[0].strip().lower()
    family_matches = [
        candidate
        for candidate in available_models
        if candidate.split(":", 1)[0].strip().lower() == requested_family
    ]
    fallback_model = family_matches[0] if family_matches else available_models[0]
    reason = "family_match" if family_matches else "first_available"
    if strict_resolution:
        selection["resolved_model"] = None
        selection["reason"] = f"missing_requested_model_{reason}"
        return selection

    selection["resolved_model"] = fallback_model
    selection["substituted"] = True
    selection["reason"] = reason
    return selection


def _resolved_model_name(task=None, api_urls=None):
    """Resolve a requested Ollama model to an installed local model when needed."""
    return _resolve_model_selection(task=task, api_urls=api_urls).get("resolved_model")


def _model_name(task=None):
    """Model name.
    """
    # Prefer task-specific model overrides for the latency-sensitive flows.
    task_name = str(task or "").strip().lower()
    task_env = TASK_MODEL_ENV_MAP.get(task_name)
    if task_env:
        value = (os.getenv(task_env) or "").strip()
        if value:
            return value
    global_override = (os.getenv("OLLAMA_MODEL") or "").strip()
    if global_override:
        return global_override
    task_default = TASK_MODEL_DEFAULTS.get(task_name)
    if task_default:
        return task_default
    return OLLAMA_MODEL_DEFAULT


def _num_predict_for_task(task, default):
    """Return the configured generation-token budget for a task."""
    task_name = str(task or "").strip().lower()
    env_name = TASK_NUM_PREDICT_ENV_MAP.get(task_name)
    if not env_name:
        return max(32, int(default or 32))
    return _env_int(env_name, int(default or 32), minimum=32, maximum=2048)


def _timeout_seconds(task=None):
    """Timeout seconds.
    """
    # Give slower local generations a bigger default timeout than short classification calls.
    task_name = str(task or "").strip().lower()
    env_name = "OLLAMA_LONG_TASK_TIMEOUT_SECONDS" if task_name in LONG_OLLAMA_TASKS else "OLLAMA_TIMEOUT_SECONDS"
    default_value = (
        OLLAMA_LONG_TASK_TIMEOUT_SECONDS_DEFAULT
        if task_name in LONG_OLLAMA_TASKS
        else OLLAMA_TIMEOUT_SECONDS_DEFAULT
    )
    raw = (os.getenv(env_name) or "").strip()
    try:
        parsed = float(raw) if raw else float(default_value)
    except ValueError:
        parsed = float(default_value)
    return max(5.0, min(300.0, parsed))


def _keep_alive_value(task=None):
    """Return Ollama keep-alive hint to reduce model cold starts."""
    task_name = str(task or "").strip().lower()
    env_names = []
    if task_name:
        env_names.append(f"OLLAMA_{task_name.upper()}_KEEP_ALIVE")
    env_names.append("OLLAMA_KEEP_ALIVE")
    for env_name in env_names:
        value = (os.getenv(env_name) or "").strip()
        if value:
            return value
    return OLLAMA_KEEP_ALIVE_DEFAULT


def _tags_timeout_seconds():
    """Return timeout for lightweight `/api/tags` lookups."""
    raw = (os.getenv("OLLAMA_TAGS_TIMEOUT_SECONDS") or "").strip()
    try:
        parsed = float(raw) if raw else 3.0
    except ValueError:
        parsed = 3.0
    return max(1.0, min(10.0, parsed))


def _summary_min_chars():
    """Summary min chars.
    """
    # Shared helper for this file.
    return _env_int(
        "OLLAMA_SUMMARY_MIN_CHARS",
        SUMMARY_MIN_CHARS_DEFAULT,
        minimum=50,
        maximum=5000,
    )


def _vision_render_max_chars():
    """Return the maximum cleaned body chars rendered into vision pages."""
    return _env_int(
        "OLLAMA_VISION_MAX_CHARS",
        VISION_RENDER_MAX_CHARS_DEFAULT,
        minimum=800,
        maximum=30000,
    )


def _vision_render_max_pages():
    """Return the maximum number of rendered email images sent to Ollama."""
    return _env_int(
        "OLLAMA_VISION_MAX_PAGES",
        VISION_RENDER_MAX_PAGES_DEFAULT,
        minimum=1,
        maximum=12,
    )


def _vision_render_wrap_width():
    """Return the wrap width used by rendered email pages."""
    return _env_int(
        "OLLAMA_VISION_WRAP_WIDTH",
        VISION_RENDER_WRAP_WIDTH_DEFAULT,
        minimum=40,
        maximum=140,
    )


def _vision_render_max_lines():
    """Return the number of content lines rendered on each email image page."""
    return _env_int(
        "OLLAMA_VISION_MAX_LINES",
        VISION_RENDER_MAX_LINES_DEFAULT,
        minimum=18,
        maximum=70,
    )


def _vision_render_available():
    """Return True when true HTML screenshot dependencies are available."""
    return sync_playwright is not None


def _vision_browser_timeout_seconds():
    """Return timeout for headless-browser HTML rendering."""
    return _env_float(
        "OLLAMA_VISION_BROWSER_TIMEOUT_SECONDS",
        VISION_BROWSER_TIMEOUT_SECONDS_DEFAULT,
        minimum=3.0,
        maximum=30.0,
    )


def _vision_browser_wait_ms():
    """Return the post-load settle delay for HTML screenshot rendering."""
    return _env_int(
        "OLLAMA_VISION_BROWSER_WAIT_MS",
        VISION_BROWSER_WAIT_MS_DEFAULT,
        minimum=0,
        maximum=5000,
    )


def _vision_render_viewport_width():
    """Return viewport width used for HTML email screenshots."""
    return _env_int(
        "OLLAMA_VISION_VIEWPORT_WIDTH",
        VISION_RENDER_VIEWPORT_WIDTH_DEFAULT,
        minimum=640,
        maximum=2200,
    )


def _vision_render_page_height():
    """Return max pixel height for each attached screenshot page."""
    return _env_int(
        "OLLAMA_VISION_PAGE_HEIGHT",
        VISION_RENDER_PAGE_HEIGHT_DEFAULT,
        minimum=600,
        maximum=3000,
    )


def _vision_browser_channel():
    """Return an optional browser channel for Playwright launch."""
    return (os.getenv("OLLAMA_VISION_BROWSER_CHANNEL") or "").strip()


def _vision_browser_executable_path():
    """Return an optional browser executable path for Playwright launch."""
    return (os.getenv("OLLAMA_VISION_BROWSER_EXECUTABLE_PATH") or "").strip()


def _visual_summary_enabled():
    """Return whether summary generation may escalate to HTML screenshots."""
    return _env_flag(["OLLAMA_VISUAL_SUMMARY_ENABLED"], default=True)


def _visual_summary_text_failure_chars():
    """Return the text-length threshold that counts as failed extraction."""
    return _env_int(
        "OLLAMA_VISUAL_SUMMARY_TEXT_FAILURE_CHARS",
        VISUAL_SUMMARY_TEXT_FAILURE_CHARS_DEFAULT,
        minimum=20,
        maximum=2000,
    )


def _visual_summary_min_html_chars():
    """Return the minimum HTML-derived text size that justifies visual fallback checks."""
    return _env_int(
        "OLLAMA_VISUAL_SUMMARY_MIN_HTML_CHARS",
        VISUAL_SUMMARY_MIN_HTML_CHARS_DEFAULT,
        minimum=40,
        maximum=6000,
    )


def _visual_summary_complexity_threshold():
    """Return the number of layout signals required before treating layout as important."""
    return _env_int(
        "OLLAMA_VISUAL_SUMMARY_COMPLEXITY_THRESHOLD",
        VISUAL_SUMMARY_COMPLEXITY_THRESHOLD_DEFAULT,
        minimum=1,
        maximum=8,
    )


def _visual_summary_text_overlap_threshold():
    """Return the maximum text overlap below which HTML and text diverge enough to escalate."""
    return _env_float(
        "OLLAMA_VISUAL_SUMMARY_TEXT_OVERLAP_THRESHOLD",
        VISUAL_SUMMARY_TEXT_OVERLAP_THRESHOLD_DEFAULT,
        minimum=0.2,
        maximum=0.98,
    )


def _visual_context_allowed(task=None):
    """Return True when a task should pay the cost of real visual context."""
    return str(task or "").strip().lower() != "classify"


def _vision_render_cache_key(email_data):
    """Return a stable cache key for rendered email page images."""
    raw_html = str(email_data.get("body_html") or "").strip()
    payload = {
        "sender": _normalized_header_text(email_data.get("sender") or ""),
        "title": _normalized_header_text(email_data.get("title") or ""),
        "recipients": _normalized_header_text(email_data.get("recipients") or ""),
        "cc": _normalized_header_text(email_data.get("cc") or ""),
        "type": _compact_text(email_data.get("type") or ""),
        "priority": int(email_data.get("priority") or 1),
        "received_at": _compact_text(email_data.get("received_at") or email_data.get("date") or ""),
        "body_html": raw_html,
        "viewport_width": _vision_render_viewport_width(),
        "page_height": _vision_render_page_height(),
        "max_pages": _vision_render_max_pages(),
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _vision_metadata_block(email_data):
    """Return compact sender/subject metadata for multimodal prompts."""
    return (
        "Sender and subject metadata:\n"
        f"- From: {_normalized_header_text(email_data.get('sender') or '(unknown sender)')}\n"
        f"- Subject: {_normalized_header_text(email_data.get('title') or '(No subject)')}\n"
        f"- To: {_normalized_header_text(email_data.get('recipients') or '(unknown)')}\n"
        f"- Cc: {_normalized_header_text(email_data.get('cc') or '(none)')}\n"
        f"- Mailbox type: {_compact_text(email_data.get('type') or '(unknown)')}\n"
        f"- Priority: {int(email_data.get('priority') or 1)}\n"
        f"- Received: {_compact_text(email_data.get('received_at') or email_data.get('date') or '(unknown)')}"
    )


def _source_text_limit(task=None, text_max_chars=None):
    """Return a capped source-text budget for prompt payloads."""
    if text_max_chars is not None:
        try:
            parsed = int(text_max_chars)
        except (TypeError, ValueError):
            parsed = 0
        if parsed > 0:
            return max(800, min(12000, parsed))

    task_name = str(task or "").strip().lower()
    if task_name == "summarize":
        return 5200
    if task_name == "draft_plan":
        return 2600
    if task_name == "classify":
        return 1200
    return 3600


def _source_text_for_user_message(email_data, task=None, text_max_chars=None):
    """Return cleaned source text for model prompts."""
    task_name = str(task or "").strip().lower()
    if task_name == "summarize":
        summary_context = _body_for_context(
            email_data,
            max_chars=_source_text_limit(task=task, text_max_chars=text_max_chars),
            max_sentences=10,
        )
        if summary_context:
            return summary_context
    return _clean_body_for_prompt(
        email_data,
        max_chars=_source_text_limit(task=task, text_max_chars=text_max_chars),
    ) or _vision_render_body_text(email_data)


def _html_requires_visual_context(email_data):
    """Return True when raw HTML is different enough to justify an image fallback."""
    if not isinstance(email_data, dict):
        return False
    raw_html = str(email_data.get("body_html") or "").strip()
    if not raw_html:
        return False
    if HTML_VISUAL_COMPLEXITY_PATTERN.search(raw_html):
        return True
    html_text = _compact_text(html_to_text(raw_html))
    plain_text = _compact_text(_email_body_text(email_data))
    if not html_text:
        return False
    if not plain_text:
        return True
    return len(html_text) >= 160 and _token_overlap_ratio(html_text, plain_text) < 0.72


def _summary_visual_decision(email_data):
    """Return whether summary generation should escalate from text to screenshots."""
    if not _visual_summary_enabled():
        return {"should_escalate": False, "reason": "visual_summary_disabled"}
    if not isinstance(email_data, dict):
        return {"should_escalate": False, "reason": "invalid_email"}

    raw_html = str(email_data.get("body_html") or "").strip()
    if not raw_html:
        return {"should_escalate": False, "reason": "no_html"}

    html_text = _compact_text(html_to_text(raw_html))
    plain_text = _compact_text(_email_body_text(email_data))
    html_chars = len(html_text)
    plain_chars = len(plain_text)
    complexity_hits = len(list(HTML_VISUAL_COMPLEXITY_PATTERN.finditer(raw_html)))
    text_failure = (
        html_chars >= _visual_summary_min_html_chars()
        and plain_chars < _visual_summary_text_failure_chars()
    )
    overlap = _token_overlap_ratio(html_text, plain_text) if html_text and plain_text else 0.0
    layout_essential = complexity_hits >= _visual_summary_complexity_threshold()
    text_diverges = (
        html_chars >= _visual_summary_min_html_chars()
        and plain_chars >= _visual_summary_text_failure_chars()
        and overlap < _visual_summary_text_overlap_threshold()
    )
    should_escalate = bool(text_failure or (layout_essential and text_diverges))
    if text_failure:
        reason = "text_extraction_failed"
    elif layout_essential and text_diverges:
        reason = "layout_essential"
    else:
        reason = "text_sufficient"
    return {
        "should_escalate": should_escalate,
        "reason": reason,
        "html_chars": html_chars,
        "plain_chars": plain_chars,
        "complexity_hits": complexity_hits,
        "text_overlap": round(overlap, 4),
    }


def _vision_render_body_text(email_data):
    """Return the full email body text rendered into screenshot-like pages."""
    body = _email_body_text(email_data).replace("\r\n", "\n").replace("\r", "\n").strip()
    body = re.sub(r"[ \t]+", " ", body)
    body = re.sub(r"\n{3,}", "\n\n", body)
    max_chars = _vision_render_max_chars()
    if len(body) > max_chars:
        body = f"{body[: max_chars - 3].rstrip()}..."
    if body:
        return body
    title = _normalized_header_text(email_data.get("title") or "")
    return title or "(No usable email body text was available.)"


def _html_document_for_visual_render(email_data):
    """Return a browser-renderable HTML document for true screenshot capture."""
    raw_html = str(email_data.get("body_html") or "").strip()
    if not raw_html:
        return ""
    sanitized = re.sub(
        r"<script\b[^>]*>.*?</script>",
        "",
        raw_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    visual_reset = (
        "<style>"
        "html,body{margin:0;padding:0;background:#ffffff !important;}"
        "body{min-height:100vh;}"
        "img{max-width:100%;height:auto;}"
        "table{border-collapse:collapse;}"
        "</style>"
    )
    if re.search(r"<html\b", sanitized, flags=re.IGNORECASE):
        if re.search(r"</head>", sanitized, flags=re.IGNORECASE):
            return re.sub(
                r"</head>",
                f"{visual_reset}</head>",
                sanitized,
                count=1,
                flags=re.IGNORECASE,
            )
        return re.sub(
            r"<html\b[^>]*>",
            lambda match: f"{match.group(0)}<head>{visual_reset}</head>",
            sanitized,
            count=1,
            flags=re.IGNORECASE,
        )
    return (
        "<!doctype html><html><head>"
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"{visual_reset}"
        "</head><body>"
        f"{sanitized}"
        "</body></html>"
    )


def _vision_browser_launch_options():
    """Return browser launch options to try for Playwright-based screenshots."""
    executable_path = _vision_browser_executable_path()
    explicit_channel = _vision_browser_channel()
    options = []
    if executable_path:
        options.append({"executable_path": executable_path})
    elif explicit_channel:
        options.append({"channel": explicit_channel})
    options.append({})
    if not executable_path and not explicit_channel:
        options.extend({"channel": channel} for channel in VISION_BROWSER_CHANNEL_CANDIDATES)

    deduped = []
    seen = set()
    for option in options:
        key = tuple(sorted(option.items()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(option)
    return deduped


def _close_vision_browser_locked():
    """Close the shared Playwright browser/session held in module state."""
    browser = VISION_BROWSER_STATE.get("browser")
    playwright = VISION_BROWSER_STATE.get("playwright")
    VISION_BROWSER_STATE["browser"] = None
    VISION_BROWSER_STATE["playwright"] = None
    VISION_BROWSER_STATE["launch_options_key"] = None
    if browser is not None:
        try:
            browser.close()
        except Exception:
            pass
    if playwright is not None:
        try:
            playwright.stop()
        except Exception:
            pass


def _shutdown_vision_browser():
    """Best-effort process shutdown hook for the persistent screenshot browser."""
    with VISION_BROWSER_STATE_LOCK:
        _close_vision_browser_locked()


atexit.register(_shutdown_vision_browser)


def _get_or_launch_vision_browser():
    """Return a persistent Playwright browser plus any launch errors."""
    launch_options = _vision_browser_launch_options()
    launch_key = tuple(tuple(sorted(option.items())) for option in launch_options)
    launch_errors = []
    with VISION_BROWSER_STATE_LOCK:
        browser = VISION_BROWSER_STATE.get("browser")
        current_key = VISION_BROWSER_STATE.get("launch_options_key")
        if browser is not None and current_key == launch_key:
            return browser, launch_errors
        if current_key != launch_key:
            _close_vision_browser_locked()
        if VISION_BROWSER_STATE.get("playwright") is None:
            VISION_BROWSER_STATE["playwright"] = sync_playwright().start()
        playwright = VISION_BROWSER_STATE["playwright"]
        for option in launch_options:
            try:
                browser = playwright.chromium.launch(headless=True, **option)
                VISION_BROWSER_STATE["browser"] = browser
                VISION_BROWSER_STATE["launch_options_key"] = launch_key
                return browser, launch_errors
            except Exception as exc:  # pragma: no cover - exercised by the fallback logging tests.
                launch_errors.append(f"{option or {'default': True}}:{exc}")
        _close_vision_browser_locked()
        return None, launch_errors


def _encode_png_bytes(image_bytes):
    """Encode raw PNG bytes as base64 for Ollama image payloads."""
    return base64.b64encode(image_bytes).decode("ascii")


def _split_rendered_email_pages(image_bytes):
    """Split a tall HTML screenshot into multiple page images."""
    if not image_bytes:
        return []
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            screenshot = image.convert("RGB")
            width, height = screenshot.size
            page_height = _vision_render_page_height()
            max_pages = _vision_render_max_pages()
            if height <= page_height:
                return [_encode_png_bytes(image_bytes)]

            pages = []
            for index in range(max_pages):
                top = index * page_height
                if top >= height:
                    break
                bottom = min(height, top + page_height)
                cropped = screenshot.crop((0, top, width, bottom))
                buffer = io.BytesIO()
                cropped.save(buffer, format="PNG", optimize=True, compress_level=9)
                pages.append(_encode_png_bytes(buffer.getvalue()))
                if bottom >= height:
                    break
            return pages
    except OSError:
        return [_encode_png_bytes(image_bytes)]


def _capture_html_email_pages(page, *, viewport_width, segment_height, max_pages, wait_ms):
    """Capture only the visible HTML segments we may attach to the vision model."""
    full_height = int(
        page.evaluate(
            """
            () => Math.ceil(
                Math.max(
                    document.body ? document.body.scrollHeight : 0,
                    document.documentElement ? document.documentElement.scrollHeight : 0,
                    window.innerHeight || 0
                )
            )
            """
        )
        or segment_height
    )
    total_pages = max(1, min(max_pages, (full_height + segment_height - 1) // segment_height))
    rendered_pages = []
    settle_after_scroll_ms = min(wait_ms, 120)
    for index in range(total_pages):
        top = index * segment_height
        page.evaluate("(y) => window.scrollTo(0, y)", top)
        if settle_after_scroll_ms:
            page.wait_for_timeout(settle_after_scroll_ms)
        remaining = max(1, min(segment_height, full_height - top))
        image_bytes = page.screenshot(
            type="png",
            full_page=False,
            clip={
                "x": 0,
                "y": 0,
                "width": viewport_width,
                "height": remaining,
            },
        )
        rendered_pages.append(_encode_png_bytes(image_bytes))
        if top + remaining >= full_height:
            break
    return rendered_pages


def _render_html_email_pages(email_data, email_id=None):
    """Render true screenshots from the email's HTML with a headless browser."""
    started_at = time.perf_counter()
    if not _vision_render_available():
        detail = "visual_render_unavailable_playwright_missing"
        _log_action(task="vision", status="fallback", email_id=email_id, detail=detail)
        _log_performance("playwright_render", 0, email_id=email_id, rendered_pages=0, skipped=1)
        return []

    document_html = _html_document_for_visual_render(email_data)
    if not document_html:
        _log_performance("playwright_render", 0, email_id=email_id, rendered_pages=0, skipped=1)
        return []

    timeout_ms = int(_vision_browser_timeout_seconds() * 1000)
    viewport_width = _vision_render_viewport_width()
    viewport_height = _vision_render_page_height()
    wait_ms = _vision_browser_wait_ms()
    max_pages = _vision_render_max_pages()

    try:
        with VISION_BROWSER_RENDER_LOCK:
            # Visual rendering is one of the pricier paths in the app, so we reuse a
            # single browser and serialize access instead of spinning up a fresh one
            # for every email that wants a screenshot-based summary.
            browser, launch_errors = _get_or_launch_vision_browser()
            if browser is None:
                _log_action(
                    task="vision",
                    status="fallback",
                    email_id=email_id,
                    detail="visual_render_browser_launch_failed",
                )
                if launch_errors:
                    _log_action(
                        task="vision",
                        status="error",
                        email_id=email_id,
                        detail=" | ".join(launch_errors[:3]),
                    )
                _log_performance(
                    "playwright_render",
                    int((time.perf_counter() - started_at) * 1000),
                    email_id=email_id,
                    rendered_pages=0,
                    launch_failed=1,
                )
                return []

            context = None
            try:
                context = browser.new_context(
                    viewport={"width": viewport_width, "height": viewport_height}
                )
                page = context.new_page()
                page.set_content(
                    document_html,
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                if wait_ms:
                    page.wait_for_timeout(wait_ms)
                rendered_pages = _capture_html_email_pages(
                    page,
                    viewport_width=viewport_width,
                    segment_height=viewport_height,
                    max_pages=max_pages,
                    wait_ms=wait_ms,
                )
            finally:
                if context is not None:
                    context.close()
    except (PlaywrightTimeoutError, PlaywrightError, OSError) as exc:
        with VISION_BROWSER_STATE_LOCK:
            _close_vision_browser_locked()
        _log_action(
            task="vision",
            status="fallback",
            email_id=email_id,
            detail=f"visual_render_failed: {exc}",
        )
        _log_performance(
            "playwright_render",
            int((time.perf_counter() - started_at) * 1000),
            email_id=email_id,
            rendered_pages=0,
            failed=1,
        )
        return []

    if rendered_pages:
        _log_action(
            task="vision",
            status="call_success",
            email_id=email_id,
            detail=f"rendered_html_pages={len(rendered_pages)}",
        )
    _log_performance(
        "playwright_render",
        int((time.perf_counter() - started_at) * 1000),
        email_id=email_id,
        rendered_pages=len(rendered_pages),
    )
    return rendered_pages


def _vision_wrap_lines(text, width=None):
    """Wrap multiline email text into deterministic image-friendly lines."""
    wrap_width = width or _vision_render_wrap_width()
    wrapped_lines = []
    for raw_line in str(text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        normalized = raw_line.rstrip()
        if not normalized:
            if wrapped_lines and wrapped_lines[-1] == "":
                continue
            wrapped_lines.append("")
            continue
        line_indent = ""
        line_body = normalized
        if normalized.startswith(("- ", "* ")):
            line_indent = normalized[:2]
            line_body = normalized[2:].strip()
        current_width = max(20, wrap_width - len(line_indent))
        pieces = textwrap.wrap(
            line_body or normalized,
            width=current_width,
            break_long_words=True,
            break_on_hyphens=False,
            replace_whitespace=False,
        ) or [line_body or normalized]
        for index, piece in enumerate(pieces):
            prefix = line_indent if index == 0 else (" " * len(line_indent) if line_indent else "")
            wrapped_lines.append(f"{prefix}{piece}".rstrip())
    return wrapped_lines or ["(No usable email body text was available.)"]


def _vision_render_page_chunks(content_lines):
    """Split wrapped content into page-sized chunks for multimodal analysis."""
    max_lines = _vision_render_max_lines()
    chunks = []
    remaining = list(content_lines or [])
    while remaining:
        chunk = remaining[:max_lines]
        remaining = remaining[max_lines:]
        chunks.append(chunk)
    return chunks or [["(No usable email body text was available.)"]]


def _render_text_page_png(page_lines, page_number, page_count, email_data):
    """Render one page of email text into a PNG and return base64 bytes."""
    font = ImageFont.load_default()
    image = Image.new("L", (1200, 1600), color=255)
    draw = ImageDraw.Draw(image)
    sample_bbox = draw.textbbox((0, 0), "Ag", font=font)
    line_height = max(18, (sample_bbox[3] - sample_bbox[1]) + 6)
    margin_x = 48
    y = 48
    header_lines = [
        "Rendered email page for multimodal analysis",
        f"Page {page_number} of {page_count}",
        f"From: {_truncate_compact_text(email_data.get('sender') or '(unknown sender)', max_chars=180)}",
        f"Subject: {_truncate_compact_text(email_data.get('title') or '(No subject)', max_chars=180)}",
    ]
    for line in header_lines:
        draw.text((margin_x, y), str(line or ""), fill=20, font=font)
        y += line_height
    y += 8
    draw.line((margin_x, y, 1152, y), fill=160, width=1)
    y += 18
    for line in page_lines:
        draw.text((margin_x, y), str(line or ""), fill=28, font=font)
        y += line_height
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True, compress_level=9)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _render_email_image_pages(email_data, email_id=None, force=False):
    """Render true HTML screenshots for visually complex emails when available."""
    if not isinstance(email_data, dict):
        return []
    if not force and not _html_requires_visual_context(email_data):
        return []

    cache_key = _vision_render_cache_key(email_data)
    with VISION_RENDER_CACHE_LOCK:
        cached = VISION_RENDER_CACHE.get(cache_key)
        if cached:
            VISION_RENDER_CACHE.pop(cache_key, None)
            VISION_RENDER_CACHE[cache_key] = cached
            return list(cached)

    rendered_pages = _render_html_email_pages(email_data, email_id=email_id)
    if not rendered_pages:
        return []

    with VISION_RENDER_CACHE_LOCK:
        VISION_RENDER_CACHE.pop(cache_key, None)
        VISION_RENDER_CACHE[cache_key] = tuple(rendered_pages)
        while len(VISION_RENDER_CACHE) > VISION_RENDER_CACHE_MAX_ITEMS:
            oldest_key = next(iter(VISION_RENDER_CACHE))
            VISION_RENDER_CACHE.pop(oldest_key, None)
    return rendered_pages


def _vision_user_message(
    email_data,
    instruction_text,
    email_id=None,
    task=None,
    text_max_chars=None,
    allow_visual=None,
    force_visual=False,
):
    """Build the shared AI user message, attaching real screenshots only when available."""
    source_text = _source_text_for_user_message(
        email_data,
        task=task,
        text_max_chars=text_max_chars,
    )
    if not source_text:
        return None
    content = (
        f"{instruction_text}\n\n"
        f"{_vision_metadata_block(email_data)}\n\n"
        "Source email text:\n"
        "---BEGIN EMAIL TEXT---\n"
        f"{source_text}\n"
        "---END EMAIL TEXT---\n\n"
        "Use the cleaned email text for grounded wording and direct facts from the message body. "
        "Use the sender and subject metadata only as supporting context."
    )
    user_message = {
        "role": "user",
        "content": content,
    }
    if allow_visual is False:
        return user_message
    if allow_visual is None and not _visual_context_allowed(task=task):
        return user_message
    should_attach_visual = bool(allow_visual)
    if allow_visual is None:
        should_attach_visual = _html_requires_visual_context(email_data)
    if not should_attach_visual:
        return user_message

    rendered_pages = _render_email_image_pages(
        email_data,
        email_id=email_id,
        force=bool(force_visual),
    )
    if not rendered_pages:
        return user_message
    user_message["content"] += (
        "\n\nRendered screenshots of the original HTML email are attached. "
        "Use those screenshots as the source of truth for layout-dependent details such as banners, "
        "buttons, cards, offer hierarchy, and image-only text. "
        "Use the cleaned email text to cross-check readable wording when the layout is not important."
    )
    user_message["images"] = rendered_pages
    return user_message


def _email_body_text(email_data):
    """Return the best available plain-text body for AI analysis."""
    if not isinstance(email_data, dict):
        return repair_body_text(email_data or "", None)
    return repair_body_text(
        email_data.get("body") or "",
        email_data.get("body_html"),
    )


def _is_loopback_host(hostname):
    """Return whether loopback host.
    """
    # Shared helper for this file.
    if not hostname:
        return False
    lowered = hostname.lower()
    if lowered in LOCALHOST_NAMES:
        return True
    try:
        return ipaddress.ip_address(lowered).is_loopback
    except ValueError:
        return False


def _endpoint_allowed():
    """Endpoint allowed.
    """
    # Shared helper for this file.
    parsed = urlparse(_api_url())
    if parsed.scheme != "http":
        return False
    return _is_loopback_host(parsed.hostname)


def ai_enabled():
    """Ai enabled.
    """
    # Shared helper for this file.
    return _endpoint_allowed()


def should_summarize_email(email_data):
    """Return whether summarize email.
    """
    # Decide whether summary generation should run for this message and context.
    body = _email_body_text(email_data).strip()
    return len(body) >= _summary_min_chars()


def classification_to_email_type(classification):
    """Classification recipient email type.
    """
    # Keep labels aligned with the mailbox triage buckets.
    if not isinstance(classification, dict):
        return "read-only"
    explicit_type = str(classification.get("email_type") or "").strip().lower()
    if explicit_type in VALID_EMAIL_TYPES:
        return explicit_type
    category = str(classification.get("category") or "").strip().lower()
    needs_response = bool(classification.get("needs_response"))
    if category == "junk":
        try:
            confidence = float(classification.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
            return "junk-uncertain"
        return "junk"
    if needs_response:
        return "response-needed"
    return "read-only"


def _compact_text(value):
    """Compact text.
    """
    # Shared helper for this file.
    return " ".join(str(value or "").split()).strip()


def _truncate_compact_text(value, max_chars=160):
    """Return compact text clipped to a safe prompt-friendly length."""
    text = _compact_text(value)
    if not text or len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _normalized_header_text(value):
    """Repair and compact short header-style text."""
    return _compact_text(repair_header_text(value or ""))


def _ensure_sentence_ending(text):
    """Return compacted text with terminal punctuation."""
    # Keep generated copy readable by avoiding repeated punctuation patterns.
    value = _compact_text(text)
    if not value:
        return ""
    if value[-1] in ".!?":
        return value
    return f"{value}."


def _strip_read_time_prefix(text):
    """Remove leading article read-time labels from short teaser blurbs."""
    cleaned = _compact_text(text)
    if not cleaned:
        return ""
    previous = None
    while cleaned and cleaned != previous:
        previous = cleaned
        cleaned = READ_TIME_PREFIX_PATTERN.sub("", cleaned).strip(" -:")
    return cleaned


def _looks_read_time_label(text):
    """Return True when text is only an article read-time tag."""
    normalized = _compact_text(text).lower()
    if not normalized:
        return False
    return bool(
        re.fullmatch(
            r"\d+\s*(?:-\s*\d+)?(?:\s*-\s*|\s+)(?:min(?:ute)?s?)\s+read",
            normalized,
        )
    )


def _looks_direct_request_question(text):
    """Return True when a question is asking the mailbox owner for a response."""
    normalized = _compact_text(text)
    if not normalized.endswith("?"):
        return False
    lowered = normalized.lower()
    if _has_any_pattern(lowered, REQUEST_SENTENCE_MARKERS):
        return True
    direct_prefixes = (
        "can you ",
        "could you ",
        "would you ",
        "will you ",
        "are you ",
        "are you able to ",
        "have you ",
        "did you ",
        "do you have ",
        "do you want ",
        "should we ",
        "can we ",
        "could we ",
    )
    return lowered.startswith(direct_prefixes)


def _looks_newsletter_teaser_question(text, email_data=None):
    """Return True for rhetorical/news teaser questions that should not survive into summaries."""
    normalized = _strip_read_time_prefix(text)
    if not normalized.endswith("?"):
        return False
    newsletter_like = email_data is not None and _looks_bulk_or_newsletter(email_data)
    if email_data is not None and not newsletter_like:
        return False
    if _looks_direct_request_question(normalized):
        return False
    lowered = normalized.lower()
    teaser_prefixes = (
        "do you think ",
        "what's ",
        "what is ",
        "why ",
        "how ",
        "which ",
        "who ",
    )
    if newsletter_like:
        teaser_prefixes = teaser_prefixes + (
            "when ",
            "where ",
            "is ",
            "are ",
            "can ",
            "could ",
            "would ",
            "will ",
        )
    if lowered.startswith(teaser_prefixes):
        return True
    if not newsletter_like:
        return False
    return len(_content_tokens(lowered)) <= 18 and not _has_any_pattern(
        lowered, REQUEST_SENTENCE_MARKERS
    )


def _sentence_needs_merge(left, right):
    """Return True when a sentence split likely happened inside an abbreviation."""
    previous = _compact_text(left)
    current = _compact_text(right)
    if not previous or not current:
        return False
    lowered = previous.lower()
    if lowered.endswith(SENTENCE_ABBREVIATION_ENDINGS):
        return True
    if re.search(r"(?:\b[A-Z]\.|\b(?:[A-Za-z]\.){2,})$", previous):
        return True
    return bool(re.search(r"\b[A-Z][a-z]?\.$", previous) and re.match(r"^[A-Za-z]", current))


def _merge_sentence_fragments(parts):
    """Merge sentence fragments split after abbreviations like E. coli or U.S."""
    merged = []
    for part in parts:
        sentence = _compact_text(part)
        if not sentence:
            continue
        if merged and _sentence_needs_merge(merged[-1], sentence):
            merged[-1] = _compact_text(f"{merged[-1]} {sentence}")
            continue
        merged.append(sentence)
    return merged


def _strip_footer_noise_text(text):
    """Remove common newsletter/legal footer phrases from text."""
    # Keep prompts and summaries focused on actionable content instead of boilerplate.
    cleaned = str(text or "")
    if not cleaned:
        return ""
    for pattern in FOOTER_NOISE_REGEX_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"(?:\(\s*\[link\]\s*\)\s*){2,}",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"(?:\(?\s*\[link\]\s*\)?\s*[|,;/:-]?\s*){2,}",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\[\s*link\s*\](?:\s*[|,;])?\s*\[\s*link\s*\]",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\[\s*link\s*\]", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*[|]+\s*", " ", cleaned)
    cleaned = re.sub(r"\.{2,}", ".", cleaned)
    cleaned = re.sub(r"\?{2,}", "?", cleaned)
    cleaned = re.sub(r"!{2,}", "!", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = cleaned.strip(" -|,;:")
    if _looks_source_signature_sentence(cleaned):
        return ""
    return cleaned


def _looks_footer_noise_fragment(text):
    """Return True when text appears to be newsletter/footer boilerplate."""
    # Keep this rule here so the behavior stays consistent.
    normalized = _compact_text(text).lower()
    if not normalized:
        return False
    for pattern in FOOTER_NOISE_REGEX_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            return True
    if normalized.count("[link]") >= 2 and len(_text_tokens(normalized)) <= 14:
        return True
    return False


def _looks_utility_sentence(text):
    """Return True when text is mostly footer, support, or utility boilerplate."""
    normalized = _compact_text(text).lower()
    if not normalized:
        return True
    hits = sum(marker in normalized for marker in SUMMARY_UTILITY_MARKERS)
    if hits >= 2:
        return True
    if hits >= 1 and len(_content_tokens(normalized)) <= 14:
        return True
    if EMAIL_ADDRESS_PATTERN.search(normalized) and any(
        marker in normalized for marker in ("support", "customer service", "contact us")
    ):
        return True
    if re.search(r"\b1[-\s]?800[-\s]?[a-z0-9-]+\b", normalized):
        return True
    if re.search(r"\b(?:route|street|st\.|avenue|ave\.|road|rd\.|junction)\b", normalized) and re.search(
        r"\b[a-z]{2}\s+\d{5}(?:-\d{4})?\b",
        normalized,
    ):
        return True
    return False


def _looks_source_signature_sentence(text):
    """Return True when text looks like a publisher/source signature line."""
    # Keep summaries focused by dropping short masthead lines (e.g., "<Publisher> Online").
    normalized = _compact_text(text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if "[link]" in lowered:
        return False
    if any(marker in lowered for marker in REQUEST_SENTENCE_MARKERS):
        return False

    tokens = _text_tokens(lowered)
    if len(tokens) < 3 or len(tokens) > 10:
        return False
    # Treat bare publication mastheads as non-content when there are no action verbs nearby.
    common_verbs = {
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "can",
        "could",
        "will",
        "would",
        "should",
        "must",
        "need",
        "needs",
        "join",
        "reply",
        "confirm",
        "review",
        "update",
    }
    has_likely_verb = any(
        token in common_verbs or token.endswith("ed") or token.endswith("ing")
        for token in tokens
    )
    if has_likely_verb:
        return False
    return tokens[-1] in {"online", "newsletter", "digest"}


def _text_tokens(value):
    """Text tokens.
    """
    # Shared helper for this file.
    text = str(value or "").lower()
    return [token for token in re.findall(r"[a-z0-9']+", text) if len(token) > 2]


def _token_overlap_ratio(left, right):
    """Token overlap ratio.
    """
    # Shared helper for this file.
    left_tokens = set(_text_tokens(left))
    right_tokens = set(_text_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / float(min(len(left_tokens), len(right_tokens)))


def _is_noise_fragment(text):
    """Return whether noise fragment.
    """
    # Shared helper for this file.
    normalized = _compact_text(text).lower()
    if not normalized or normalized == "[link]":
        return True
    if _looks_read_time_label(normalized):
        return True
    if _looks_footer_noise_fragment(normalized):
        return True
    if _looks_source_signature_sentence(normalized):
        return True
    return any(marker in normalized for marker in SUMMARY_NOISE_MARKERS)


def _is_near_subject_copy(candidate_text, title_text):
    """Return whether near subject copy.
    """
    # Shared helper for this file.
    candidate = _compact_text(candidate_text)
    title = _compact_text(title_text)
    if not candidate or not title or title.lower() == "(no subject)":
        return False
    lowered_candidate = candidate.lower()
    lowered_title = title.lower()
    if lowered_candidate == lowered_title:
        return True
    overlap = _token_overlap_ratio(lowered_candidate, lowered_title)
    if overlap >= 0.92:
        return True
    if lowered_candidate.startswith(lowered_title):
        remainder = lowered_candidate[len(lowered_title):].strip(" .:-")
        if len(remainder) < 36:
            return True
    return False


def _summary_uses_subject_content(summary_text, email_data):
    """Return True when a summary relies on subject-only wording instead of body content."""
    summary = _compact_text(summary_text)
    title = _normalized_header_text(email_data.get("title") or "")
    if not summary or not title or title.lower() == "(no subject)":
        return False
    if (
        summary.lower().startswith("a promotional update from ")
        and (
            len(_clean_body_for_prompt(email_data, max_chars=240)) < 120
            or _extract_offer_phrases(
                " ".join(
                    [
                        title,
                        _clean_body_for_prompt(email_data, max_chars=1600),
                    ]
                ),
                max_items=1,
            )
            or _extract_bullet_item_names(_email_body_text(email_data), max_items=1)
        )
    ):
        return False

    body_context = _body_for_context(email_data, max_chars=2400, max_sentences=12)
    title_tokens = set(_content_tokens(title))
    body_tokens = set(_content_tokens(body_context))
    subject_only_tokens = title_tokens - body_tokens
    if not subject_only_tokens:
        return False

    summary_tokens = set(_content_tokens(summary))
    subject_hits = summary_tokens & subject_only_tokens
    if not subject_hits:
        return False

    first_sentence = _compact_text(
        re.split(r"(?<=[.!?])\s+", summary, maxsplit=1)[0]
    )
    lowered_first = first_sentence.lower()
    if _is_near_subject_copy(first_sentence, title):
        return True
    if re.search(
        r"\b(?:this email|the email|newsletter|email|message|article alert|news digest|"
        r"question digest|reminder)\b",
        lowered_first,
    ) and "about" in lowered_first:
        return True
    return len(subject_hits) >= max(2, min(4, len(subject_only_tokens)))


def _strip_title_prefix(candidate_text, title_text):
    """Remove a repeated title prefix from a sentence when the remainder is informative."""
    candidate = _compact_text(candidate_text)
    title = _compact_text(title_text)
    if not candidate or not title or title.lower() == "(no subject)":
        return candidate

    title_variants = [title]
    if ":" in title:
        trailing = _compact_text(title.split(":", 1)[1])
        if trailing:
            title_variants.append(trailing)
        flattened = _compact_text(title.replace(":", " "))
        if flattened:
            title_variants.append(flattened)

    for variant in title_variants:
        lowered_variant = variant.lower().rstrip(" .:-")
        lowered_candidate = candidate.lower()
        if not lowered_candidate.startswith(lowered_variant):
            continue
        remainder = candidate[len(variant) :].strip(" .:-")
        if len(remainder) >= 24:
            if re.match(
                r"^(?:highlights|covers|features|includes|announces|explores)\b",
                remainder,
                flags=re.IGNORECASE,
            ):
                continue
            return remainder
    return candidate


def _strip_reply_chain(text):
    """Strip reply chain.
    """
    content = str(text or "")
    cut_positions = []
    # Trim quoted history so prompts stay focused on the newest message only.
    for pattern in REPLY_CHAIN_PATTERNS:
        match = re.search(
            pattern,
            content,
            flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
        )
        if match:
            cut_positions.append(match.start())
    if not cut_positions:
        return content
    return content[: min(cut_positions)]


def _looks_link_heavy_line(line_text):
    """Return True when a line is mostly a URL or navigation chrome."""
    normalized = _compact_text(line_text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if lowered in {"[link]", "link", "click here", "view in browser"}:
        return True
    if re.fullmatch(r"(?:\[link\]\s*){1,4}", normalized, flags=re.IGNORECASE):
        return True
    alpha_count = sum(character.isalpha() for character in normalized)
    if lowered.startswith(("http://", "https://")):
        return True
    if "[link]" in normalized and alpha_count < 18:
        return True
    if re.search(r"https?://\S+", str(line_text or ""), flags=re.IGNORECASE) and alpha_count < 24:
        return True
    return False


def _looks_digest_scaffold_line(line_text):
    """Return True for section labels, credits, or navigation lines that should not drive summaries."""
    normalized = _compact_text(line_text)
    if not normalized:
        return False
    lowered = normalized.lower().strip(" .:;|!-")
    scaffold_markers = {
        "sponsored by",
        "content from",
        "today's news",
        "top stories",
        "more news",
        "the number",
        "spotlight",
        "catch up",
        "take a break",
        "don't miss this",
        "this week's must-listen",
        "scientists at work",
        "cbc - this week",
        "latest and greatest tech",
        "top deals",
        "shop deals by category",
        "store hours",
        "help centre",
        "terms",
        "privacy",
        "email preferences",
        "newsletters & alerts",
        "contact us",
    }
    if lowered in scaffold_markers:
        return True
    if lowered.startswith(("no images? click here", "sponsored by", "content from:")):
        return True
    if re.match(r"^(?:by|from)\s+[a-z][a-z .'-]{1,50}$", lowered):
        return True
    if re.match(
        r"^(?:credit:|photo:)?\s*[a-z0-9 .'/&-]{1,80}/(?:reuters|getty|ap|afp|shutterstock|bloomberg|alamy)\b",
        lowered,
    ):
        return True
    if re.match(
        r"^[a-z .'-]{2,50},\s*(?:contributing editor|editor|staff writer|reporter)$",
        lowered,
    ):
        return True
    if re.match(
        r"^(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b.*\d{1,2}:\d{2}",
        lowered,
    ):
        return True
    if re.match(
        r"^(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|"
        r"dec(?:ember)?)\s+\d{1,2},\s+\d{4}$",
        lowered,
    ):
        return True
    token_count = len(_text_tokens(normalized))
    alpha_count = sum(character.isalpha() for character in normalized)
    compact_upper = re.sub(r"[^A-Za-z]", "", normalized)
    if compact_upper and compact_upper.isupper() and token_count <= 4 and alpha_count <= 32:
        return True
    return False


def _looks_markup_noise_line(line_text):
    """Return True when a line is mostly CSS/HTML chrome from a bad plain-text extraction."""
    normalized = _compact_text(line_text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if normalized.startswith(("<", "</")) and normalized.endswith(">"):
        return True
    patterns = (
        r"^@import\b",
        r"^url\(",
        r"@font-face\b",
        r"\bexternalclass\b",
        r"\bfont-family\s*:",
        r"\bfont-display\s*:",
        r"\bfont-style\b",
        r"\bfont-weight\b",
        r"\btext-size-adjust\s*:",
        r"\bdisplay\s*:\s*block\b",
        r"!important",
        r"\bline-height\s*:\s*\d+%?",
        r"\burl\(['\"]?https?://\S+\.(?:woff2?|woff|ttf|svg|eot)",
        r"\bformat\(['\"](?:woff2?|woff|truetype|svg)['\"]\)",
        r"^\}?(?:#|\.)(?:[a-z0-9_-]+)",
        r"^(?:body|html|img|table|td|div|span|p|a)\s*\{",
    )
    if any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in patterns):
        return True
    if re.fullmatch(r"(?:src|style|width|height|margin|padding|\d{1,4})", lowered):
        return True
    if normalized.count("{") + normalized.count("}") >= 2 and len(_text_tokens(normalized)) <= 18:
        return True
    return False


def _looks_newsletter_intro_line(line_text):
    """Return True for breezy newsletter intros that should not dominate summaries."""
    lowered = _compact_text(line_text).lower().strip(" -:;,.!?")
    if not lowered:
        return True
    starters = (
        "good morning",
        "good afternoon",
        "good evening",
        "happy monday",
        "happy tuesday",
        "happy wednesday",
        "happy thursday",
        "happy friday",
        "happy saturday",
        "happy sunday",
        "presented by",
        "together with",
    )
    return lowered.startswith(starters)


def _looks_credit_or_byline_line(line_text):
    """Return True for author-credit or quote-credit lines that are not summary-worthy."""
    normalized = _compact_text(line_text).strip()
    if not normalized:
        return True
    lowered = normalized.lower()
    if lowered.startswith(("quote:", "-")) and len(_text_tokens(normalized)) <= 24:
        return True
    if re.fullmatch(
        r"-?[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,6}(?:,\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,6})+",
        normalized,
    ):
        return True
    return False


def _looks_numeric_scoreboard_line(line_text):
    """Return True for ticker/scoreboard lines dominated by numeric market data."""
    normalized = _compact_text(line_text)
    if not normalized:
        return False
    digit_count = sum(character.isdigit() for character in normalized)
    alpha_count = sum(character.isalpha() for character in normalized)
    symbol_hits = sum(normalized.count(symbol) for symbol in ("%", "$", ",", "+"))
    if digit_count >= 4 and digit_count >= alpha_count and symbol_hits >= 1:
        return True
    return bool(
        re.fullmatch(r"(?:[A-Za-z][A-Za-z .'-]{0,20}\s+)?[+\-$0-9.,% ]{6,}", normalized)
    )


def _clean_body_for_prompt(body, max_chars=8000):
    """Clean body for prompt.
    """
    # Clean this up so the rest of the code sees something predictable.
    title_text = ""
    if isinstance(body, dict):
        text = _email_body_text(body)
        title_text = _normalized_header_text(body.get("title") or "")
    else:
        text = repair_body_text(body or "", None)
    text = re.sub(r"(\d{4})View Online\b", r"\1\nView Online", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*https?://[^>\n]+>", " [link] ", text, flags=re.IGNORECASE)
    text = re.sub(
        r"<[^>\n]*(?:href=|src=|alt=|border=|https?://|/\s*a\b|img\b)[^>\n]*>",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _strip_reply_chain(text)
    cleaned_lines = []
    seen_lines = set()
    for line in text.split("\n"):
        trimmed = line.strip()
        if not trimmed:
            # Keep paragraph boundaries intact for later sentence splitting.
            cleaned_lines.append("")
            continue
        if trimmed.startswith(">"):
            # Ignore quoted history from earlier messages in the thread.
            continue
        if _looks_markup_noise_line(trimmed):
            continue
        normalized_line = re.sub(r"https?://\S+", "[link]", trimmed)
        normalized_line = re.sub(r"\[\s*https?://[^\]]+\s*\]", "[link]", normalized_line)
        normalized_line = re.sub(r"\(\s*\[link\]\s*\)", "", normalized_line)
        normalized_line = re.sub(r"(?:\s*\[link\]){2,}", " [link]", normalized_line)
        normalized_line = re.sub(r"^read more:?\s*", "", normalized_line, flags=re.IGNORECASE)
        normalized_line = re.sub(r"^question:\s*", "", normalized_line, flags=re.IGNORECASE)
        normalized_line = re.sub(r"^answer from\s+[^:]+:?[\s-]*", "", normalized_line, flags=re.IGNORECASE)
        normalized_line = _strip_read_time_prefix(normalized_line)
        normalized_line = re.sub(
            r"^(?:credit:?\s*)?[a-z0-9 .'/&-]{1,60}/(?:reuters|getty images|ap|afp|shutterstock|bloomberg|alamy)\s+",
            "",
            normalized_line,
            flags=re.IGNORECASE,
        )
        normalized_line = re.sub(
            r"^(?:contributor/getty images|mohammed aty/reuters|robin lloyd,\s+contributing editor)\s+",
            "",
            normalized_line,
            flags=re.IGNORECASE,
        )
        normalized_line = re.sub(
            r"\b(?:star border|checkerboard(?:\s+artist)?(?:\s+image)?\s*\d*|"
            r"artist image\s*\d+|image\s+\d+)\b",
            " ",
            normalized_line,
            flags=re.IGNORECASE,
        )
        normalized_line = _strip_footer_noise_text(normalized_line)
        normalized_line = _compact_text(normalized_line)
        if not normalized_line:
            # Drop lines that turn empty after noise stripping.
            continue
        lowered = normalized_line.lower()
        if _looks_credit_or_byline_line(normalized_line):
            continue
        if _looks_numeric_scoreboard_line(normalized_line):
            continue
        if (
            title_text
            and _is_near_subject_copy(normalized_line, title_text)
            and len(_text_tokens(normalized_line)) <= 16
            and not re.search(r"[.!?]", normalized_line)
        ):
            continue
        if _looks_link_heavy_line(normalized_line):
            continue
        if re.match(
            r"^(?:read more|get the details|get tickets|learn more|listen now|watch now|"
            r"try premium button|follow us|preferences|feedback|alerts center|"
            r"contact us|other\s*\(|top stories for|answer from|view in browser|view this email|open in browser|view online|"
            r"or,\s*see\b|click here\b|click to\b|save now\b|see deals\b|order now\b|"
            r"shop now\b|top deals\b|view web version\b|unsubscribe\b)\b",
            lowered,
        ):
            continue
        if re.match(
            r"^(?:this message was sent to|you are currently subscribed as|copyright \d{4}|"
            r"dow jones & company|cbc canadian broadcasting corporation)\b",
            lowered,
        ):
            continue
        if "because you created an account" in lowered:
            continue
        if lowered.startswith("platforms:"):
            continue
        if lowered.startswith(("support our mission", "sign up for wsj newsletters", "today's newsletter was curated by")):
            continue
        if lowered in {
            "entertaining and insightful stories delivered weekly",
            "read, watch and listen to highlights from across the cbc",
            "this is an edition of the what's news newsletter, which helps you catch up on the headlines and understand the news, free in your inbox daily.",
        }:
            continue
        if re.match(r"^(?:a|an)\s+[a-z0-9][^.!?]{0,40}$", lowered) and len(_text_tokens(lowered)) <= 4:
            continue
        if re.match(r"^[a-z .'-]+\s+on\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", lowered):
            continue
        if _looks_digest_scaffold_line(normalized_line):
            continue
        if lowered in {"[link]", "joji", "cbc gem: say goodbye to ads."}:
            continue
        if normalized_line in seen_lines:
            continue
        seen_lines.add(normalized_line)
        cleaned_lines.append(normalized_line)

    cleaned = "\n".join(cleaned_lines)
    cleaned = _strip_footer_noise_text(cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.strip()
    return cleaned[:max_chars]


def _extract_key_sentences(body_text, max_sentences=8):
    """Extract key sentences.
    """
    # Some payloads leave this field out, so read it carefully.
    email_context = body_text if isinstance(body_text, dict) else None
    cleaned_body = _clean_body_for_prompt(body_text or "", max_chars=5000)
    if not cleaned_body:
        return []

    flattened = _compact_text(cleaned_body)
    if not flattened:
        return []
    flattened = re.sub(r"\s*[|*]\s*", ". ", flattened)
    flattened = re.sub(r"\s*[-]{2,}\s*", ". ", flattened)

    parts = _merge_sentence_fragments(re.split(r"(?<=[.!?])\s+", flattened))
    if len(parts) <= 1:
        # Fallback splitter for emails that do not have much punctuation structure.
        parts = _merge_sentence_fragments(re.split(r";\s+|\.\s+", flattened))

    selected = []
    for part in parts:
        sentence = _strip_read_time_prefix(part)
        sentence = _compact_text(sentence).strip(" -:")
        sentence = _strip_footer_noise_text(sentence)
        if len(sentence) < 24:
            continue
        for marker in SUMMARY_NOISE_MARKERS:
            sentence = re.sub(re.escape(marker), " ", sentence, flags=re.IGNORECASE)
        sentence = _compact_text(sentence).strip(" -:")
        if len(sentence) < 24:
            continue
        if _looks_newsletter_teaser_question(sentence, email_data=email_context):
            continue
        if _looks_utility_sentence(sentence):
            continue
        lowered = sentence.lower()
        if lowered.startswith(("from:", "to:", "cc:", "bcc:", "sent:", "date:", "subject:")):
            # Skip copied header metadata that is not really body content.
            continue
        if _looks_source_signature_sentence(sentence):
            continue
        if _is_noise_fragment(sentence):
            continue
        if any(_token_overlap_ratio(sentence, existing) > 0.94 for existing in selected):
            # Drop near-duplicate lines from the extracted candidates.
            continue
        selected.append(sentence)
        if len(selected) >= max_sentences:
            break
    return selected


def _summary_profile(email_data):
    """Return summary length settings based on email body size."""
    body = _clean_body_for_prompt(email_data, max_chars=20000)
    body_length = len(body)
    if body_length >= 4000:
        profile = {
            "char_limit": 2200,
            "output_sentences": 8,
            "context_sentences": 32,
            "context_chars": 3200,
            "prompt_target": "up to 8 sentences when the email needs it",
            "num_predict": 320,
        }
    elif body_length >= 1800:
        profile = {
            "char_limit": 1700,
            "output_sentences": 6,
            "context_sentences": 26,
            "context_chars": 2800,
            "prompt_target": "up to 6 sentences when the email needs it",
            "num_predict": 240,
        }
    elif body_length >= 900:
        profile = {
            "char_limit": 1200,
            "output_sentences": 5,
            "context_sentences": 18,
            "context_chars": 2200,
            "prompt_target": "up to 5 sentences when the email needs it",
            "num_predict": 180,
        }
    else:
        profile = {
            "char_limit": 750,
            "output_sentences": 3,
            "context_sentences": 12,
            "context_chars": 1600,
            "prompt_target": "up to 3 sentences when the email needs it",
            "num_predict": 120,
        }
    return profile


def _body_for_context(email_data, max_chars=8000, max_sentences=14):
    """Body for context.
    """
    # Build body for context text that is passed into model prompts.
    effective_max_chars = max(240, min(int(max_chars or 8000), 3200))
    effective_max_sentences = max(1, min(int(max_sentences or 14), 8))
    key_sentences = _extract_key_sentences(
        email_data,
        max_sentences=effective_max_sentences,
    )
    if key_sentences:
        title = _compact_text(email_data.get("title") or "")
        text = " ".join(
            _compact_text(_strip_title_prefix(sentence, title)) or _compact_text(sentence)
            for sentence in key_sentences
        )
    else:
        text = _clean_body_for_prompt(email_data, max_chars=effective_max_chars)
        for marker in SUMMARY_NOISE_MARKERS:
            text = re.sub(re.escape(marker), " ", text, flags=re.IGNORECASE)
    text = _strip_footer_noise_text(text)
    text = _compact_text(text)
    if len(text) > effective_max_chars:
        return f"{text[: effective_max_chars - 3].rstrip()}..."
    return text


def _natural_join(items):
    """Join short phrases into natural English."""
    values = [_compact_text(item) for item in items if _compact_text(item)]
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} and {values[1]}"
    return f"{', '.join(values[:-1])}, and {values[-1]}"


def _first_regex_match(text, pattern):
    """Return first regex match as compact text."""
    match = re.search(pattern, str(text or ""), flags=re.IGNORECASE)
    if not match:
        return ""
    return _compact_text(match.group(0))


def _summary_sender_name(email_data):
    """Return sender display text suitable for summaries."""
    sender_raw = _normalized_header_text(email_data.get("sender") or "")
    if not sender_raw:
        return "The sender"
    display = sender_raw.split("<", 1)[0].strip().strip('"')
    if "@" in display and " " not in display:
        display = display.split("@", 1)[0]
    display = display.replace(".", " ").replace("_", " ").replace("-", " ")
    display = " ".join(display.split())
    return display or "The sender"


def _summary_title_topic(title_text):
    """Return the most useful topic phrase from an email subject line."""
    title = _normalized_header_text(title_text)
    if not title or title.lower() == "(no subject)":
        return ""
    if ":" in title:
        trailing = _compact_text(title.split(":", 1)[1])
        if len(trailing) >= 12:
            return trailing
    return title


def _extract_labeled_prompt_text(raw_text):
    """Return the body text that follows a `Prompt:` label when present."""
    lines = str(raw_text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    for index, raw_line in enumerate(lines):
        line = _compact_text(raw_line)
        if not line:
            continue
        inline_match = re.match(r"^prompt:\s*(.+)$", line, flags=re.IGNORECASE)
        if inline_match:
            return _compact_text(inline_match.group(1)).strip(" -:,.!?")
        if line.lower().rstrip(":") != "prompt":
            continue
        for candidate in lines[index + 1 :]:
            candidate_text = _compact_text(candidate)
            if not candidate_text:
                continue
            if re.match(
                r"^(?:log in|sign in|click|or,\s*see\b|email\b|answer\b|from\b|you are receiving\b)",
                candidate_text,
                flags=re.IGNORECASE,
            ):
                continue
            return candidate_text.strip(" -:,.!?")
    return ""


def _prompt_summary_sentence(prompt_text):
    """Rewrite prompt text into a descriptive summary sentence."""
    prompt = _compact_text(prompt_text).strip(" -:,.!?")
    if not prompt:
        return ""
    lowered_prompt = prompt[0].lower() + prompt[1:] if len(prompt) > 1 else prompt.lower()
    if re.match(
        r"^(?:describe|share|write|tell|record|reflect|remember|explain|list|talk about|consider|recount)\b",
        prompt,
        flags=re.IGNORECASE,
    ):
        return f"The email includes a prompt to {lowered_prompt}"
    return f"It includes a prompt about {prompt}"


def _prompt_reminder_summary(email_data):
    """Return a compact summary for reminder-style prompt emails."""
    if not _looks_bulk_or_newsletter(email_data):
        return None
    raw_body = _email_body_text(email_data)
    prompt_text = _extract_labeled_prompt_text(raw_body)
    if not prompt_text:
        return None
    title = _normalized_header_text(email_data.get("title") or "")
    combined = _compact_text(" ".join([title, raw_body])).lower()
    if not (
        re.search(r"\breminder\b", title, flags=re.IGNORECASE)
        or "continue writing" in combined
        or "writing reminder" in combined
        or "life story" in combined
    ):
        return None

    sender_name = _summary_sender_name(email_data)
    sentences = [_ensure_sentence_ending(f"{sender_name} sent a reminder")]
    prompt_sentence = _prompt_summary_sentence(prompt_text)
    if prompt_sentence:
        sentences.append(_ensure_sentence_ending(prompt_sentence))
    if re.search(r"\b(?:answer|complete)\s+a\s+(?:\d+|one)[-\s]?question survey\b", raw_body, flags=re.IGNORECASE):
        sentences.append(_ensure_sentence_ending("It also mentions a 1-question survey"))
    if re.search(r"\b(?:log in|sign in)\b", raw_body, flags=re.IGNORECASE):
        sentences.append(_ensure_sentence_ending("It includes a login link to save the response"))
    summary = _compact_text(" ".join(sentences))
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _activity_notification_summary(email_data):
    """Return a compact summary for app/activity notification emails."""
    if not _looks_bulk_or_newsletter(email_data):
        return None
    title = _normalized_header_text(email_data.get("title") or "")
    body = _clean_body_for_prompt(email_data, max_chars=1800)
    combined = _compact_text(" ".join([title, body])).lower()
    unread_match = re.search(
        r"\b(?:(\d+)\s+unread messages?|unread messages?\s+from\s+(\d+)\s+person)\b",
        combined,
        flags=re.IGNORECASE,
    )
    activity_markers = (
        "likes",
        "comments",
        "follows",
        "mentions",
        "replies",
        "new activity",
    )
    explicit_notification_markers = (
        "unread message",
        "unread messages",
        "liked your",
        "commented on",
        "followed you",
        "mentioned you",
        "new activity",
        "new like",
        "new comment",
        "new reply",
    )
    if not unread_match and not any(marker in combined for marker in explicit_notification_markers):
        return None

    unread_count = next((value for value in unread_match.groups() if value), "") if unread_match else ""
    sender_name = _summary_sender_name(email_data)
    sentences = [_ensure_sentence_ending(f"An activity update from {sender_name}")]
    if unread_count:
        noun = "message" if unread_count == "1" else "messages"
        sentences.append(_ensure_sentence_ending(f"It says there {'is' if unread_count == '1' else 'are'} {unread_count} unread {noun}"))
    if any(marker in combined for marker in ("likes", "comments", "follows", "mentions", "replies")):
        sentences.append(_ensure_sentence_ending("It also mentions recent likes, comments, or follows"))
    summary = _compact_text(" ".join(sentences))
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _job_alert_summary(email_data):
    """Return a compact summary for generic job-alert emails."""
    if not _looks_bulk_or_newsletter(email_data):
        return None
    title = _normalized_header_text(email_data.get("title") or "")
    raw_body = _email_body_text(email_data)
    combined = _compact_text(" ".join([title, raw_body]))
    if not re.search(r"\bjob alert\b|\b\d+\s+new jobs?\b", combined, flags=re.IGNORECASE):
        return None

    sender_name = _summary_sender_name(email_data)
    focus_match = re.search(
        r"\bjob alert\s+for\s+(.+?)\s+in\s+([A-Z][A-Za-z .'-]+(?:,\s*[A-Z][A-Za-z .'-]+)+)",
        raw_body,
        flags=re.IGNORECASE,
    )
    title_match = re.search(r"\b(\d+)\s+new jobs?\s+for\s+'?([^'\n]+)'?", title, flags=re.IGNORECASE)
    raw_lines = [
        _compact_text(line).strip(" -:;,.")
        for line in str(raw_body or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
        if _compact_text(line).strip(" -:;,.")
    ]
    first_role = ""
    for index, line in enumerate(raw_lines):
        lowered = line.lower()
        if "job matches your preferences" in lowered or "job match your preferences" in lowered:
            for candidate in raw_lines[index + 1 : index + 4]:
                candidate_lower = candidate.lower()
                if _looks_utility_sentence(candidate) or _is_noise_fragment(candidate):
                    continue
                if any(marker in candidate_lower for marker in ("view job", "see jobs", "top applicant", "try premium")):
                    continue
                if len(candidate) >= 8:
                    first_role = candidate
                    break
            break

    sentences = [_ensure_sentence_ending(f"A job alert from {sender_name}")]
    if title_match:
        sentences.append(
            _ensure_sentence_ending(f"It mentions {title_match.group(1)} new job for {title_match.group(2)}")
        )
    elif focus_match:
        sentences.append(
            _ensure_sentence_ending(f"It covers jobs for {focus_match.group(1)} in {focus_match.group(2)}")
        )
    if first_role:
        sentences.append(_ensure_sentence_ending(f"It includes {first_role}"))
    summary = _compact_text(" ".join(sentences))
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _looks_marketing_alert_teaser(teaser_text):
    """Return True for promo-style alert copy that should never be reused verbatim."""
    normalized = _compact_text(repair_body_text(teaser_text or "", None)).lower()
    if not normalized:
        return False
    if re.search(r"\byou'?re one of\b|\byou are one of\b", normalized):
        return True
    markers = (
        "get the details",
        "top fan",
        "top fans",
        "top listener",
        "top listeners",
        "just had to let you know",
        "headed your way",
        "dropped tour dates",
        "artist image",
        "checkerboard",
        "star border",
    )
    return any(marker in normalized for marker in markers)


def _paraphrase_marketing_alert_teaser(teaser_text):
    """Convert clicky entertainment-alert copy into a short neutral summary sentence."""
    teaser = _compact_text(teaser_text)
    if not teaser or not _looks_marketing_alert_teaser(teaser):
        return ""

    cleaned = re.sub(
        r"\b(?:star border|checkerboard(?:\s+artist)?(?:\s+image)?\s*\d*|"
        r"artist image\s*\d+|image\s+\d+)\b",
        " ",
        teaser,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(?:get the details|get tickets|learn more|listen now|watch now)\b[\s:!,-]*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    artist_match = re.search(
        r"\bone of\s+([A-Z][A-Za-z0-9'&.-]*(?:\s+[A-Z][A-Za-z0-9'&.-]*)*)'s\s+"
        r"(?:top\s+)?(?:fans?|listeners?)\b",
        cleaned,
    )
    artist_name = _compact_text(artist_match.group(1)) if artist_match else ""
    cleaned = re.sub(
        r"^.*?\b(?:we|i)\s+just\s+had\s+to\s+let\s+you\s+know:\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\byep,\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = _compact_text(cleaned).strip(" -:;,.")
    lowered = cleaned.lower()

    if "tour dates" in lowered:
        if artist_name:
            detail = f"{artist_name} has new tour dates"
        else:
            detail = "New tour dates are available"
        if re.search(r"\b(?:headed your way|near you|in your area|coming to your area)\b", lowered):
            detail += " near you"
        return detail
    if "presale" in lowered:
        if artist_name:
            return f"{artist_name} has a presale update"
        return "There is a presale update"
    if artist_name and re.search(r"\b(?:concert|show|live date|tickets?)\b", lowered):
        return f"{artist_name} has a live-event update"
    return ""


def _topic_phrase_from_sentence(sentence_text):
    """Compress a sentence into a shorter topic phrase for digest overviews."""
    text = _strip_read_time_prefix(sentence_text)
    text = re.sub(r"<[^>\n]*(?:href=|src=|alt=|border=|https?://|/\s*a\b|img\b)[^>\n]*>", " ", text, flags=re.IGNORECASE)
    text = _compact_text(_strip_footer_noise_text(text)).strip(" -:;,.")
    if not text:
        return ""

    def _has_topic_detail(fragment_text):
        tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'&./-]*", _compact_text(fragment_text))
        if len(tokens) >= 3:
            return True
        return len(tokens) >= 2 and sum(len(token) for token in tokens) >= 12
    text = re.sub(
        r"^(?:it|this (?:story|article|digest|newsletter)|the email|the sender)\s+"
        r"(?:covers|focuses on|highlights|includes|notes that|mentions|recommends|asks(?: whether)?|"
        r"asks for|reminds you(?: to)?)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"^(?:enjoy|discover|get|shop)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\s*[—-]\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,3}(?:,\s*[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){0,3})+$",
        "",
        text,
    )
    if len(text) > 96:
        split_match = re.search(
            r",\s+(?:with|while|after|before|including)\b|\s+(?:while|after|before|because)\b",
            text,
            flags=re.IGNORECASE,
        )
        if split_match:
            lead = _compact_text(text[: split_match.start()]).strip(" -:;,.")
            tail = _compact_text(text[split_match.end() :]).strip(" -:;,.")
            if _has_topic_detail(lead):
                text = lead
            elif _has_topic_detail(tail):
                text = tail
    if not text:
        return ""
    return text


def _digest_overview_summary(email_data, sender_name, key_sentences):
    """Return a safe paragraph summary for multi-story digests."""
    title = _normalized_header_text(email_data.get("title") or "")
    topics = []
    for sentence in key_sentences or []:
        if _looks_newsletter_intro_line(sentence) or _looks_credit_or_byline_line(sentence):
            continue
        topic = _topic_phrase_from_sentence(_strip_title_prefix(sentence, title))
        if not topic:
            continue
        if any(_token_overlap_ratio(topic, existing) > 0.82 for existing in topics):
            continue
        topics.append(topic)
        if len(topics) >= 3:
            break
    if len(topics) < 2:
        return None

    intro = f"{sender_name} sent a news digest"
    summary = _compact_text(
        f"{_ensure_sentence_ending(intro)} "
        f"{_ensure_sentence_ending(f'It covers {_natural_join(topics)}')}"
    )
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _filtered_digest_questions(email_data, digest_questions):
    """Drop subject-copy or low-value questions when better digest questions exist."""
    title = _normalized_header_text(email_data.get("title") or "")
    filtered = []
    for question in digest_questions or []:
        candidate = _compact_text(question)
        if not candidate:
            continue
        if title and _is_near_subject_copy(candidate, title) and len(digest_questions or []) > 1:
            continue
        filtered.append(candidate)
    return filtered or list(digest_questions or [])


def _extract_digest_questions(raw_body, max_questions=3):
    """Extract distinct featured questions from inline or multiline digest bodies."""
    question_source = repair_body_text(raw_body or "", None).replace("\r\n", "\n").replace("\r", "\n")
    if not question_source:
        return []

    questions = []
    for match in re.finditer(
        r"Question:\s*(.+?)(?=(?:Question:|\r?\n|$))",
        question_source,
        flags=re.IGNORECASE,
    ):
        question = repair_header_text(match.group(1))
        question = _strip_footer_noise_text(question)
        question = _compact_text(question).strip(" -:;,.")
        if not question or len(question) < 18:
            continue
        if not question.endswith("?"):
            question = f"{question}?"
        question = _truncate_compact_text(question, max_chars=200)
        if any(_token_overlap_ratio(question, existing) > 0.9 for existing in questions):
            continue
        questions.append(question)
        if len(questions) >= max_questions:
            break
    if questions:
        return questions

    lines = [
        _compact_text(_strip_footer_noise_text(line)).strip(" -:;,.")
        for line in question_source.split("\n")
        if _compact_text(_strip_footer_noise_text(line)).strip(" -:;,.")
    ]
    lowered_source = question_source.lower()
    question_digest_context = any(
        marker in lowered_source
        for marker in ("question:", "answered", "asked in", "read more", "followers", " digest")
    )
    standalone_question_count = sum(
        1
        for line in lines
        if line.endswith("?")
        and 18 <= len(line) <= 220
        and not _looks_utility_sentence(line)
        and not _looks_newsletter_intro_line(line)
    )
    for index, line in enumerate(lines):
        lowered = line.lower()
        if len(line) < 18 or len(line) > 220:
            continue
        if not line.endswith("?"):
            continue
        if _is_digest_call_to_action_line(line) or _looks_utility_sentence(line):
            continue
        if _looks_newsletter_intro_line(line) or _looks_credit_or_byline_line(line):
            continue
        if lowered.startswith(("asked in ", "answer from ", "read more", "top stories")):
            continue
        lookahead = " ".join(lines[index + 1 : index + 3]).lower()
        if (standalone_question_count < 2 or not question_digest_context) and not any(
            marker in lookahead for marker in ("answered", "asked in", "posted", "read more")
        ):
            continue
        question = _truncate_compact_text(line, max_chars=200)
        if any(_token_overlap_ratio(question, existing) > 0.9 for existing in questions):
            continue
        questions.append(question)
        if len(questions) >= max_questions:
            break
    return questions


def _digest_question_summary(sender_name, digest_questions):
    """Return a structured summary for question-heavy digests."""
    if not digest_questions:
        return None

    intro = f"{sender_name} sent a question digest"

    topic_fragments = []
    for question in digest_questions[:3]:
        topic = repair_body_text(question or "", None).strip().rstrip("?")
        if not topic:
            continue
        replacements = (
            (r"\bhow can i\b", "how to"),
            (r"\bwhat should i do\b", ""),
            (r"\bdo you have to\b", "whether someone has to"),
            (r"\bcan you\b", "whether someone can"),
            (r"\byou\b", "someone"),
        )
        for pattern, replacement in replacements:
            topic = re.sub(pattern, replacement, topic, flags=re.IGNORECASE)
        topic = _compact_text(topic).strip(" -:;,.")
        if not topic:
            continue
        topic = _truncate_compact_text(topic, max_chars=100)
        if any(_token_overlap_ratio(topic, existing) > 0.88 for existing in topic_fragments):
            continue
        topic_fragments.append(topic)
    if not topic_fragments:
        return None

    lead = "It features a question about" if len(topic_fragments) == 1 else "It features questions about"
    summary = _compact_text(
        f"{_ensure_sentence_ending(intro)} "
        f"{_ensure_sentence_ending(f'{lead} {_natural_join(topic_fragments)}')}"
    )
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _is_digest_call_to_action_line(line_text):
    """Return True for short CTA lines commonly repeated beneath digest items."""
    normalized = _compact_text(line_text)
    if not normalized:
        return False
    return bool(
        re.match(
            r"^(?:read(?:\s+(?:more|transcript))?|watch now|listen now|get the details|"
            r"get tickets|learn more|view article|read now|play now(?:\s+\S+)?|see more)\b",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def _extract_bullet_item_names(raw_body, max_items=3):
    """Extract short item names from repeated promotional bullet lines."""
    body = repair_body_text(raw_body or "", None).replace("\r\n", "\n").replace("\r", "\n")
    if not body:
        return []
    items = []
    for match in re.finditer(r"(?m)^\s*(?:->|[-*])\s*([^\n.]{3,80})", body):
        candidate = _compact_text(match.group(1)).strip(" -*:;,.>")
        lowered = candidate.lower()
        if not candidate or _looks_digest_scaffold_line(candidate) or _is_digest_call_to_action_line(candidate):
            continue
        if re.match(r"^(?:see|shop|learn|read|explore)\b", lowered) and len(_text_tokens(candidate)) <= 4:
            continue
        if any(marker in lowered for marker in ("free shipping", "lifetime warranty", "read now", "save now")):
            continue
        if any(_token_overlap_ratio(candidate, existing) > 0.9 for existing in items):
            continue
        items.append(candidate)
        if len(items) >= max_items:
            break
    return items


def _digest_story_blurbs(body_text, max_items=4):
    """Extract descriptive blurb lines for multi-item digests and roundups."""
    raw_text = _email_body_text(body_text) if isinstance(body_text, dict) else repair_body_text(body_text or "", None)
    raw_text = str(raw_text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw_text:
        return []
    email_title = ""
    if isinstance(body_text, dict):
        email_title = _normalized_header_text(body_text.get("title") or "")

    normalized_lines = []
    for raw_line in raw_text.split("\n"):
        raw_line = re.sub(
            r"<[^>\n]*(?:href=|src=|alt=|border=|https?://|/\s*a\b|img\b)[^>\n]*>",
            " ",
            raw_line,
            flags=re.IGNORECASE,
        )
        candidate = re.sub(r"https?://\S+", " ", raw_line)
        candidate = _compact_text(_strip_footer_noise_text(candidate))
        candidate = _strip_read_time_prefix(candidate)
        candidate = re.sub(
            r"\b\d+\s*(?:-\s*\d+)?\s+min(?:ute)?s?\s+read\b[:\-]?\s*",
            "",
            candidate,
            flags=re.IGNORECASE,
        )
        candidate = re.sub(r"^\d+[.)]\s*", "", candidate)
        candidate = re.sub(r"^(?:read now|listen now|watch now|learn more here)\b[:\s-]*", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(
            r"^(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|"
            r"dec(?:ember)?)\s+\d{1,2},\s+\d{4}\s*[-:]\s*",
            "",
            candidate,
            flags=re.IGNORECASE,
        )
        candidate = _compact_text(candidate).strip(" -:;,.")
        if not candidate:
            continue
        if email_title and _is_near_subject_copy(candidate, email_title):
            continue
        if _looks_link_heavy_line(candidate) or _looks_digest_scaffold_line(candidate):
            continue
        if _looks_newsletter_intro_line(candidate) or _looks_credit_or_byline_line(candidate):
            continue
        if _is_digest_call_to_action_line(candidate) or _looks_utility_sentence(candidate) or _is_noise_fragment(candidate):
            continue
        lowered = candidate.lower()
        if any(
            marker in lowered
            for marker in (
                "highlights from across the cbc",
                "edition of the what's news newsletter",
                "free in your inbox daily",
            )
        ):
            continue
        normalized_lines.append(candidate)

    blurbs = []
    index = 0
    while index < len(normalized_lines):
        line = normalized_lines[index]
        candidate = _compact_text(_strip_title_prefix(line, email_title))
        if not candidate or len(candidate) < 18:
            index += 1
            continue
        candidate_tokens = len(_text_tokens(candidate))
        description = ""
        description_index = index
        prefer_following_description = not candidate.endswith((".", "!", "?")) and len(candidate) < 72
        if prefer_following_description or not (
            len(candidate) >= 42 and (candidate.endswith((".", "!", "?")) or candidate_tokens >= 8)
        ):
            for next_index, next_line in enumerate(normalized_lines[index + 1 : index + 5], start=index + 1):
                next_candidate = _compact_text(_strip_title_prefix(next_line, email_title))
                next_tokens = len(_text_tokens(next_candidate))
                if len(next_candidate) < 36 or next_tokens < 7:
                    continue
                description = next_candidate
                description_index = next_index
                break
        if not description and len(candidate) >= 42 and (candidate.endswith((".", "!", "?")) or candidate_tokens >= 8):
            description = candidate
        if not description:
            index += 1
            continue
        if any(_token_overlap_ratio(description, existing) > 0.88 for existing in blurbs):
            index = max(index + 1, description_index + 1)
            continue
        blurbs.append(description)
        index = max(index + 1, description_index + 1)
        if len(blurbs) >= max_items:
            break
    return blurbs


def _title_feature_items(title_text, max_items=3):
    """Extract short featured items from a title like 'Fresh Gear: Trek, Norda & Kids Bikes'."""
    title = _normalized_header_text(title_text or "")
    if not title or ":" not in title:
        return []
    _, remainder = title.split(":", 1)
    if re.search(r"\b(?:%|\$\d|\boff\b|\bdeal\b|\boffer\b)\b", remainder, flags=re.IGNORECASE):
        return []
    raw_parts = re.split(r",|&|\band\b", remainder)
    items = []
    for part in raw_parts:
        candidate = _compact_text(part).strip(" -:;,.")
        if len(candidate) < 3 or len(candidate) > 48:
            continue
        if candidate.lower() in {"more", "and more"}:
            continue
        if _looks_digest_scaffold_line(candidate):
            continue
        if any(_token_overlap_ratio(candidate, existing) > 0.9 for existing in items):
            continue
        items.append(candidate)
        if len(items) >= max_items:
            break
    return items


def _subject_feature_items(title_text, max_items=3):
    """Extract comma-separated featured items from promotional subjects without a colon."""
    title = _normalized_header_text(title_text or "")
    if not title or ":" in title:
        return []
    match = re.search(
        r"\b(?:these|featuring|includes?|with)\s+([^.?!]{8,140})",
        title,
        flags=re.IGNORECASE,
    )
    if not match:
        return []
    raw_parts = re.split(r",|&|\band\b", match.group(1))
    items = []
    for part in raw_parts:
        candidate = _compact_text(part).strip(" -:;,.!?")
        if len(candidate) < 4 or len(candidate) > 48:
            continue
        if candidate.lower() in {"more", "and more"}:
            continue
        if any(_token_overlap_ratio(candidate, existing) > 0.9 for existing in items):
            continue
        items.append(candidate)
        if len(items) >= max_items:
            break
    return items


def _clean_offer_target(target_text):
    """Normalize an offer target phrase so promo summaries stay concise."""
    target = _compact_text(target_text).strip(" -:;,.>")
    if not target:
        return ""
    target = re.sub(r"[\u00b9\u00b2\u00b3\u2070-\u2079]", "", target)
    target = re.split(r"\s+>\s+", target, maxsplit=1)[0]
    target = re.split(r"\s+(?:get|shop|learn|explore)\b", target, maxsplit=1, flags=re.IGNORECASE)[0]
    target = re.sub(r"\b(?:shop now|learn more|get the details|view offer|explore more)\b.*$", "", target, flags=re.IGNORECASE)
    target = re.sub(r"\s+\d+\s*$", "", target)
    target = target.rstrip(" -:;,.>")
    return target


def _clean_offer_phrase(offer_text):
    """Normalize a captured offer phrase into noun-like summary wording."""
    candidate = _compact_text(offer_text).strip(" -:;,.>")
    if not candidate:
        return ""
    candidate = re.sub(r"[\u00b9\u00b2\u00b3\u2070-\u2079]", "", candidate)
    candidate = re.split(r"\s+>\s+", candidate, maxsplit=1)[0]
    candidate = re.sub(r"\b(?:shop now|learn more|get the details|view offer|explore more)\b.*$", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s+\d+\s*$", "", candidate)
    candidate = re.sub(r"^(?:save|get|enjoy)\s+", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\bfree\s+for\s+(\d+)\s+weeks?\b", lambda match: f"{match.group(1)}-week free trial", candidate, flags=re.IGNORECASE)
    candidate = candidate.rstrip(" -:;,.>")
    if candidate.lower() in {"free shipping", "lifetime warranty"}:
        candidate = candidate.lower()
    return candidate


def _offer_phrase_is_generic(offer_text):
    """Return True for low-information promo perks that should trail real discounts."""
    lowered = _compact_text(offer_text).lower()
    if not lowered:
        return True
    return lowered in {"free shipping", "lifetime warranty"} or lowered.endswith("delivery fee")


def _extract_offer_phrases(raw_text, max_items=4):
    """Extract concise offer phrases from promotional copy."""
    text = repair_body_text(raw_text or "", None)
    if not text:
        return []
    patterns = (
        (
            r"\b(save|get)\s+up to\s+(\$\d+(?:,\d{3})*(?:\.\d{2})?)(?:\s+(off|in))?\s+(?:on\s+)?([^.!\n]{3,80})",
            lambda match: (
                f"up to {match.group(2)} in {_clean_offer_target(match.group(4))}"
                if (match.group(3) or "").lower() == "in"
                else f"up to {match.group(2)} off {_clean_offer_target(match.group(4))}"
            ),
        ),
        (
            r"\bearn\s+up to\s+(\$\d+(?:,\d{3})*(?:\.\d{2})?)\b",
            lambda match: f"up to {match.group(1)} in referral rewards",
        ),
        (
            r"\b(save|get)\s+up to\s+(\d{1,3}%)(?:\s+off)?\s+(?:when you buy\s+([^.!\n]{3,80})|(?:on\s+)?([^.!\n]{3,80}))",
            lambda match: (
                f"up to {match.group(2)} off when buying {_clean_offer_target(match.group(3))}"
                if match.group(3)
                else f"up to {match.group(2)} off {_clean_offer_target(match.group(4))}"
            ),
        ),
        (
            r"\bget\s+([^.!\n]{3,60}?)\s+for as low as\s+(\$\d+(?:,\d{3})*(?:\.\d{2})?)\s+with a qualifying trade-in",
            lambda match: (
                f"{_clean_offer_target(match.group(1))} for as low as {match.group(2)} with a qualifying trade-in"
            ),
        ),
        (
            r"\bget\s+(a\s+gift\s+card\s+worth\s+up to\s+\$\d+(?:,\d{3})*(?:\.\d{2})?\s+with a qualifying\s+[^.!\n]{3,60})",
            lambda match: _clean_offer_target(match.group(1)),
        ),
        (
            r"\b(\d+)\s*weeks?\s+free\b",
            lambda match: f"{int(match.group(1))}-week free trial",
        ),
        (r"\b((?:\$0|\$\d+(?:\.\d{2})?)\s+Delivery Fee(?:\s+on\s+eligible\s+orders)?)\b", None),
        (r"\b(\d+%\s+Uber One credits on eligible rides)\b", None),
        (r"\b(?:get|enjoy)\s+((?:up to\s+)?\d+%\s+off(?:\s+your)?\s+next\s+\d+\s+orders?(?:\s+of\s+\$\d+\s+or\s+more)?)\b", None),
        (r"\b(all for\s+\$\d+(?:\.\d{2})?\s+off)\b", None),
        (r"\b(\$\d+(?:\.\d{2})?\s+off)\b", None),
        (r"\b(free shipping)\b", None),
        (r"\b(lifetime warranty)\b", None),
        (r"\b(\$\d+(?:\.\d{2})?\s+CAD\s+Trading Voucher)\b", None),
    )
    matches = []
    for pattern, normalizer in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            if normalizer:
                candidate = normalizer(match)
            else:
                candidate = match.group(1)
            candidate = _clean_offer_phrase(candidate)
            if not candidate:
                continue
            if candidate.lower().startswith("all for "):
                candidate = candidate[8:]
            matches.append((match.start(), candidate))

    if not matches:
        return []

    matches.sort(key=lambda item: item[0])
    concrete = []
    generic = []
    for _, candidate in matches:
        bucket = generic if _offer_phrase_is_generic(candidate) else concrete
        if any(_token_overlap_ratio(candidate, existing) > 0.9 for existing in concrete + generic):
            continue
        bucket.append(candidate)

    phrases = concrete[:max_items]
    if len(phrases) < max_items:
        phrases.extend(generic[: max_items - len(phrases)])
    return phrases[:max_items]


def _promotion_source_context(email_data, max_chars=5000):
    """Return shared text inputs used by promo classification and summary logic."""
    title = _normalized_header_text(email_data.get("title") or "")
    raw_body = _email_body_text(email_data)
    cleaned_body = _clean_body_for_prompt(email_data, max_chars=max_chars)
    combined = _compact_text(
        " ".join(part for part in [title, cleaned_body or raw_body] if part)
    )
    return {
        "title": title,
        "raw_body": raw_body,
        "cleaned_body": cleaned_body or raw_body,
        "combined": combined,
    }


def _looks_marketing_promo_title(title_text):
    """Return True for subject lines that look like ad copy instead of editorial topics."""
    title = _normalized_header_text(title_text or "")
    if not title or title.lower() == "(no subject)":
        return False
    lowered = title.lower()
    if lowered.startswith("save the date"):
        return False
    if re.search(
        r"\b(?:\d{1,3}%\s+off|\$\d+(?:,\d{3})*(?:\.\d{2})?\s+off|free\s+(?:trial|shipping)|"
        r"deal(?:s)?|offer(?:s)?|sale|discount|coupon|promo|membership|member week|savings?|"
        r"bundle|kit|gear)\b",
        lowered,
        flags=re.IGNORECASE,
    ):
        return True
    return bool(
        re.match(
            r"^(?:score\s+big|save\s+big|save\s+up\s+to|shop\b|unlock(?:\s+savings)?|"
            r"last chance|claim\b|grab\b|treat yourself)\b",
            lowered,
            flags=re.IGNORECASE,
        )
    )


def _promotion_theme_phrase(source_text):
    """Return a neutral promo theme label without copying marketing wording."""
    normalized = _compact_text(source_text).lower()
    if not normalized:
        return ""
    if re.search(r"\brefer friends\b|\breferral\b|\bshare your code\b", normalized):
        return "referral rewards"
    if re.search(r"\bfree\s+trial\b|\b\d+\s*-\s*week free trial\b|\b\d+\s+weeks?\s+free\b", normalized):
        return "a free trial"
    if re.search(r"\bmembership\b|\bmember(?:s| week| exclusive)\b", normalized):
        return "membership perks"
    if re.search(r"\bnew arrivals?\b|\bnew collection\b|\bfresh arrivals?\b", normalized):
        return "new arrivals"
    if re.search(r"\bdiscount\b|\b\d{1,3}%\s+off\b|\$\d+(?:,\d{3})*(?:\.\d{2})?\s+off\b|\bsavings?\b", normalized):
        return "discounts"
    if re.search(r"\bdeals\b", normalized):
        return "special deals"
    if re.search(r"\boffers?\b", normalized):
        return "special offers"
    return ""


def _promotion_item_phrase(item_text):
    """Convert a product/item label into a slightly more natural summary phrase."""
    item = _compact_text(item_text).strip(" -:;,.")
    if not item:
        return ""
    if item.lower().startswith("the "):
        item = item[4:]
    item = re.sub(
        r"\b(?:today|tonight|now|online|this week|this weekend|this month)\b$",
        "",
        item,
        flags=re.IGNORECASE,
    ).strip(" -:;,.")
    if not item:
        return ""
    lowered = item.lower()
    if re.fullmatch(r"jumper\s+\d{4}", lowered):
        return f"{item} bag"
    if lowered == "camera top insert":
        return "a camera top insert"
    if lowered == "packable rain cover":
        return "a packable rain cover"
    if lowered.endswith(" family"):
        return f"{item[:-7].strip()} bikes"
    if lowered == "kids bikes":
        return "kids' bikes"
    if lowered.endswith(" runners"):
        return f"{item} shoes"
    return item


def _promotion_summary(email_data):
    """Build a compact deterministic summary for commercial bulk promotions."""
    if not _looks_bulk_or_newsletter(email_data):
        return None
    profile = _summary_profile(email_data)
    sender_name = _summary_sender_name(email_data)
    context = _promotion_source_context(email_data, max_chars=5000)
    title = context["title"]
    raw_body = context["raw_body"]
    sender_info = _sender_parts(email_data.get("sender"))
    combined = context["combined"].lower()
    promotion_assessment = _commercial_promotion_assessment(
        combined,
        bulk_signal=True,
        sender_automated=_sender_looks_automated(sender_info),
        transactional_like=False,
    )
    title_items = _title_feature_items(title, max_items=3) or _subject_feature_items(title, max_items=3)
    bullet_items = _extract_bullet_item_names(raw_body, max_items=3)
    offer_phrases = _extract_offer_phrases(context["combined"], max_items=4)
    title_promotional = _looks_marketing_promo_title(title)
    promo_theme = _promotion_theme_phrase(context["combined"])
    has_promo_signal = (
        promotion_assessment["commercial"]
        or bool(offer_phrases)
        or (
            title_promotional
            and (
                bool(title_items)
                or bool(bullet_items)
                or bool(promo_theme)
            )
        )
    )
    if not has_promo_signal:
        return None

    intro = f"A promotional update from {sender_name}"
    sentences = [_ensure_sentence_ending(intro)]

    featured_items = [_promotion_item_phrase(item) for item in (bullet_items or title_items)]
    featured_items = [item for item in featured_items if item]
    detail_sentence = ""
    if featured_items and offer_phrases:
        detail_sentence = (
            f"The message promotes {_natural_join(featured_items[:3])} "
            f"with {_natural_join(offer_phrases[:3])}"
        )
    elif offer_phrases:
        detail_sentence = f"The message advertises {_natural_join(offer_phrases[:3])}"
    elif featured_items and promo_theme:
        detail_sentence = (
            f"The message promotes {_natural_join(featured_items[:3])} with {promo_theme}"
        )
    elif featured_items:
        detail_sentence = f"The message promotes {_natural_join(featured_items[:3])}"
    elif promo_theme:
        detail_sentence = f"The message focuses on {promo_theme}"
    if detail_sentence:
        sentences.append(_ensure_sentence_ending(detail_sentence))
    summary = _compact_text(" ".join(sentences))
    if not summary:
        return None
    if len(summary) > profile["char_limit"]:
        summary = f"{summary[: profile['char_limit'] - 3].rstrip()}..."
    return summary


def _staff_update_summary(email_data):
    """Build a compact summary for short appreciation/progress updates."""
    if _looks_bulk_or_newsletter(email_data) or _looks_actionable(email_data):
        return None
    title = _normalized_header_text(email_data.get("title") or "")
    body = _clean_body_for_prompt(email_data, max_chars=2200)
    lowered = body.lower()
    if not body or not any(marker in lowered for marker in ("thank", "appreciate")):
        return None
    if not any(marker in lowered for marker in ("team", "company", "leadership", "employees", "organization", "collaboration")):
        return None

    speaker = "HR" if re.search(r"\bhr\b", title, flags=re.IGNORECASE) else _summary_sender_name(email_data)
    sentences = [
        _ensure_sentence_ending(f"{speaker} thanks the team for its hard work, dedication, and collaboration")
    ]
    progress_match = re.search(
        r"(?:this month|recently)[^.]*(?:made|seen)\s+(?:meaningful\s+)?progress[^.]*?(?:from|in)\s+([^.]+)",
        body,
        flags=re.IGNORECASE,
    )
    if progress_match:
        progress_text = _compact_text(progress_match.group(1)).strip(" -:;,.")
        progress_text = re.sub(r"\blaunching\b", "launching", progress_text, flags=re.IGNORECASE)
        if progress_text:
            sentences.append(
                _ensure_sentence_ending(f"It says the company made progress in {progress_text}")
            )
    elif "progress" in lowered:
        sentences.append(
            _ensure_sentence_ending("It says the company has made progress across several parts of the business")
        )

    if "shared values" in lowered or "define who we are" in lowered:
        sentences.append(
            _ensure_sentence_ending("It ties those efforts to the organization's shared values")
        )
    summary = _compact_text(" ".join(sentences))
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _article_teaser_phrase(sentence_text):
    """Compress a single-article teaser into a shorter paraphrased topic phrase."""
    sentence = _compact_text(sentence_text).strip(" -:;,.")
    if not sentence:
        return ""
    marketing_teaser = _paraphrase_marketing_alert_teaser(sentence)
    if marketing_teaser:
        return marketing_teaser
    patterns = (
        (
            r"^(?P<lead>.+?)\s+after\s+(?P<context>.+?),\s+(?P<speaker>[A-Z][A-Za-z0-9'&.\- ]+?)\s+says\s+(?P<lemma>.+)$",
            lambda match: (
                f"{match.group('speaker')} says {match.group('lemma')} "
                f"after {match.group('context')}"
            ),
        ),
        (
            r"^(?P<entity>.+?)\s+will\s+invest\s+(?P<amount>.+?)\s+in\s+(?P<target>.+?)\s+to\s+expand\s+(?P<goal>.+)$",
            lambda match: (
                f"{match.group('entity')}'s {match.group('amount')} investment in "
                f"{match.group('target')} to expand {match.group('goal')}"
            ),
        ),
        (
            r"^(?P<name>[A-Z][A-Za-z0-9'&.\- ]+?),\s+[^,]+,\s+is\s+(?P<role>.+)$",
            lambda match: f"{match.group('name')}'s role as {match.group('role')}",
        ),
        (
            r"^(?:A|An)\s+judge\s+ruled\s+that\s+(?P<lemma>.+?),\s+but\s+(?P<tail>.+)$",
            lambda match: (
                f"a ruling allowed "
                f"{re.sub(r'\\s+can\\s+', ' to ', match.group('lemma'), count=1, flags=re.IGNORECASE)}, "
                f"while {match.group('tail')}"
            ),
        ),
        (
            r"^(?P<entity>.+?)\s+has\s+.+?\s+to\s+compete\s+with\s+(?P<rivals>.+?)\s+by\s+offering\s+(?P<offer>.+)$",
            lambda match: (
                f"{match.group('entity')}'s push to compete with {match.group('rivals')} "
                f"through {match.group('offer')}"
            ),
        ),
        (
            r"^(?P<entity>.+?)\s+(?P<verb>are|is)\s+set\s+to\s+spend\s+(?P<amount>.+?)\s+on\s+(?P<target>.+?),\s+largely\s+to\s+meet\s+demand\s+from\s+(?P<cause>.+)$",
            lambda match: (
                f"{match.group('entity')} {'plan' if match.group('verb').lower() == 'are' else 'plans'} "
                f"to spend {match.group('amount')} on {match.group('target')} to meet demand from {match.group('cause')}"
            ),
        ),
    )
    for pattern, builder in patterns:
        match = re.match(pattern, sentence, flags=re.IGNORECASE)
        if match:
            return _compact_text(builder(match)).strip(" -:;,.")
    return _topic_phrase_from_sentence(sentence)


def _single_article_alert_summary(email_data):
    """Return a deterministic summary for a single-story editorial alert."""
    if not _looks_single_article_alert(email_data):
        return None
    sender_name = _summary_sender_name(email_data)
    title = _normalized_header_text(email_data.get("title") or "")
    base_teaser_lines = [
        _compact_text(_strip_title_prefix(line, title))
        for line in _clean_body_for_prompt(email_data, max_chars=2400).split("\n")
        if _compact_text(line)
    ]
    teaser_candidates = []

    def _add_teaser_candidate(candidate_text):
        candidate = _compact_text(candidate_text).strip(" -:;,.")
        if not candidate:
            return
        candidate_sentences = _extract_key_sentences(candidate, max_sentences=1)
        if candidate_sentences:
            candidate = _compact_text(candidate_sentences[0]).strip(" -:;,.")
        if not candidate:
            return
        if any(
            candidate == existing
            or (
                _token_overlap_ratio(candidate, existing) > 0.96
                and abs(len(candidate) - len(existing)) < 24
            )
            for existing in teaser_candidates
        ):
            return
        teaser_candidates.append(candidate)

    def _article_phrase_sentence(phrase_text):
        phrase = _compact_text(phrase_text).strip(" -:;,.")
        if not phrase:
            return ""
        if (
            re.match(r"^(?:a|an|the)\b", phrase, flags=re.IGNORECASE)
            or (
                phrase[:1].isupper()
                and re.search(
                    r"\b(?:is|are|was|were|says|say|plans?|warns?|gets?|keeps?|faces?|becomes?|let|allowed)\b",
                    phrase,
                    flags=re.IGNORECASE,
                )
            )
        ):
            phrase = phrase[0].upper() + phrase[1:] if phrase else phrase
            return _ensure_sentence_ending(phrase)
        return _ensure_sentence_ending(f"It reports on {phrase}")

    for index, line in enumerate(base_teaser_lines):
        _add_teaser_candidate(line)
        if index + 1 >= len(base_teaser_lines):
            continue
        next_line = base_teaser_lines[index + 1]
        if not line or not next_line:
            continue
        if _looks_utility_sentence(line) or _looks_utility_sentence(next_line):
            continue
        if _is_noise_fragment(line) or _is_noise_fragment(next_line):
            continue
        should_merge = (
            len(line) < 48
            or len(_text_tokens(line)) <= 8
            or not re.search(r"[.!?]$", line)
            or next_line[:1].islower()
        )
        if should_merge:
            _add_teaser_candidate(f"{line} {next_line}")

    detail_sentence = ""
    for line in teaser_candidates:
        if len(line) < 24:
            continue
        if _looks_utility_sentence(line) or _is_noise_fragment(line):
            continue
        phrase = _article_teaser_phrase(line)
        if phrase and phrase != line and len(_text_tokens(phrase)) >= 2:
            detail_sentence = _article_phrase_sentence(phrase)
            break
        rewritten = _rewrite_fallback_summary_sentence(line, email_data)
        if rewritten and not _summary_uses_subject_content(rewritten, email_data):
            detail_sentence = rewritten
            break
    if not detail_sentence:
        for sentence in _extract_key_sentences(email_data, max_sentences=4):
            candidate = _compact_text(_strip_title_prefix(sentence, title))
            if not candidate:
                continue
            phrase = _article_teaser_phrase(candidate)
            if phrase and phrase != candidate and len(_text_tokens(phrase)) >= 2:
                detail_sentence = _article_phrase_sentence(phrase)
                break
            rewritten = _rewrite_fallback_summary_sentence(candidate, email_data)
            if rewritten and not _summary_uses_subject_content(rewritten, email_data):
                detail_sentence = rewritten
                break
    if not detail_sentence:
        return _ensure_sentence_ending(f"An article alert from {sender_name}")
    summary = _compact_text(
        f"{_ensure_sentence_ending(f'An article alert from {sender_name}')} "
        f"{detail_sentence}"
    )
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _extract_digest_item_titles(body_text, max_items=4):
    """Extract repeated card/list item titles from newsletter-style bodies."""
    raw_text = _email_body_text(body_text) if isinstance(body_text, dict) else repair_body_text(body_text or "", None)
    raw_text = str(raw_text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw_text:
        return []
    email_title = ""
    if isinstance(body_text, dict):
        email_title = _normalized_header_text(body_text.get("title") or "")

    non_empty_lines = [
        _compact_text(line)
        for line in raw_text.split("\n")
        if _compact_text(line)
    ]
    if not non_empty_lines:
        return []

    titles = []
    skip_markers = (
        "newsletter",
        "digest",
        "briefing",
        "copyright",
        "unsubscribe",
        "preferences",
        "view in browser",
        "mailing address",
        "all rights reserved",
        "subscribed",
        "support our mission",
        "latest in ",
        "today in ",
        "don't miss this",
        "top stories",
        "today's news",
        "the number",
        "spotlight",
        "catch up",
        "sponsored by",
        "content from",
    )
    for index, line in enumerate(non_empty_lines):
        if _is_digest_call_to_action_line(line):
            continue
        cleaned_line = _compact_text(_strip_footer_noise_text(line))
        candidate = cleaned_line.strip(" -:;,.")
        if not candidate:
            continue
        lowered = candidate.lower()
        if any(marker in lowered for marker in skip_markers):
            continue
        if _looks_newsletter_intro_line(candidate) or _looks_credit_or_byline_line(candidate):
            continue
        if _looks_digest_scaffold_line(candidate):
            continue
        if _looks_utility_sentence(candidate) or _is_noise_fragment(candidate):
            continue
        if len(candidate) < 18 or len(candidate) > 120:
            continue
        if cleaned_line.endswith("."):
            continue
        tokens = _text_tokens(candidate)
        if len(tokens) < 3 or len(tokens) > 16:
            continue
        if sum(character.isalpha() for character in candidate) < 12:
            continue
        previous_lines = " ".join(non_empty_lines[max(0, index - 2) : index]).lower()
        if "sponsored by" in previous_lines or "content from" in previous_lines:
            continue
        lookahead = non_empty_lines[index + 1 : index + 4]
        if not any(_is_digest_call_to_action_line(next_line) for next_line in lookahead):
            continue
        if any(_token_overlap_ratio(candidate, existing) > 0.9 for existing in titles):
            continue
        titles.append(candidate)
        if len(titles) >= max_items:
            break
    return titles


def _digest_item_titles_summary(sender_name, item_titles):
    """Return a compact summary for card-style newsletters with repeated featured items."""
    if len(item_titles or []) < 2:
        return None

    featured_titles = list(item_titles[:3])
    intro = f"{sender_name} sent a newsletter"
    featured_line = f"It features {_natural_join(featured_titles)}"
    if len(item_titles) > len(featured_titles):
        featured_line += ", plus more"
    summary = _compact_text(
        f"{_ensure_sentence_ending(intro)} {_ensure_sentence_ending(featured_line)}"
    )
    return summary[:SUMMARY_MAX_CHARS] if summary else None


def _looks_multi_item_digest(body_text, key_sentences):
    """Return True when a bulk message is clearly a multi-story digest."""
    if isinstance(body_text, dict) and not _looks_bulk_or_newsletter(body_text):
        return False
    cleaned = _clean_body_for_prompt(body_text or "", max_chars=5000)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    digest_markers = (
        "other stories include",
        "the newsletter also features",
        "additionally",
        "in the technology section",
        "in the business section",
        "in the health section",
        "top stories",
        "latest stories",
        "featured stories",
        "roundup",
        "digest",
        "briefing",
    )
    marker_hits = sum(marker in lowered for marker in digest_markers)
    read_time_hits = len(
        re.findall(
            r"\b\d+\s*(?:-\s*\d+)?(?:\s*-\s*|\s+)(?:min(?:ute)?s?)\s+read\b",
            cleaned,
            flags=re.IGNORECASE,
        )
    )
    digest_item_titles = _extract_digest_item_titles(body_text, max_items=6)
    digest_blurbs = _digest_story_blurbs(body_text, max_items=6)
    if len(digest_blurbs) >= 2:
        if (
            len(key_sentences or []) <= 2
            and marker_hits < 2
            and read_time_hits < 2
            and len(digest_item_titles) < 2
        ):
            return False
        return True
    if isinstance(body_text, dict):
        email_title = _normalized_header_text(body_text.get("title") or "")
        subject_copy_titles = [
            item_title
            for item_title in digest_item_titles
            if email_title and _is_near_subject_copy(item_title, email_title)
        ]
        if subject_copy_titles and len(digest_item_titles) <= 2 and len(key_sentences or []) <= 2:
            digest_item_titles = [
                item_title
                for item_title in digest_item_titles
                if item_title not in subject_copy_titles
            ]
    if len(digest_item_titles) >= 2:
        return True
    heading_like_lines = 0
    for raw_line in cleaned.split("\n"):
        line = _compact_text(raw_line)
        if not line:
            continue
        if len(line) < 18 or len(line) > 120:
            continue
        if line.endswith((".", "!", "?")):
            continue
        if line.lower().startswith(
            ("from:", "to:", "cc:", "bcc:", "sent:", "subject:", "question:", "answer from")
        ):
            continue
        if sum(character.isalpha() for character in line) < 12:
            continue
        heading_like_lines += 1
    if marker_hits >= 2:
        return True
    if read_time_hits >= 2:
        return True
    if heading_like_lines >= 3 and len(key_sentences or []) >= 4:
        return True
    return len(key_sentences or []) >= 5 and bool(marker_hits or read_time_hits or heading_like_lines >= 2)


def _looks_single_article_alert(email_data):
    """Return True for short bulk/editorial alerts that cover one main article."""
    if not _looks_bulk_or_newsletter(email_data):
        return False
    title = _normalized_header_text(email_data.get("title") or "")
    if not title or title.lower() == "(no subject)":
        return False
    key_sentences = _extract_key_sentences(email_data, max_sentences=4)
    if not key_sentences or len(key_sentences) > 2:
        return False
    return not _looks_multi_item_digest(email_data, key_sentences)


def _should_use_structured_summary(email_data):
    """Return True when the email is better summarized as separate bullet items."""
    key_sentences = _extract_key_sentences(
        email_data,
        max_sentences=10,
    )
    if not key_sentences:
        return False
    if _looks_multi_item_digest(email_data, key_sentences):
        return True
    profile = _summary_profile(email_data)
    if profile["output_sentences"] < 5:
        return False
    body = _clean_body_for_prompt(email_data, max_chars=12000)
    return len(key_sentences) >= 6 and len(body) >= 1600


def _format_summary_list(items, char_limit):
    """Render summary items as a spaced plain-text bullet list."""
    bullets = []
    for item in items:
        line = _compact_text(item).strip(" -:*")
        if not line:
            continue
        line = _ensure_sentence_ending(line)
        if any(_token_overlap_ratio(line, existing) > 0.9 for existing in bullets):
            continue
        candidate = bullets + [line]
        rendered = "\n\n".join(f"- {entry}" for entry in candidate)
        if len(rendered) > char_limit and bullets:
            break
        if len(rendered) > char_limit:
            trimmed = line[: max(0, char_limit - 5)].rstrip(" ,;:-")
            if trimmed:
                bullets.append(trimmed)
            break
        bullets.append(line)
    if not bullets:
        return None
    return "\n\n".join(f"- {entry}" for entry in bullets)


def _normalize_summary_layout(summary_text):
    """Normalize inline bullet-like output into properly separated lines."""
    value = repair_body_text(summary_text or "", None).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not value:
        return ""
    marker_matches = re.findall(
        r"(?:^|\s)(?:[-*]|\d+[.)])\s+(?=[A-Z0-9\"'])",
        value,
    )
    if len(marker_matches) >= 2:
        value = re.sub(
            r"\s+((?:[-*]|\d+[.)])\s+(?=[A-Z0-9\"']))",
            r"\n\n\1",
            value,
        )
    return re.sub(r"\n{3,}", "\n\n", value).strip()


def _bulk_newsletter_summary(email_data):
    """Return deterministic summary for newsletter/promotional emails."""
    if not _looks_bulk_or_newsletter(email_data):
        return None

    title = _normalized_header_text(email_data.get("title") or "")
    raw_body = _email_body_text(email_data)
    body = _clean_body_for_prompt(email_data, max_chars=12000)
    combined = _compact_text(" ".join(part for part in [title, body] if part))
    if not combined:
        return None

    sender_name = _summary_sender_name(email_data)
    key_sentences = _extract_key_sentences(email_data, max_sentences=6)
    multi_item_digest = _looks_multi_item_digest(raw_body, key_sentences)
    digest_blurbs = _digest_story_blurbs(email_data, max_items=4)

    # Quora/news digests are better summarized by the featured topics than by model paraphrase.
    digest_questions = _filtered_digest_questions(
        email_data,
        _extract_digest_questions(raw_body, max_questions=3),
    )
    if digest_questions:
        digest_summary = _digest_question_summary(
            sender_name=sender_name,
            digest_questions=digest_questions,
        )
        if digest_summary:
            return digest_summary

    prompt_summary = _prompt_reminder_summary(email_data)
    if prompt_summary:
        return prompt_summary

    activity_summary = _activity_notification_summary(email_data)
    if activity_summary:
        return activity_summary

    job_alert_summary = _job_alert_summary(email_data)
    if job_alert_summary:
        return job_alert_summary

    promotion_summary = _promotion_summary(email_data)
    if promotion_summary:
        return promotion_summary

    article_alert_summary = _single_article_alert_summary(email_data)
    if article_alert_summary:
        return article_alert_summary

    digest_item_titles = _extract_digest_item_titles(raw_body, max_items=4)
    if len(digest_item_titles) >= 2:
        title_repeats_lead_story = bool(
            title and any(_is_near_subject_copy(item_title, title) for item_title in digest_item_titles)
        )
        card_style_newsletter = bool(
            re.search(
                r"\b(?:read transcript|weekly curated newsletter|water cooler trivia)\b",
                raw_body,
                flags=re.IGNORECASE,
            )
        )
        if not (title_repeats_lead_story and len(digest_blurbs) >= 2 and not card_style_newsletter):
            digest_summary = _digest_item_titles_summary(
                sender_name=sender_name,
                item_titles=digest_item_titles,
            )
            if digest_summary:
                return digest_summary

    if len(digest_blurbs) >= 2:
        digest_summary = _digest_overview_summary(
            email_data,
            sender_name=sender_name,
            key_sentences=digest_blurbs,
        )
        if digest_summary:
            return digest_summary

    if multi_item_digest:
        digest_summary = _digest_overview_summary(
            email_data,
            sender_name=sender_name,
            key_sentences=key_sentences,
        )
        if digest_summary:
            return digest_summary

    # Ticket promos should emphasize the offer window, code, and a sample of dates/locations.
    if re.search(r"\bupcoming tour\b", combined, flags=re.IGNORECASE) and re.search(
        r"\bpresale starts\b",
        combined,
        flags=re.IGNORECASE,
    ):
        presale = _first_regex_match(raw_body, r"Presale starts at [^\n]+")
        offer_code_match = re.search(r"Offer code:\s*\*?([A-Z0-9-]+)\*?", raw_body, flags=re.IGNORECASE)
        offer_code = offer_code_match.group(1) if offer_code_match else ""
        ends = _first_regex_match(raw_body, r"Ends [^\n]+")
        date_matches = re.findall(
            r"\*?([A-Za-z .'-]+,\s*[A-Z]{2})\*?\s*\n+\s*([A-Z][a-z]+ \d{1,2}, \d{4})",
            raw_body,
        )
        sample_dates = [f"{city} on {date}" for city, date in date_matches[:2]]
        sentences = [
            _ensure_sentence_ending(
                f"{sender_name} is offering presale access to an upcoming tour"
            )
        ]
        detail_bits = [bit for bit in [presale, f"Offer code {offer_code}" if offer_code else "", ends] if bit]
        if detail_bits:
            sentences.append(_ensure_sentence_ending(" ".join(detail_bits)))
        if sample_dates:
            sentences.append(
                _ensure_sentence_ending(
                    f"The email lists dates including {' and '.join(sample_dates)}"
                )
            )
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    # Subscription upsells should emphasize the product, benefits, and pricing.
    if re.search(r"\bpremium\b", combined, flags=re.IGNORECASE) and re.search(
        r"(?:free trial|\$\d+(?:\.\d{2})?)",
        combined,
        flags=re.IGNORECASE,
    ):
        trial = _first_regex_match(raw_body, r"Try it free for \d+\s+days?")
        price = _first_regex_match(raw_body, r"\$\d+(?:\.\d{2})?/\w+")
        perks = []
        if "eliminate the ads" in combined or "say goodbye to ads" in combined:
            perks.append("ad-free viewing")
        if "cbc news network" in combined:
            perks.append("CBC News Network access")
        if "early premieres" in combined:
            perks.append("early premieres on select titles")
        sentences = [
            _ensure_sentence_ending(
                f"{sender_name} is promoting a Premium upgrade"
            )
        ]
        if perks:
            sentences.append(
                _ensure_sentence_ending(f"It adds {_natural_join(perks)}")
            )
        pricing_bits = [bit for bit in [trial, price] if bit]
        if pricing_bits:
            sentences.append(_ensure_sentence_ending(" ".join(pricing_bits)))
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    numbered_sections = []
    for match in re.finditer(r"(?:^|\s)(\d+)\.\s+(.*?)(?=(?:\s+\d+\.\s+)|$)", body, flags=re.DOTALL):
        section_text = _compact_text(match.group(2))
        if not section_text:
            continue
        section_lead = _extract_key_sentences(section_text, max_sentences=1)
        candidate = section_lead[0] if section_lead else section_text
        candidate = _strip_title_prefix(candidate, title)
        candidate = _strip_footer_noise_text(candidate)
        candidate = _compact_text(candidate)
        if not candidate or _looks_utility_sentence(candidate) or _is_noise_fragment(candidate):
            continue
        numbered_sections.append(candidate)

    if numbered_sections:
        sentences = [_ensure_sentence_ending(f"{sender_name} sent a newsletter")]
        lead = numbered_sections[0]
        if lead and not _is_near_subject_copy(lead, title):
            sentences.append(_ensure_sentence_ending(f"It covers {lead}"))
        if len(numbered_sections) > 1:
            sentences.append(_ensure_sentence_ending(f"It also covers {numbered_sections[1]}"))
        summary = _compact_text(" ".join(sentences))
        if summary:
            return summary[:SUMMARY_MAX_CHARS]

    # Article alerts should prefer a natural teaser sentence over awkward topic-clause rewrites.
    if title and key_sentences and not multi_item_digest:
        teaser = next(
            (
                _strip_title_prefix(sentence, title)
                for sentence in key_sentences
                if not _looks_utility_sentence(sentence)
            ),
            "",
        )
        teaser = _strip_footer_noise_text(teaser)
        teaser = _compact_text(teaser)
        if teaser and len(teaser) >= 30 and len(body) < 1800:
            marketing_teaser = _paraphrase_marketing_alert_teaser(teaser)
            if marketing_teaser:
                summary = _compact_text(
                    f"{sender_name} sent an article alert. "
                    f"{_ensure_sentence_ending(marketing_teaser)}"
                )
            else:
                detail_sentence = _rewrite_fallback_summary_sentence(teaser, email_data)
                if detail_sentence and not _summary_uses_subject_content(
                    detail_sentence,
                    email_data,
                ):
                    summary = _compact_text(
                        f"{sender_name} sent an article alert. {detail_sentence}"
                    )
                else:
                    summary = _compact_text(f"{sender_name} sent an article alert.")
            if summary and len(summary) <= SUMMARY_MAX_CHARS:
                return summary

    if len(body) >= 1200:
        # Long newsletters need real summarization, not the short promo fallback.
        return None

    if len(key_sentences) >= 4:
        return None

    promotion_markers = (
        "% off",
        "promo",
        "discount",
        "offer",
        "save up to",
        "maximum savings",
        "limited time",
        "special offer",
        "shop now",
        "book now",
        "membership",
        "sale",
        "coupon",
    )
    editorial_markers = (
        "top stories",
        "briefing",
        "digest",
        "newsletter",
        "what happened",
        "highlights",
        "roundup",
        "analysis",
        "week in review",
        "today in",
        "latest news",
    )
    promotion_hits = _matching_patterns(combined, promotion_markers)
    editorial_hits = _matching_patterns(combined, editorial_markers)
    offer = _first_regex_match(combined, r"\b\d{1,3}%\s+off\b")
    trip_limit = _first_regex_match(combined, r"\b(?:up to\s+)?\d+\s+(?:trips?|rides?)\b")
    validity = _first_regex_match(
        combined,
        r"\b(?:valid through|through|until|expires?(?: on)?)\s+"
        r"(?:[A-Z][a-z]+ \d{1,2}, \d{4}(?: \d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm))?|"
        r"\d{1,2}/\d{1,2}/\d{2,4})",
    )
    max_savings = _first_regex_match(
        combined,
        r"\b(?:maximum savings of|save up to|up to)\s*\$\d+(?:\.\d{2})?(?:\s+per\s+(?:ride|trip|order|item))?",
    )
    auto_applied = bool(
        re.search(
            r"\b(?:promo|discount|offer)\s+has\s+automatically\s+been\s+applied\b",
            combined,
            flags=re.IGNORECASE,
        )
    )
    if editorial_hits and len(promotion_hits) < 3 and not offer and not max_savings:
        return None
    if not (offer or max_savings or auto_applied or len(promotion_hits) >= 2):
        return None

    features = []
    feature_patterns = (
        (r"\b(?:reserve your ride|schedule your ride|flight schedule|departure gate)\b", "ride scheduling"),
        (r"\bcommute alerts?\b", "commute alerts"),
        (r"\b(?:pin verification|safety preferences|night rides?)\b", "night-ride safety settings"),
        (r"\b(?:uber one|membership)\b", "membership perks"),
    )
    for pattern, label in feature_patterns:
        if re.search(pattern, combined, flags=re.IGNORECASE):
            features.append(label)

    sentences = []
    if offer:
        offer_text = offer
        if trip_limit and trip_limit.lower() not in offer_text.lower():
            offer_text = f"{offer_text} on {trip_limit}"
        sentence = f"{sender_name} is offering you {offer_text}"
        if auto_applied:
            sentence += ", and the promo is already applied to your account"
        sentences.append(_ensure_sentence_ending(sentence))
    elif max_savings:
        sentences.append(_ensure_sentence_ending(f"{sender_name} is promoting a savings offer"))
    elif title and title.lower() != "(no subject)":
        sentences.append(_ensure_sentence_ending(f"{sender_name} sent you a promotional update about {title}"))

    if validity:
        validity_lower = validity.lower()
        if validity_lower.startswith(("valid through", "until ")):
            sentences.append(_ensure_sentence_ending(f"The offer is {validity}"))
        elif validity_lower.startswith("through "):
            sentences.append(_ensure_sentence_ending(f"The offer runs {validity}"))
        elif validity_lower.startswith("expires"):
            sentences.append(_ensure_sentence_ending(f"It {validity}"))
        else:
            sentences.append(_ensure_sentence_ending(f"The offer timing is {validity}"))

    if max_savings:
        cap = _first_regex_match(
            max_savings,
            r"\$\d+(?:\.\d{2})?(?:\s+per\s+(?:ride|trip|order|item))?",
        )
        if cap:
            sentences.append(_ensure_sentence_ending(f"Savings are capped at {cap}"))

    if features:
        sentences.append(_ensure_sentence_ending(f"It also promotes {_natural_join(features)}"))
    elif sentences:
        sentences.append("The rest of the email is promotional copy and does not require a reply.")

    summary = _compact_text(" ".join(sentences[:3]))
    if not summary:
        return None
    if len(summary) > SUMMARY_MAX_CHARS:
        summary = f"{summary[: SUMMARY_MAX_CHARS - 3]}..."
    return summary


def _looks_summary_failure(summary_text):
    """Looks summary failure.
    """
    # Replace the earlier brittle mojibake regex with the shared detector.
    normalized = _compact_text(summary_text).lower()
    if not normalized:
        return True
    if normalized in {"n/a", "none", "unknown", "summary unavailable"}:
        return True
    if normalized.startswith("{") or normalized.startswith("["):
        return True
    if contains_common_mojibake(summary_text):
        return True
    if _is_noise_fragment(normalized):
        return True
    return any(marker in normalized for marker in SUMMARY_FAILURE_MARKERS)


def _prepare_model_summary(summary_text, char_limit):
    """Lightly clean raw model output without rewriting the model's wording."""
    summary = repair_body_text(summary_text or "", None).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not summary:
        return ""
    summary = re.sub(
        r"^\s*(?:plain[- ]text\s+)?(?:email\s+)?summary\s*[:\-]\s*",
        "",
        summary,
        flags=re.IGNORECASE,
    )
    summary = re.sub(r"\n{3,}", "\n\n", summary)
    summary = _compact_text(summary)
    if not summary:
        return ""
    limit = max(160, int(char_limit or 0))
    if len(summary) > limit:
        summary = f"{summary[: limit - 3].rstrip()}..."
    return summary


def _content_tokens(value):
    """Return content-heavy tokens for grounding checks."""
    # Shared helper for this file.
    tokens = _text_tokens(value)
    return [token for token in tokens if token not in SUMMARY_STOPWORDS]


def _summary_support_ratio(sentence_text, reference_tokens):
    """Return how strongly sentence tokens are grounded in the source."""
    # Keep hallucination filtering deterministic and inexpensive.
    tokens = _content_tokens(sentence_text)
    if not tokens:
        return 0.0
    if not reference_tokens:
        return 0.0
    supported = sum(1 for token in tokens if token in reference_tokens)
    return supported / float(len(tokens))


def _summary_candidate_fragments(summary_text, structured=False):
    """Split model output into candidate summary units while preserving bullet items."""
    raw = _normalize_summary_layout(summary_text)
    if not raw:
        return []

    line_fragments = []
    current_item = ""
    saw_list_marker = False
    for raw_line in raw.split("\n"):
        line = str(raw_line or "").strip()
        if not line:
            if current_item:
                line_fragments.append(current_item)
                current_item = ""
            continue
        line = repair_header_text(line)
        line = re.sub(
            r"^(?:key details|action for you|summary)\s*:\s*",
            "",
            line,
            flags=re.IGNORECASE,
        )
        line = re.sub(r"\s{2,}", " ", line).strip()
        if not line:
            continue
        marker_match = SUMMARY_LIST_MARKER_PATTERN.match(line)
        if marker_match:
            if current_item:
                line_fragments.append(current_item)
            current_item = line[marker_match.end() :].strip()
            saw_list_marker = True
            continue
        if saw_list_marker and current_item:
            current_item = _compact_text(f"{current_item} {line}")
            continue
        line_fragments.append(line)
    if current_item:
        line_fragments.append(current_item)

    line_fragments = [
        _compact_text(fragment).strip(" -:")
        for fragment in line_fragments
        if _compact_text(fragment).strip(" -:")
    ]
    if (structured or saw_list_marker) and len(line_fragments) >= 1:
        if not (structured and not saw_list_marker and len(line_fragments) == 1):
            return line_fragments

    normalized = _compact_text(raw)
    normalized = re.sub(r"\bkey details\s*:\s*", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\baction for you\s*:\s*", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bsummary\s*:\s*", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s{2,}", " ", normalized).strip()
    if not normalized:
        return []

    parts = _merge_sentence_fragments(re.split(r"(?<=[.!?])\s+", normalized))
    parts = [_compact_text(fragment).strip(" -:") for fragment in parts]
    parts = [fragment for fragment in parts if fragment]
    return parts or [normalized]


def _summary_sentence_is_copied(
    sentence_text,
    source_text,
    source_sentences=None,
    *,
    list_like=False,
):
    """Return True when a summary sentence is too close to source wording."""
    sentence = _compact_text(sentence_text).strip(" -:")
    if len(sentence) < 24:
        return False

    source = _compact_text(source_text)
    if not source:
        return False

    sentence_lower = sentence.lower()
    source_lower = source.lower()
    if sentence_lower in source_lower:
        return True

    reference_sentences = source_sentences or _extract_key_sentences(source, max_sentences=20)
    if not reference_sentences:
        reference_sentences = [
            _compact_text(part).strip(" -:")
            for part in re.split(r"(?<=[.!?])\s+", source)
            if _compact_text(part)
        ]

    sentence_tokens = set(_content_tokens(sentence))
    if not sentence_tokens:
        return False

    for source_sentence in reference_sentences:
        source_candidate = _compact_text(source_sentence).strip(" -:")
        if len(source_candidate) < 24:
            continue
        overlap = _token_overlap_ratio(sentence, source_candidate)
        threshold = 0.66 if list_like else 0.72
        length_window = 90 if list_like else 70
        if overlap < threshold or abs(len(sentence) - len(source_candidate)) > length_window:
            continue
        novel_tokens = sentence_tokens - set(_content_tokens(source_candidate))
        if len(novel_tokens) <= 2:
            return True
    return False


def _sanitize_model_summary(summary_text, email_data, structured=False):
    """Normalize and ground model summary text against source content."""
    # Keep only concise sentences that the source actually supports.
    profile = _summary_profile(email_data)
    parts = _summary_candidate_fragments(summary_text, structured=structured)
    if not parts:
        return None

    source = _compact_text(
        _body_for_context(
            email_data,
            max_chars=min(6000, profile["context_chars"]),
            max_sentences=min(24, profile["context_sentences"]),
        )
    )
    source_tokens = set(_content_tokens(source))
    source_sentences = _extract_key_sentences(source, max_sentences=20)

    # Break generated text into candidate sentences and keep only the grounded ones.
    kept = []
    for sentence in parts:
        if _looks_newsletter_teaser_question(sentence, email_data=email_data):
            continue
        if _looks_summary_call_to_action(sentence, email_data):
            continue
        cleaned = _strip_read_time_prefix(sentence)
        cleaned = _compact_text(_strip_footer_noise_text(cleaned))
        marketing_alert = _paraphrase_marketing_alert_teaser(cleaned)
        if marketing_alert:
            cleaned = marketing_alert
        if len(cleaned) < 22:
            continue
        lowered = cleaned.lower()
        if any(marker in lowered for marker in SUMMARY_HALLUCINATION_MARKERS):
            continue
        if _summary_uses_subject_content(cleaned, email_data):
            continue
        if _is_noise_fragment(cleaned):
            continue
        if _summary_sentence_is_copied(
            cleaned,
            source,
            source_sentences,
            list_like=structured,
        ):
            continue
        if any(_token_overlap_ratio(cleaned, existing) > 0.93 for existing in kept):
            continue
        # Require a reasonable amount of grounding in the body text or images.
        support_ratio = _summary_support_ratio(cleaned, source_tokens)
        if support_ratio < 0.52:
            continue
        kept.append(cleaned)
        if len(kept) >= profile["output_sentences"]:
            break

    if not kept:
        return None

    if structured:
        return _format_summary_list(kept, profile["char_limit"])

    normalized = " ".join(_ensure_sentence_ending(sentence) for sentence in kept)
    normalized = _compact_text(normalized)
    if len(normalized) > profile["char_limit"]:
        normalized = f"{normalized[: profile['char_limit'] - 3]}..."
    return normalized or None


def _looks_summary_call_to_action(summary_text, email_data):
    """Return True when a bulk-email summary is mainly CTA/button wording instead of gist."""
    if not _looks_bulk_or_newsletter(email_data):
        return False
    normalized = _compact_text(summary_text)
    if not normalized:
        return False
    lowered = normalized.lower()
    if not re.match(
        r"^(?:click|tap|log in|sign in|open|view|read|shop|book|follow|subscribe|answer|complete|save|start|continue)\b",
        lowered,
    ):
        return False
    title = _compact_text(email_data.get("title") or "")
    if title and _token_overlap_ratio(normalized, title) >= 0.5:
        return False
    return True


def _looks_summary_parrot(summary_text, email_data):
    """Return True when summary appears copied from the email body."""
    # Keep model summaries readable by rejecting near-verbatim echoes of the source.
    profile = _summary_profile(email_data)
    normalized_summary_text = _normalize_summary_layout(summary_text)
    summary = _compact_text(normalized_summary_text)
    if len(summary) < 24:
        return False
    list_like = "\n" in normalized_summary_text or bool(
        SUMMARY_LIST_MARKER_PATTERN.match(normalized_summary_text.lstrip())
    )

    source = _compact_text(
        _body_for_context(
            email_data,
            max_chars=min(6000, profile["context_chars"]),
            max_sentences=min(24, profile["context_sentences"]),
        )
    )
    if not source:
        return False

    summary_lower = summary.lower()
    source_lower = source.lower()
    if len(summary) >= 80 and summary_lower in source_lower:
        return True

    summary_sentences = [
        _compact_text(part).strip(" -:")
        for part in re.split(r"(?<=[.!?])\s+", summary)
    ]
    summary_sentences = [part for part in summary_sentences if len(part) >= 24]
    if not summary_sentences:
        summary_sentences = [summary]

    source_sentences = _extract_key_sentences(source, max_sentences=20)
    if not source_sentences:
        source_sentences = [
            _compact_text(part).strip(" -:")
            for part in re.split(r"(?<=[.!?])\s+", source)
            if _compact_text(part)
        ]

    return any(
        _summary_sentence_is_copied(
            sentence,
            source,
            source_sentences,
            list_like=list_like,
        )
        for sentence in summary_sentences
    )


def _looks_digest_title_summary(summary_text):
    """Return True for short newsletter summaries that intentionally name featured items."""
    normalized = _compact_text(summary_text).lower()
    if not normalized:
        return False
    if len(normalized) > 240 or " it features " not in normalized:
        return False
    return any(
        marker in normalized
        for marker in ("newsletter", "news digest", "question digest")
    )


def _looks_bulk_summary_boilerplate_heavy(summary_text, email_data):
    """Return True when a bulk-email summary is dominated by CTA or promo boilerplate."""
    if not _looks_bulk_or_newsletter(email_data):
        return False
    normalized = _compact_text(summary_text).lower()
    if not normalized:
        return False
    cta_markers = (
        "order now",
        "shop now",
        "view flyer",
        "load your offers",
        "load this offer",
        "read transcript",
        "watch now",
        "explore more",
        "play now",
        "upgrade to premium",
        "premium subscribers",
        "membership",
        "$0 delivery fee",
        "terms and conditions apply",
    )
    hits = sum(marker in normalized for marker in cta_markers)
    uppercase_tokens = len(re.findall(r"\b[A-Z0-9]{3,}\b", str(summary_text or "")))
    return hits >= 2 or (hits >= 1 and uppercase_tokens >= 4) or (hits >= 1 and len(normalized) > 260)


def _usable_summary_candidate(summary_text, email_data):
    """Return normalized summary text only when it passes basic quality checks."""
    summary = _finalize_summary_text(summary_text, email_data)
    if not summary:
        return None
    digest_like_summary = bool(
        re.match(
            r"^(?:a|an)\s+(?:newsletter|news digest|question digest|article alert|promotional update|job alert|activity update|reminder)\s+from\b",
            summary,
            flags=re.IGNORECASE,
        )
        or re.match(
            r"^[a-z0-9 .&'_-]+\s+sent\s+(?:a|an)\s+(?:newsletter|news digest|question digest|article alert|reminder)\b",
            summary,
            flags=re.IGNORECASE,
        )
    )
    if _looks_summary_failure(summary):
        return None
    if _looks_summary_call_to_action(summary, email_data):
        return None
    if _looks_bulk_summary_boilerplate_heavy(summary, email_data):
        return None
    if _summary_uses_subject_content(summary, email_data):
        sparse_promo = (
            summary.lower().startswith("a promotional update from ")
            and len(_clean_body_for_prompt(email_data, max_chars=240)) < 120
        )
        if not sparse_promo:
            return None
    if _looks_summary_parrot(summary, email_data):
        if _looks_digest_title_summary(summary):
            return summary
        if summary.lower().startswith("a promotional update from "):
            return summary
        if digest_like_summary and not _summary_uses_subject_content(summary, email_data):
            return summary
        return None
    return summary


def _looks_generic_posture_summary(summary_text):
    """Return True for low-value fallback summaries that only describe email posture."""
    normalized = _compact_text(summary_text).lower()
    if not normalized:
        return True
    generic_phrases = (
        "it appears to be an informational message",
        "it looks like an informational update or newsletter",
        "no direct reply appears to be required",
        "no explicit action request is obvious from the content",
        "it appears to include follow-up instructions or a requested next step",
    )
    return any(phrase in normalized for phrase in generic_phrases)


def _summary_evidence_block(email_data, max_points=8):
    """Return compact paraphrased evidence for summary generation."""
    sender = _normalized_header_text(email_data.get("sender") or "")
    actionable = _looks_actionable(email_data)
    deadline = _extract_deadline_phrase(_reply_plan_source_text(email_data, max_chars=1800))
    evidence_points = _select_fallback_summary_sentences(
        email_data,
        max_sentences=max(3, min(max_points, 8)),
    )
    if not evidence_points:
        evidence_points = _dedupe_text_items(
            (
                _rewrite_fallback_summary_sentence(sentence, email_data)
                for sentence in _extract_key_sentences(email_data, max_sentences=max_points)
            ),
            max_items=max(3, min(max_points, 8)),
            max_chars=220,
        )
    lines = [
        f"From: {sender}",
        f"Actionable: {'yes' if actionable else 'no'}",
        "Use body evidence only; do not quote or summarize the subject line.",
    ]
    if deadline:
        lines.append(f"Deadline: {deadline}")
    lines.append("Key evidence:")
    if evidence_points:
        lines.extend(f"- {point}" for point in evidence_points)
    else:
        lines.append("- (none)")
    return "\n".join(lines)


def _rewrite_parroted_summary(summary_text, email_data, email_id=None, structured=False):
    """Attempt to rewrite a copied summary into original condensed wording."""
    # Give the model one more chance to paraphrase before we fall back.
    profile = _summary_profile(email_data)
    candidate = _normalize_summary_layout(summary_text)
    if not candidate:
        return None

    system_prompt = (
        "You rewrite copied email summaries into original condensed wording. "
        "Use neutral phrasing and never address the mailbox owner as you/your. "
        "Use only explicit facts present in the email context. "
        "Do not copy or quote source sentences. "
        "No sentence in your rewrite may closely match a sentence from the email. "
        "Do not repeat article headlines, teaser bullets, or newsletter list items verbatim. "
        "Do not add subscription/sign-up suggestions unless explicitly requested in the email. "
        "Use declarative statements only (no questions). "
        + "Return one compact plain-text paragraph. "
        + f"Use {profile['prompt_target']} when the source email is long."
    )
    user_prompt = (
        "Rewrite this summary in your own words so it is not copied from the email.\n\n"
        f"Current summary:\n{candidate}\n\n"
        f"{_summary_evidence_block(email_data, max_points=min(8, profile['output_sentences']))}"
    )
    response_text = _call_ollama(
        task="summarize_rewrite",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=_num_predict_for_task(
            "summarize_rewrite",
            max(280, min(800, profile["num_predict"])),
        ),
    )
    if not response_text:
        return None

    rewritten = _rewrite_summary_for_second_person(
        _sanitize_model_summary(response_text, email_data, structured=structured) or ""
    )
    rewritten = _finalize_summary_text(rewritten, email_data)
    if _looks_summary_failure(rewritten):
        return None
    if _summary_uses_subject_content(rewritten, email_data):
        return None
    if _looks_summary_parrot(rewritten, email_data):
        return None
    if len(rewritten) > profile["char_limit"]:
        rewritten = f"{rewritten[: profile['char_limit'] - 3]}..."
    return rewritten or None


def _looks_summary_closing_sentence(sentence_text):
    """Return True when sentence is mainly a sign-off or closing pleasantry."""
    normalized = _compact_text(sentence_text).lower().strip(" .,!;:")
    if not normalized:
        return True
    if normalized in {"best", "regards", "sincerely", "cheers", "thanks", "thank you"}:
        return True
    return any(normalized.startswith(marker) for marker in SUMMARY_CLOSING_MARKERS)


def _looks_low_information_sentence(sentence_text):
    """Return True for greetings or pleasantries that add little summary value."""
    normalized = _compact_text(sentence_text).lower()
    if not normalized:
        return True
    if any(marker in normalized for marker in SUMMARY_PLEASANTRY_MARKERS):
        return True
    if normalized.startswith("welcome to ") and any(
        marker in normalized for marker in ("roundup", "digest", "newsletter", "briefing")
    ):
        return True
    if normalized.startswith(("hi ", "hello ", "dear ")) and len(_text_tokens(normalized)) <= 10:
        return True
    return _looks_summary_closing_sentence(normalized)


def _has_temporal_summary_hint(sentence_text):
    """Return True when sentence appears to include timing or deadline detail."""
    normalized = _compact_text(sentence_text).lower()
    if not normalized:
        return False
    if re.search(
        r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|today|tomorrow|"
        r"tonight|this morning|this afternoon|next week|next month|next quarter|"
        r"deadline|noon|midnight|morning|afternoon|evening|eod|eom)\b",
        normalized,
    ):
        return True
    if re.search(
        r"\b(?:by|before|after|until)\s+(?:\d{1,2}(?::\d{2})?\s*(?:am|pm)?|"
        r"noon|midnight|end of day|eod|monday|tuesday|wednesday|thursday|friday|"
        r"saturday|sunday)\b",
        normalized,
    ):
        return True
    return bool(
        re.search(
            r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|"
            r"dec(?:ember)?)\b",
            normalized,
        )
    )


def _force_summary_sentence_paraphrase(sentence_text, email_data):
    """Force a fallback summary sentence away from source wording when needed."""
    text = _compact_text(sentence_text).strip(" -:")
    if not text:
        return ""
    if _looks_bulk_or_newsletter(email_data):
        topic = _topic_phrase_from_sentence(text)
        if topic:
            return _ensure_sentence_ending(topic)
    if re.match(
        r"^(?:you should|make sure|the sender asks|the email reminds you|it includes|"
        r"it highlights|it covers|the email notes that|the sender notes that)\b",
        text,
        flags=re.IGNORECASE,
    ):
        return _ensure_sentence_ending(text)

    prefix = "The sender notes that"
    if not _looks_actionable(email_data):
        prefix = "The email notes that"
    text = text[0].lower() + text[1:] if len(text) > 1 and text[0].isupper() else text
    return _ensure_sentence_ending(f"{prefix} {text}")


def _rewrite_fallback_summary_sentence(sentence_text, email_data):
    """Rewrite an extracted body sentence into concise summary wording."""
    text = _strip_read_time_prefix(sentence_text)
    text = _compact_text(text).strip(" -:")
    if not text:
        return ""
    if _looks_newsletter_teaser_question(text, email_data=email_data):
        return ""
    if re.match(r"^(?:click here|click to|or,\s*see\b)", text, flags=re.IGNORECASE):
        return ""
    text = re.sub(r"\bwith this prompt\b", "", text, flags=re.IGNORECASE)
    text = _compact_text(text).strip(" -:")

    question_replacements = (
        (
            r"^(?:can|could|would|will) you (.+)\?$",
            lambda match: f"The sender asks whether you can {match.group(1)}",
        ),
        (
            r"^are you (.+)\?$",
            lambda match: f"The sender asks whether you are {match.group(1)}",
        ),
        (
            r"^are you able to (.+)\?$",
            lambda match: f"The sender asks whether you are able to {match.group(1)}",
        ),
        (
            r"^have you (.+)\?$",
            lambda match: f"The sender asks whether you have {match.group(1)}",
        ),
        (
            r"^did you (.+)\?$",
            lambda match: f"The sender asks whether you {match.group(1)}",
        ),
        (
            r"^do you have (.+)\?$",
            lambda match: f"The sender asks whether you have {match.group(1)}",
        ),
        (
            r"^continue (.+)$",
            lambda match: f"The email reminds you to continue {match.group(1)}",
        ),
        (
            r"^(?:answer|complete)\s+a\s+((?:\d+|one)[-\s]?question)\s+survey\.?$",
            lambda match: f"It includes a {match.group(1)} survey",
        ),
    )
    for pattern, replacer in question_replacements:
        rewritten_question = re.sub(pattern, replacer, text, flags=re.IGNORECASE)
        if rewritten_question != text:
            text = rewritten_question
            break

    text = re.sub(
        r"^(?:hi|hello|dear)\b[^.!?]{0,60}[,.!]\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"^(?:hi|hello|dear)\b(?:\s+(?!(?:i|we|just|please|can|could|would|thank|"
        r"thanks|following)\b)[a-z0-9.'&-]+){0,8}\s+"
        r"(?=(?:i|we|just|please|can|could|would|thank|thanks|following)\b)",
        "",
        text,
        flags=re.IGNORECASE,
    )
    if re.match(r"^(?:hi|hello|dear)\b", text, flags=re.IGNORECASE):
        lowered = text.lower()
        for marker in (
            " i wanted to ",
            " we wanted to ",
            " just wanted to ",
            " please ",
            " can you ",
            " could you ",
            " would you ",
            " thank you ",
            " thanks ",
            " following up ",
            " i need ",
            " we need ",
        ):
            marker_index = lowered.find(marker)
            if marker_index > 0:
                text = text[marker_index + 1 :]
                break
    text = _compact_text(text).strip(" -:")
    if not text or _looks_low_information_sentence(text):
        return ""
    marketing_alert = _paraphrase_marketing_alert_teaser(text)
    if marketing_alert:
        return _ensure_sentence_ending(marketing_alert)

    sender_name = _sender_display_name(email_data.get("sender")) or "The sender"

    replacements = (
        (
            r"^i wanted to send (?:a )?(?:quick )?(?:note|update)(?: before [^,]+)? "
            r"to thank (?:everyone|the team|all of you) for (.+)$",
            lambda match: f"{sender_name} thanks the team for {match.group(1)}",
        ),
        (
            r"^i wanted to thank (?:everyone|the team|all of you) for (.+)$",
            lambda match: f"{sender_name} thanks the team for {match.group(1)}",
        ),
        (
            r"^i appreciate (.+)$",
            lambda match: f"{sender_name} appreciates {match.group(1)}",
        ),
        (
            r"^going into ([^,]+), i need everyone to (.+)$",
            lambda match: f"For {match.group(1)}, everyone should {match.group(2)}",
        ),
        (
            r"^going into ([^,]+), i need you to (.+)$",
            lambda match: f"For {match.group(1)}, you should {match.group(2)}",
        ),
        (
            r"^i need everyone to (.+)$",
            lambda match: f"Everyone should {match.group(1)}",
        ),
        (
            r"^i need you to (.+)$",
            lambda match: f"You should {match.group(1)}",
        ),
        (
            r"^please make sure (.+)$",
            lambda match: f"Make sure {match.group(1)}",
        ),
        (
            r"^if you are running into (.+?), let me know (.+)$",
            lambda match: f"If you run into {match.group(1)}, let the sender know {match.group(2)}",
        ),
        (
            r"^if you run into (.+?), let me know (.+)$",
            lambda match: f"If you run into {match.group(1)}, let the sender know {match.group(2)}",
        ),
        (
            r"^i am expecting (.+)$",
            lambda match: f"{sender_name} expects {match.group(1)}",
        ),
        (
            r"^let's keep (.+)$",
            lambda match: f"Keep {match.group(1)}",
        ),
    )
    for pattern, replacer in replacements:
        updated = re.sub(pattern, replacer, text, flags=re.IGNORECASE)
        if updated != text:
            text = updated
            break

    generic_replacements = (
        (r"^i(?:'m| am) writing to ", ""),
        (r"^i wanted to ", ""),
        (r"^just wanted to ", ""),
    )
    for pattern, replacement in generic_replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    text = re.sub(r"\blet me know\b", "let the sender know", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\bthe work that has gone into\b",
        "the work on",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bso we can\b", "so the team can", text, flags=re.IGNORECASE)
    text = _compact_text(text).strip(" -:")
    if not text or _looks_low_information_sentence(text):
        return ""
    if text:
        text = text[0].upper() + text[1:]
    source_context = _body_for_context(email_data, max_chars=1800, max_sentences=12)
    if _summary_sentence_is_copied(text, source_context, [sentence_text]):
        text = _force_summary_sentence_paraphrase(text, email_data)
    return _ensure_sentence_ending(text)


def _summary_sentence_score(sentence_text, position):
    """Score extracted sentences so the fallback keeps concrete details first."""
    normalized = _compact_text(sentence_text)
    if not normalized:
        return -1
    lowered = normalized.lower()
    if _looks_low_information_sentence(lowered):
        return -1

    score = max(0, 6 - position)
    if any(marker in lowered for marker in SUMMARY_TASK_MARKERS):
        score += 6
    if _has_temporal_summary_hint(lowered):
        score += 6
    if any(marker in lowered for marker in SUMMARY_OVERVIEW_MARKERS):
        score += 2
    if re.search(
        r"\b(?:priority|blocker|delay|deliverable|meeting|budget|invoice|proposal|"
        r"schedule|timeline|review|approval|update|follow-up)\b",
        lowered,
    ):
        score += 2
    return score


def _select_fallback_summary_sentences(email_data, max_sentences=3):
    """Select concrete body sentences for the summary fallback."""
    title = _compact_text(email_data.get("title") or "")
    key_sentences = _extract_key_sentences(email_data, max_sentences=10)
    if not key_sentences:
        return []
    newsletter_like = _looks_bulk_or_newsletter(email_data)
    actionable = _looks_actionable(email_data)

    candidates = []
    overview = None
    for position, sentence in enumerate(key_sentences):
        rewritten = _rewrite_fallback_summary_sentence(sentence, email_data)
        if not rewritten or _is_noise_fragment(rewritten):
            continue
        score = _summary_sentence_score(sentence, position)
        candidates.append((score, position, rewritten))
        lowered = _compact_text(sentence).lower()
        if overview is None and position < 4:
            if any(marker in lowered for marker in ("thank", "appreciate")):
                overview = (position, rewritten)
            elif not any(marker in lowered for marker in SUMMARY_TASK_MARKERS):
                overview = (position, rewritten)

    if not candidates:
        return []

    selected_items = []
    if overview:
        selected_items.append(overview)

    ranked_candidates = sorted(candidates, key=lambda item: (-item[0], item[1]))
    if len(candidates) >= 5 and max_sentences >= 4:
        # Long newsletters need coverage across sections, not just the globally highest-scoring bits.
        segment_count = min(max_sentences, len(candidates))
        segment_size = max(1, (len(candidates) + segment_count - 1) // segment_count)
        segmented = []
        for start in range(0, len(candidates), segment_size):
            segment = candidates[start : start + segment_size]
            if not segment:
                continue
            segmented.append(sorted(segment, key=lambda item: (-item[0], item[1]))[0])
        ranked_candidates = segmented + [
            candidate for candidate in ranked_candidates if candidate not in segmented
        ]

    for _, position, rewritten in ranked_candidates:
        if any(
            _token_overlap_ratio(rewritten, existing_text) > 0.9
            for _, existing_text in selected_items
        ):
            continue
        if _is_near_subject_copy(rewritten, title) and selected_items:
            continue
        selected_items.append((position, rewritten))
        if len(selected_items) >= max_sentences:
            break

    if newsletter_like and not actionable:
        selected_items = sorted(selected_items, key=lambda item: item[0])
    return [rewritten for _, rewritten in selected_items[:max_sentences]]


def _merge_summary_with_fallback_coverage(summary_text, email_data):
    """Supplement long model summaries with missing fallback details when space allows."""
    profile = _summary_profile(email_data)
    if "\n" in str(summary_text or ""):
        return _compact_text(str(summary_text or "").strip())
    summary = _compact_text(summary_text)
    if not summary or profile["output_sentences"] < 5:
        return summary

    summary_sentences = [
        _compact_text(part).strip(" -:")
        for part in re.split(r"(?<=[.!?])\s+", summary)
        if _compact_text(part)
    ]
    if not summary_sentences:
        summary_sentences = [summary]

    fallback_sentences = _select_fallback_summary_sentences(
        email_data,
        max_sentences=profile["output_sentences"],
    )
    if not fallback_sentences:
        return summary

    merged_sentences = list(summary_sentences)
    source_context = _body_for_context(
        email_data,
        max_chars=min(6000, profile["context_chars"]),
        max_sentences=min(24, profile["context_sentences"]),
    )
    source_sentences = _extract_key_sentences(source_context, max_sentences=20)
    for sentence in reversed(fallback_sentences):
        if any(_token_overlap_ratio(sentence, existing) > 0.6 for existing in merged_sentences):
            continue
        if _summary_sentence_is_copied(sentence, source_context, source_sentences):
            continue
        candidate = _compact_text(
            " ".join(_ensure_sentence_ending(item) for item in merged_sentences + [sentence])
        )
        if len(candidate) > profile["char_limit"]:
            break
        merged_sentences.append(sentence)
        if len(merged_sentences) >= profile["output_sentences"]:
            break

    merged_summary = _compact_text(" ".join(_ensure_sentence_ending(item) for item in merged_sentences))
    if _looks_summary_parrot(merged_summary, email_data):
        return summary
    return merged_summary


def _prefer_richer_promotional_fallback(summary_text, fallback_summary):
    """Prefer a richer promo fallback when the model only returns a thin lead sentence."""
    summary = _compact_text(summary_text)
    fallback = _compact_text(fallback_summary)
    if not summary or not fallback or summary == fallback:
        return summary or fallback
    if "\n" in summary:
        return summary
    if not fallback.lower().startswith("a promotional update from "):
        return summary

    summary_sentences = _summary_candidate_fragments(summary)
    if len(summary_sentences) > 1 and len(summary) >= 220:
        return summary

    ignored_tokens = {"advertises", "message", "promotes", "promotional", "update"}
    summary_tokens = {
        token for token in _content_tokens(summary)
        if token not in ignored_tokens
    }
    fallback_tokens = {
        token for token in _content_tokens(fallback)
        if token not in ignored_tokens
    }
    extra_tokens = fallback_tokens - summary_tokens
    if len(summary_tokens) > 12:
        return summary
    if len(fallback) <= len(summary) + 35:
        return summary
    if len(fallback_tokens) <= len(summary_tokens) + 2:
        return summary
    if len(extra_tokens) < 5:
        return summary
    return fallback


def _extractive_summary_fallback(email_data):
    """Contextual summary fallback.
    """
    bulk_summary = _bulk_newsletter_summary(email_data)
    if bulk_summary:
        usable_bulk_summary = _usable_summary_candidate(bulk_summary, email_data)
        if usable_bulk_summary:
            return usable_bulk_summary
    article_summary = _single_article_alert_summary(email_data)
    if article_summary:
        return article_summary
    staff_summary = _staff_update_summary(email_data)
    if staff_summary:
        return staff_summary
    # Prefer concrete details pulled from the body over generic prose.
    profile = _summary_profile(email_data)
    title = _compact_text(email_data.get("title") or "")
    actionable = _looks_actionable(email_data)
    newsletter_like = _looks_bulk_or_newsletter(email_data)
    extracted_sentences = _select_fallback_summary_sentences(
        email_data,
        max_sentences=profile["output_sentences"],
    )
    if extracted_sentences:
        extracted_sentences = [
            _compact_text(_strip_title_prefix(sentence, title))
            for sentence in extracted_sentences
            if _compact_text(_strip_title_prefix(sentence, title))
        ]
        if not newsletter_like:
            summary = _compact_text(" ".join(extracted_sentences))
            if not _is_noise_fragment(summary):
                if len(summary) > profile["char_limit"]:
                    summary = f"{summary[: profile['char_limit'] - 3]}..."
                normalized_summary = _finalize_summary_text(summary, email_data)
                if normalized_summary and not _looks_bulk_summary_boilerplate_heavy(
                    normalized_summary,
                    email_data,
                ) and not _looks_summary_parrot(normalized_summary, email_data):
                    return normalized_summary
        topic_fragments = _dedupe_text_items(
            (_topic_phrase_from_sentence(sentence) for sentence in extracted_sentences),
            max_items=min(3, profile["output_sentences"]),
            max_chars=140,
        )
        if topic_fragments:
            if newsletter_like:
                topic_summary = _ensure_sentence_ending(
                    f"It covers {_natural_join(topic_fragments)}"
                )
            elif not actionable:
                topic_summary = _ensure_sentence_ending(
                    f"It mentions {_natural_join(topic_fragments)}"
                )
            else:
                topic_summary = _ensure_sentence_ending(
                    f"It notes {_natural_join(topic_fragments)}"
                )
            if not _summary_uses_subject_content(topic_summary, email_data):
                if len(topic_summary) > profile["char_limit"]:
                    topic_summary = f"{topic_summary[: profile['char_limit'] - 3]}..."
                return topic_summary

    sender_name = _sender_display_name(email_data.get("sender")) or "the sender"
    intro = f"This email is from {sender_name}."

    if actionable:
        posture = "It appears to include follow-up instructions or a requested next step."
        action = "Review the message for the requested action and timing."
    elif newsletter_like:
        posture = "It looks like an informational update or newsletter."
        action = "No direct reply appears to be required."
    else:
        posture = "It appears to be an informational message."
        action = "No explicit action request is obvious from the content."

    summary = _compact_text(f"{intro} {posture} {action}")
    if _is_noise_fragment(summary):
        return None
    if len(summary) > profile["char_limit"]:
        summary = f"{summary[: profile['char_limit'] - 3]}..."
    return summary or None


def _structured_summary_fallback(email_data):
    """Return a paragraph fallback for long multi-section emails."""
    profile = _summary_profile(email_data)
    items = _select_fallback_summary_sentences(
        email_data,
        max_sentences=min(6, profile["output_sentences"]),
    )
    if len(items) < 2:
        return None
    if _looks_bulk_or_newsletter(email_data):
        topic_fragments = _dedupe_text_items(
            (_topic_phrase_from_sentence(item) for item in items),
            max_items=min(3, profile["output_sentences"]),
            max_chars=140,
        )
        if topic_fragments:
            return _ensure_sentence_ending(f"It covers {_natural_join(topic_fragments)}")
    summary = _compact_text(" ".join(_ensure_sentence_ending(item) for item in items))
    if len(summary) > profile["char_limit"]:
        summary = f"{summary[: profile['char_limit'] - 3]}..."
    return summary or None


def _rewrite_summary_for_second_person(summary_text):
    """Normalize generated summaries to neutral paragraph-style wording."""
    # Keep the summary style predictable and avoid second-person phrasing in the UI.
    summary = _normalize_summary_layout(summary_text)
    if not summary:
        return ""
    replacements = (
        (r"\byou(?:'re| are)\b", "the recipient is"),
        (r"\byou(?:'ve| have)\b", "the recipient has"),
        (r"\byou'd\b", "the recipient had"),
        (r"\byou'll\b", "the recipient will"),
        (r"\bthe user\b", "the recipient"),
        (r"\bthis user\b", "the recipient"),
        (r"\buser's\b", "the recipient's"),
        (r"\bthe mailbox owner\b", "the recipient"),
        (r"\bmailbox owner\b", "the recipient"),
        (r"\brecipient's\b", "the recipient's"),
        (r"\bthe email recipient\b", "the recipient"),
        (r"\byou received an email from\b", "The email is from"),
        (r"\byou received a reminder from\b", "A reminder from"),
        (r"\byou received a newsletter from\b", "A newsletter from"),
        (r"\byou received a news digest from\b", "A news digest from"),
        (r"\byou received a question digest from\b", "A question digest from"),
        (r"\byou received an article alert from\b", "An article alert from"),
        (r"\bit includes a prompt asking you to\b", "The email includes a prompt to"),
        (r"\byou need to\b", "The email asks for"),
        (r"\byou should\b", "The email recommends"),
        (r"\byour\b", "the recipient's"),
        (r"\byou\b", "the recipient"),
    )
    normalized_lines = []
    for raw_line in summary.split("\n"):
        line = str(raw_line or "")
        if not line.strip():
            continue
        stripped = line.strip()
        if SUMMARY_LIST_MARKER_PATTERN.match(stripped):
            stripped = SUMMARY_LIST_MARKER_PATTERN.sub("", stripped, count=1)
        for pattern, replacement in replacements:
            stripped = re.sub(pattern, replacement, stripped, flags=re.IGNORECASE)
        stripped = _normalized_header_text(stripped)
        if not stripped:
            continue
        normalized_lines.append(_ensure_sentence_ending(stripped))
    rewritten = _compact_text(" ".join(normalized_lines))
    rewritten = re.sub(r"\b[Tt]he email asks for whether\b", "The email asks whether", rewritten)
    rewritten = re.sub(r"\b[Tt]he email recommends watch\b", "The email recommends watching", rewritten)
    rewritten = re.sub(r"\b[Tt]he recipient received\b", "The email", rewritten)
    return rewritten


def _summary_source_intro(email_data):
    """Return a short source-oriented intro for summaries that need context."""
    sender_name = _summary_sender_name(email_data)
    if not sender_name or sender_name == "The sender":
        return ""

    raw_body = _email_body_text(email_data)
    key_sentences = _extract_key_sentences(email_data, max_sentences=4)
    combined = _compact_text(
        " ".join(
            part
            for part in [
                _normalized_header_text(email_data.get("title") or ""),
                _clean_body_for_prompt(email_data, max_chars=2400),
            ]
            if part
        )
    ).lower()
    junk_assessment = _junk_signal_assessment(email_data)
    if (
        junk_assessment.get("commercial_promotion")
        and not junk_assessment.get("editorial_like")
        and not junk_assessment.get("transactional_like")
    ):
        return f"A promotional update from {sender_name}"
    if _extract_digest_questions(raw_body, max_questions=1):
        return f"A question digest from {sender_name}"
    if _job_alert_summary(email_data):
        return f"A job alert from {sender_name}"
    if _activity_notification_summary(email_data):
        return f"An activity update from {sender_name}"
    if _prompt_reminder_summary(email_data):
        return f"A reminder from {sender_name}"
    if _looks_single_article_alert(email_data):
        return f"An article alert from {sender_name}"
    if _looks_multi_item_digest(raw_body, key_sentences) or any(
        marker in combined for marker in ("top stories", "news digest", "briefing", "latest news")
    ):
        return f"A news digest from {sender_name}"
    if any(
        marker in combined for marker in ("newsletter", "weekly curated", "read transcript", "watch now", "roundup")
    ):
        return f"A newsletter from {sender_name}"
    return f"A promotional update from {sender_name}"


def _summary_sentence_kind(sentence_text):
    """Return a coarse summary type so generic scaffolds can be rewritten centrally."""
    lowered = _compact_text(sentence_text).lower()
    if not lowered:
        return "generic"
    checks = (
        ("promotional", r"^(?:a|an)\s+promotional update from\b"),
        ("news_digest", r"^(?:a|an)\s+news digest from\b"),
        ("question_digest", r"^(?:a|an)\s+question digest from\b"),
        ("newsletter", r"^(?:a|an)\s+newsletter from\b"),
        ("article_alert", r"^(?:a|an)\s+article alert from\b"),
        ("job_alert", r"^(?:a|an)\s+job alert from\b"),
        ("activity_update", r"^(?:a|an)\s+activity update from\b"),
        ("reminder", r"^(?:a|an)\s+reminder from\b"),
    )
    for kind, pattern in checks:
        if re.match(pattern, lowered):
            return kind
    if re.match(r"^[a-z0-9 .&'_-]+\s+sent\s+a\s+news digest\b", lowered):
        return "news_digest"
    if re.match(r"^[a-z0-9 .&'_-]+\s+sent\s+a\s+question digest\b", lowered):
        return "question_digest"
    if re.match(r"^[a-z0-9 .&'_-]+\s+sent\s+a\s+newsletter\b", lowered):
        return "newsletter"
    if re.match(r"^[a-z0-9 .&'_-]+\s+sent\s+a\s+reminder\b", lowered):
        return "reminder"
    return "generic"


def _summary_sentence_text(sentence_text):
    """Normalize a sentence fragment into sentence case with trailing punctuation."""
    text = _compact_text(sentence_text).strip(" -:;,.")
    if not text:
        return ""
    if text[0].islower():
        text = text[0].upper() + text[1:]
    return _ensure_sentence_ending(text)


def _prompt_sentence_rewrite(detail_text):
    """Rewrite prompt-style detail lines into neutral phrasing."""
    detail = _compact_text(detail_text).strip(" -:;,.")
    if not detail:
        return ""
    if re.match(
        r"^(?:describe|share|write|tell|record|reflect|remember|explain|list|talk about|consider|recount)\b",
        detail,
        flags=re.IGNORECASE,
    ):
        return f"The prompt asks the recipient to {detail}"
    return f"The prompt focuses on {detail}"


def _naturalize_summary_scaffolding(summary_text):
    """Rewrite repetitive summary scaffolds into more natural shared phrasing."""
    summary = _compact_text(summary_text)
    if not summary:
        return ""

    sentences = [
        _compact_text(part).strip(" -:;,.")
        for part in re.split(r"(?<=[.!?])\s+", summary)
        if _compact_text(part)
    ]
    if not sentences:
        return summary

    intro_patterns = (
        (r"^(.+?)\s+sent\s+a\s+news digest$", "news_digest", lambda sender: f"A news digest from {sender}"),
        (r"^(.+?)\s+sent\s+a\s+question digest$", "question_digest", lambda sender: f"A question digest from {sender}"),
        (r"^(.+?)\s+sent\s+a\s+newsletter$", "newsletter", lambda sender: f"A newsletter from {sender}"),
        (r"^(.+?)\s+sent\s+a\s+reminder$", "reminder", lambda sender: f"A reminder from {sender}"),
    )
    summary_kind = _summary_sentence_kind(sentences[0])
    for pattern, kind, builder in intro_patterns:
        match = re.match(pattern, sentences[0], flags=re.IGNORECASE)
        if not match:
            continue
        sentences[0] = builder(_compact_text(match.group(1)))
        summary_kind = kind
        break

    rewritten = []
    for index, raw_sentence in enumerate(sentences):
        sentence = re.sub(r"^It also\b", "It", raw_sentence, flags=re.IGNORECASE)
        if index == 0:
            rewritten.append(_summary_sentence_text(sentence))
            continue

        match = re.match(r"^(?:The email|The sender)\s+notes that\s+(.+)$", sentence, flags=re.IGNORECASE)
        if match:
            rewritten.append(_summary_sentence_text(match.group(1)))
            continue

        sentence = re.sub(r"^The email reminds\b", "It reminds", sentence, flags=re.IGNORECASE)
        sentence = re.sub(r"^The email asks for\b", "It asks for", sentence, flags=re.IGNORECASE)
        sentence = re.sub(r"^The email asks whether\b", "It asks whether", sentence, flags=re.IGNORECASE)

        if summary_kind == "news_digest":
            for pattern, replacement in (
                (r"^It covers (.+)$", r"Topics include \1"),
                (r"^It mentions (.+)$", r"Topics include \1"),
                (r"^It includes (.+)$", r"Topics include \1"),
            ):
                sentence = re.sub(pattern, replacement, sentence, flags=re.IGNORECASE)
        elif summary_kind == "question_digest":
            for pattern, replacement in (
                (r"^It features questions? about (.+)$", r"Featured questions include \1"),
                (r"^It covers (.+)$", r"Featured questions include \1"),
                (r"^It mentions (.+)$", r"Featured questions include \1"),
            ):
                sentence = re.sub(pattern, replacement, sentence, flags=re.IGNORECASE)
        elif summary_kind == "newsletter":
            for pattern, replacement in (
                (r"^It covers (.+)$", r"Featured items include \1"),
                (r"^It mentions (.+)$", r"Featured items include \1"),
                (r"^It includes (.+)$", r"Featured items include \1"),
            ):
                sentence = re.sub(pattern, replacement, sentence, flags=re.IGNORECASE)
        elif summary_kind == "job_alert":
            for pattern, replacement in (
                (r"^It mentions (.+)$", r"It lists \1"),
                (r"^It covers jobs for (.+)$", r"It lists jobs for \1"),
                (r"^It includes (.+)$", r"The first listing is \1"),
            ):
                sentence = re.sub(pattern, replacement, sentence, flags=re.IGNORECASE)
        elif summary_kind == "activity_update":
            sentence = re.sub(r"^It says there (?:is|are)\s+(.+)$", r"It reports \1", sentence, flags=re.IGNORECASE)
            sentence = re.sub(
                r"^It mentions recent likes, comments, or follows$",
                "It reports recent likes, comments, or follows",
                sentence,
                flags=re.IGNORECASE,
            )
        elif summary_kind == "reminder":
            prompt_match = re.match(
                r"^(?:The email|It)\s+includes a prompt to\s+(.+)$",
                sentence,
                flags=re.IGNORECASE,
            )
            if prompt_match:
                sentence = _prompt_sentence_rewrite(prompt_match.group(1))
            else:
                prompt_match = re.match(
                    r"^(?:The email|It)\s+includes a prompt about\s+(.+)$",
                    sentence,
                    flags=re.IGNORECASE,
                )
                if prompt_match:
                    sentence = _prompt_sentence_rewrite(prompt_match.group(1))
            sentence = re.sub(
                r"^It mentions a (.+survey)$",
                r"It references a \1",
                sentence,
                flags=re.IGNORECASE,
            )
            sentence = re.sub(
                r"^It includes a login link to save the response$",
                "It provides a login link to save the response",
                sentence,
                flags=re.IGNORECASE,
            )
        elif summary_kind == "promotional":
            for pattern, replacement in (
                (r"^It mentions (.+)$", r"Key details include \1"),
                (r"^It includes (.+)$", r"Key details include \1"),
            ):
                sentence = re.sub(pattern, replacement, sentence, flags=re.IGNORECASE)
        else:
            for pattern, replacement in (
                (r"^It covers (.+)$", r"The message discusses \1"),
                (r"^It mentions (.+)$", r"Key details include \1"),
                (r"^It includes (.+)$", r"Key details include \1"),
                (r"^It notes (.+)$", r"Key details include \1"),
            ):
                sentence = re.sub(pattern, replacement, sentence, flags=re.IGNORECASE)

        rewritten.append(_summary_sentence_text(sentence))

    return _compact_text(" ".join(rewritten))


def _add_summary_source_intro(summary_text, email_data):
    """Attach a source intro when a summary would otherwise start too generically."""
    summary = _compact_text(summary_text)
    if not summary:
        return ""
    first_sentence = _compact_text(re.split(r"(?<=[.!?])\s+", summary, maxsplit=1)[0])
    lowered_first = first_sentence.lower()
    if re.match(
        r"^(?:a|an)\s+(?:newsletter|news digest|question digest|article alert|promotional update|job alert|activity update|reminder)\s+from\b",
        lowered_first,
    ):
        return summary
    if re.match(r"^[a-z0-9 .&'_-]+\s+sent\s+(?:a|an)\b", lowered_first):
        return summary

    if _looks_actionable(email_data) and not _looks_bulk_or_newsletter(email_data):
        return summary

    if re.match(
        r"^(?:it\b|offers?\b|benefits?\b|members?\b|customers?\b|the offer\b|this offer\b|"
        r"savings?\b|discount\b|shipping\b|prices?\b|valid\b)",
        lowered_first,
    ):
        intro = _summary_source_intro(email_data)
        if intro:
            return f"{intro}. {summary}"
    return summary


def _finalize_summary_text(summary_text, email_data):
    """Polish summary wording so the UI lead reads naturally."""
    summary = _rewrite_summary_for_second_person(summary_text)
    if not summary:
        return ""

    summary = re.sub(r"^It also\b", "It", summary, flags=re.IGNORECASE)
    summary = re.sub(r"^Also,\s*", "", summary, flags=re.IGNORECASE)
    summary = re.sub(r"^Also\b\s*", "", summary, flags=re.IGNORECASE)
    if summary and summary[0].islower():
        summary = summary[0].upper() + summary[1:]

    summary = _add_summary_source_intro(summary, email_data)
    return _naturalize_summary_scaffolding(summary)


def _uses_second_person(text):
    """Return True when text explicitly uses second-person wording."""
    return bool(re.search(r"\b(you|your)\b", str(text or "").lower()))


def _mailbox_owner_display_name():
    """Return the mailbox owner's preferred display name when available."""
    stored_name = _normalized_header_text(get_user_display_name() or "")
    if stored_name:
        return stored_name
    return _normalized_header_text((os.getenv(MAILBOX_OWNER_NAME_ENV) or "").strip())


def _mailbox_owner_context_block():
    """Return prompt context for the mailbox owner's saved display name."""
    display_name = _mailbox_owner_display_name()
    if not display_name:
        return ""
    return (
        "Mailbox owner profile:\n"
        f"- display_name: {display_name}\n"
        f"- email_address: {LOCAL_USER_EMAIL}\n"
        "- If you include a personal sign-off, use the display_name exactly and never "
        "use the email address as the owner's name.\n"
    )


def _reply_closing_text():
    """Return the preferred reply sign-off using the saved display name when available."""
    display_name = _mailbox_owner_display_name()
    if display_name:
        return f"Best regards,\n{display_name}"
    return "Best regards,"


def _normalize_owner_signature_in_draft(draft_text):
    """Replace a trailing owner email-address signature with the saved display name."""
    text = str(draft_text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    display_name = _mailbox_owner_display_name()
    if not text or not display_name:
        return text
    owner_email = LOCAL_USER_EMAIL.lower()
    if not owner_email:
        return text
    lines = text.split("\n")
    for index in range(len(lines) - 1, -1, -1):
        line = lines[index].strip()
        if not line:
            continue
        if line.lower() == owner_email:
            lines[index] = display_name
        break
    return "\n".join(lines).strip()


def _normalized_email_for_classification(email_data):
    """Normalized email for classification.
    """
    # Keep labels aligned with the mailbox triage buckets.
    return {
        "title": _normalized_header_text(email_data.get("title") or "(No subject)"),
        "sender": _normalized_header_text(email_data.get("sender")),
        "recipients": _normalized_header_text(email_data.get("recipients")),
        "cc": _normalized_header_text(email_data.get("cc")),
        "body": _clean_body_for_prompt(email_data),
    }


def _email_context_block(email_data, body_max_chars=8000, body_max_sentences=14):
    """Email context block.
    """
    # Build the email context block that gets passed into model prompts.
    owner_context = _mailbox_owner_context_block()
    title = _normalized_header_text(email_data.get("title") or "(No subject)")
    sender = _normalized_header_text(email_data.get("sender"))
    recipients = _normalized_header_text(email_data.get("recipients"))
    cc = _normalized_header_text(email_data.get("cc"))
    body = _body_for_context(
        email_data,
        max_chars=body_max_chars,
        max_sentences=body_max_sentences,
    )
    ai_category = _compact_text(email_data.get("ai_category"))
    ai_type = _compact_text(email_data.get("type"))
    ai_hint = ""
    if ai_category or ai_type:
        ai_hint = (
            "\nAI triage context:\n"
            f"- category: {ai_category or '(unknown)'}\n"
            f"- mailbox type: {ai_type or '(unknown)'}"
        )
    return (
        (f"{owner_context}\n" if owner_context else "")
        +
        f"Subject: {title}\n"
        f"From: {sender}\n"
        f"To: {recipients}\n"
        f"Cc: {cc}\n"
        f"Body excerpts:\n{body}{ai_hint}"
    )


def _sender_parts(sender_text):
    """Sender parts.
    """
    # Break sender text into structured parts for the heuristic classification logic.
    raw = _normalized_header_text(sender_text).lower()
    if not raw:
        return {
            "raw": "",
            "email": "",
            "local": "",
            "domain": "",
            "display": "",
            "identity": "",
        }

    email = ""
    # Prefer explicit addresses, then fall back to bracketed sender forms if needed.
    email_match = EMAIL_ADDRESS_PATTERN.search(raw)
    if email_match:
        email = email_match.group(1)
    else:
        bracket_match = re.search(r"<([^>]+)>", raw)
        if bracket_match:
            bracket_value = _compact_text(bracket_match.group(1)).lower()
            bracket_email_match = EMAIL_ADDRESS_PATTERN.search(bracket_value)
            if bracket_email_match:
                email = bracket_email_match.group(1)

    local = ""
    domain = ""
    if "@" in email:
        local, domain = email.split("@", 1)

    display = _compact_text(re.sub(r"<[^>]*>", " ", raw))
    if email and display:
        display = _compact_text(display.replace(email, " "))

    identity = _compact_text(
        " ".join(part for part in [raw, email, local, domain, display] if part)
    )
    return {
        "raw": raw,
        "email": email,
        "local": local,
        "domain": domain,
        "display": display,
        "identity": identity,
    }


def _sender_address(sender_text):
    """Sender address.
    """
    # Shared helper for this file.
    return _sender_parts(sender_text).get("email", "")


def _has_any_pattern(text, patterns):
    """Return whether any pattern.
    """
    # Check for any pattern match before we do heavier work.
    value = str(text or "").lower()
    return any(pattern in value for pattern in patterns)


def _matching_patterns(text, patterns):
    """Return matching patterns."""
    value = str(text or "").lower()
    return [pattern for pattern in patterns if pattern in value]


def _sender_looks_automated(sender_info):
    """Sender looks automated.
    """
    # Shared helper for this file.
    if not isinstance(sender_info, dict):
        return False
    identity = sender_info.get("identity", "")
    return _has_any_pattern(identity, AUTOMATED_SENDER_MARKERS)


def _sender_uses_personal_domain(sender_info):
    """Sender uses personal domain.
    """
    # Shared helper for this file.
    if not isinstance(sender_info, dict):
        return False
    domain = str(sender_info.get("domain") or "").strip().lower()
    if not domain:
        return False
    return domain in PERSONAL_EMAIL_DOMAINS


def _sender_hint_block(email_data):
    """Sender hint block.
    """
    # Shared helper for this file.
    sender_info = _sender_parts(email_data.get("sender"))
    sender_automated = _sender_looks_automated(sender_info)
    sender_personal_domain = _sender_uses_personal_domain(sender_info)
    return (
        "Sender signal:\n"
        f"- sender_email: {sender_info.get('email') or '(unknown)'}\n"
        f"- sender_domain: {sender_info.get('domain') or '(unknown)'}\n"
        f"- sender_local_part: {sender_info.get('local') or '(unknown)'}\n"
        f"- sender_looks_automated: {'yes' if sender_automated else 'no'}\n"
        f"- sender_uses_personal_domain: {'yes' if sender_personal_domain else 'no'}\n"
    )


def _classification_few_shot_block():
    """Classification few shot block.
    """
    # Keep labels aligned with the mailbox triage buckets.
    return (
        "Few-shot examples:\n"
        "1) From: promotions@store-updates.example\n"
        "Subject: Exclusive member savings this week\n"
        "Body: Limited-time offer. Click to shop now. Unsubscribe here.\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.73}\n\n'
        "2) From: dailybrief@news-digest.example\n"
        "Subject: Morning briefing: top stories\n"
        "Body: Today's headlines and links. Manage preferences.\n"
        'Output: {"category":"informational","needs_response":false,"priority":1,"confidence":0.92}\n\n'
        "3) From: manager@team.example\n"
        "Subject: Can you confirm by 3 PM?\n"
        "Body: Please reply with approval before the deadline.\n"
        'Output: {"category":"urgent","needs_response":true,"priority":3,"confidence":0.91}\n\n'
        "4) From: billing@service-notify.example\n"
        "Subject: Your monthly statement is ready\n"
        "Body: View your statement online. No action required unless there is an issue.\n"
        'Output: {"category":"informational","needs_response":false,"priority":1,"confidence":0.84}\n\n'
        "5) From: security-check@pay-verify.example\n"
        "Subject: Urgent action required: verify your account\n"
        "Body: We noticed unusual activity. Click here to confirm your identity or your access may be suspended.\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.88}\n\n'
        "6) From: growth@revenue-ops.example\n"
        "Subject: Quick question about pipeline growth\n"
        "Body: We help teams generate more leads and book more demos. Worth 15 minutes next week?\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.67}\n\n'
        "7) From: deals@brand-mail.example\n"
        "Subject: 30% off today plus free shipping\n"
        "Body: Use code SPRING30 at checkout. Shop now. Manage preferences or unsubscribe.\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.74}\n\n'
        "8) From: hello@fashion-brand.example\n"
        "Subject: New arrivals for spring\n"
        "Body: Discover fresh styles and featured picks from the new collection. Shop the collection online.\n"
        'Output: {"category":"junk","needs_response":false,"priority":1,"confidence":0.64}'
    )


def _looks_bulk_or_newsletter(email_data):
    """Looks bulk or newsletter.
    """
    # Keep this rule here so the behavior stays consistent.
    sender_info = _sender_parts(email_data.get("sender"))
    sender = sender_info.get("email", "")
    body = _email_body_text(email_data).lower()
    title = _normalized_header_text(email_data.get("title") or "").lower()
    combined = " ".join(part for part in [sender, title, body] if part)

    sender_markers = (
        "no-reply",
        "noreply",
        "donotreply",
        "newsletter",
        "digest",
        "updates",
        "notification",
        "announcements",
        "marketing",
    )
    content_markers = (
        "unsubscribe",
        "manage preferences",
        "view in browser",
        "read more",
        "latest news",
        "top stories",
        "today in",
        "latest in",
        "what's news",
        "morning brew",
        "quora digest",
        "daily briefing",
        "weekly briefing",
        "daily digest",
        "weekly digest",
        "breaking news",
        "manage subscription",
        "sponsored",
    )
    promotion_markers = (
        "% off",
        "promo",
        "discount",
        "offer",
        "discover",
        "save up to",
        "maximum savings",
        "limited time",
        "special offer",
        "new arrivals",
        "new collection",
        "shop the collection",
        "shop our",
        "take a trip",
        "shop now",
        "book now",
        "membership",
    )
    promotion_hits = _matching_patterns(combined, promotion_markers)
    return (
        _has_any_pattern(combined, sender_markers)
        or _has_any_pattern(combined, content_markers)
        or len(promotion_hits) >= 2
        or (
            _looks_marketing_promo_title(title)
            and not _sender_uses_personal_domain(sender_info)
        )
    )


def _commercial_promotion_assessment(
    combined_text,
    *,
    bulk_signal=False,
    sender_automated=False,
    transactional_like=False,
):
    """Return whether the email looks like commercial bulk promotion instead of editorial mail."""
    promotion_hits = _matching_patterns(combined_text, JUNK_PROMOTION_MARKERS)
    meaningful_promotion_hits = [
        hit
        for hit in promotion_hits
        if hit not in WEAK_PROMOTION_MARKERS
    ]
    footer_hits = _matching_patterns(combined_text, JUNK_BULK_FOOTER_MARKERS)
    editorial_hits = _matching_patterns(combined_text, JUNK_EDITORIAL_MARKERS)
    percent_off = bool(re.search(r"\b\d{1,3}%\s+off\b", combined_text))
    dollar_off = bool(re.search(r"\$\d+(?:\.\d{2})?\s+off\b", combined_text))
    promo_code = bool(
        re.search(r"\b(?:use|enter|apply)\s+code\s+[a-z0-9][a-z0-9\-]{2,}\b", combined_text)
    )
    free_shipping = "free shipping" in combined_text
    matched_terms = list(promotion_hits)
    if percent_off:
        matched_terms.append("percent_off_offer")
    if dollar_off:
        matched_terms.append("dollar_off_offer")
    if promo_code:
        matched_terms.append("promo_code")
    if free_shipping:
        matched_terms.append("free_shipping")

    bulkish_promotion = bool(meaningful_promotion_hits or len(promotion_hits) >= 2) and (
        bulk_signal or bool(footer_hits) or sender_automated
    )
    editorial_like = (
        len(editorial_hits) >= 2
        and len(meaningful_promotion_hits) < 2
        and not percent_off
        and not dollar_off
        and not promo_code
        and not free_shipping
    )
    commercial = (
        not transactional_like
        and not editorial_like
        and (
            percent_off
            or dollar_off
            or promo_code
            or free_shipping
            or len(meaningful_promotion_hits) >= 2
            or len(promotion_hits) >= 2
            or bulkish_promotion
        )
        and (bulk_signal or bool(footer_hits) or sender_automated)
    )
    strong = commercial and (
        percent_off
        or dollar_off
        or promo_code
        or (len(meaningful_promotion_hits) >= 2 and (footer_hits or sender_automated))
        or (bool(meaningful_promotion_hits) and bool(footer_hits) and sender_automated)
    )
    return {
        "commercial": commercial,
        "strong": strong,
        "editorial_like": editorial_like,
        "promotion_hits": promotion_hits,
        "footer_hits": footer_hits,
        "editorial_hits": editorial_hits,
        "matched_terms": matched_terms,
    }


def _looks_actionable(email_data):
    """Looks actionable.
    """
    title = str(email_data.get("title") or "").lower()
    body = _email_body_text(email_data).lower()
    combined = " ".join([title, body])

    # Require stronger response-language cues when the message looks like bulk mail.
    explicit_question = "?" in title or "?" in body
    direct_question_markers = (
        "can you",
        "could you",
        "would you",
        "will you",
        "please",
        "let me know",
        "confirm",
    )
    response_markers = (
        "please reply",
        "let me know",
        "can you",
        "could you",
        "please confirm",
        "needs your response",
        "respond by",
        "action required",
        "rsvp",
        "approval needed",
        "deadline",
        "asap",
    )
    non_actionable_markers = (
        "no action required",
        "no response needed",
        "no reply needed",
        "for your records",
        "unless there is an issue",
    )
    # Treat bulk messages as non-actionable unless they contain explicit response language.
    if _has_any_pattern(combined, non_actionable_markers):
        return False
    if _looks_bulk_or_newsletter(email_data) and not _has_any_pattern(
        combined, response_markers
    ):
        return False
    if explicit_question and _has_any_pattern(combined, direct_question_markers):
        return True
    return _has_any_pattern(combined, response_markers)


def _junk_signal_assessment(email_data):
    """Return generalized junk-signal features for heuristics and prompts."""
    sender_info = _sender_parts(email_data.get("sender"))
    title_text = _normalized_header_text(email_data.get("title") or "")
    body_text = _clean_body_for_prompt(email_data)
    combined_text = " ".join(
        part
        for part in [
            sender_info.get("identity"),
            title_text.lower(),
            body_text.lower(),
        ]
        if part
    )
    bulk_signal = _looks_bulk_or_newsletter(email_data)
    sender_automated = _sender_looks_automated(sender_info)
    sender_personal_domain = _sender_uses_personal_domain(sender_info)
    title_promotion = _looks_marketing_promo_title(title_text)
    transactional_hits = _matching_patterns(combined_text, TRANSACTIONAL_HAM_MARKERS)
    family_hits = {
        "strong_terms": _matching_patterns(combined_text, JUNK_STRONG_MARKERS),
        "money_bait": _matching_patterns(combined_text, JUNK_MONEY_BAIT_MARKERS),
        "promotion_cta": _matching_patterns(combined_text, JUNK_PROMOTION_MARKERS),
        "urgency_pressure": _matching_patterns(combined_text, JUNK_PRESSURE_MARKERS),
        "account_bait": _matching_patterns(combined_text, JUNK_ACCOUNT_BAIT_MARKERS),
        "bulk_footer": _matching_patterns(combined_text, JUNK_BULK_FOOTER_MARKERS),
        "leadgen": _matching_patterns(combined_text, JUNK_LEADGEN_MARKERS),
        "promotion_title": ["marketing_title"] if title_promotion else [],
    }
    url_count = len(re.findall(r"(?:https?://|www\.)", body_text.lower()))
    exclamation_count = f"{title_text} {body_text}".count("!")
    all_caps_word_count = len(re.findall(r"\b[A-Z0-9]{5,}\b", f"{title_text} {body_text}"))

    score = 0
    if family_hits["strong_terms"]:
        score += 5
    if family_hits["money_bait"]:
        score += 3
    if family_hits["promotion_cta"]:
        score += 2
    if family_hits["urgency_pressure"]:
        score += 2
    if family_hits["account_bait"]:
        score += 2
    if family_hits["bulk_footer"]:
        score += 1
    if family_hits["leadgen"]:
        score += 2
    if family_hits["promotion_title"]:
        score += 2
    if sender_automated:
        score += 1
    if bulk_signal:
        score += 1
    if url_count >= 2:
        score += 1
    if exclamation_count >= 3:
        score += 1
    if all_caps_word_count >= 2:
        score += 1
    if family_hits["leadgen"] and not sender_personal_domain:
        score += 1

    transactional_like = bool(transactional_hits) and not (
        family_hits["strong_terms"] or family_hits["money_bait"]
    )
    promotion_assessment = _commercial_promotion_assessment(
        combined_text,
        bulk_signal=bulk_signal,
        sender_automated=sender_automated,
        transactional_like=transactional_like,
    )
    commercial_promotion = promotion_assessment.get("commercial") or (
        not transactional_like
        and not promotion_assessment.get("editorial_like")
        and title_promotion
        and (bulk_signal or sender_automated)
    )
    family_hits["commercial_promotion"] = (
        promotion_assessment.get("matched_terms", [])
        if commercial_promotion
        else []
    )
    if (
        commercial_promotion
        and not family_hits["commercial_promotion"]
        and family_hits["promotion_title"]
    ):
        family_hits["commercial_promotion"] = list(family_hits["promotion_title"])
    if transactional_like:
        score -= 2
    if sender_personal_domain:
        score -= 1
    if commercial_promotion:
        score += 2
    score = max(0, score)

    strong = bool(family_hits["strong_terms"]) or promotion_assessment.get("strong") or (
        score >= 7
        and (
            family_hits["money_bait"]
            or family_hits["account_bait"]
            or family_hits["leadgen"]
        )
    ) or (
        family_hits["account_bait"]
        and family_hits["urgency_pressure"]
        and not sender_personal_domain
        and not transactional_like
    )
    soft = (
        not strong
        and (
            commercial_promotion
            or (
                score >= 3
                and (
                    family_hits["money_bait"]
                    or family_hits["promotion_cta"]
                    or family_hits["urgency_pressure"]
                    or family_hits["account_bait"]
                    or family_hits["leadgen"]
                    or family_hits["promotion_title"]
                )
            )
        )
    )
    active_families = [name for name, hits in family_hits.items() if hits]
    if transactional_like:
        active_families.append("transactional_context")
    return {
        "level": "strong" if strong else ("soft" if soft else None),
        "score": score,
        "families": active_families,
        "bulk_signal": bulk_signal,
        "sender_automated": sender_automated,
        "sender_personal_domain": sender_personal_domain,
        "transactional_like": transactional_like,
        "commercial_promotion": commercial_promotion,
        "editorial_like": promotion_assessment.get("editorial_like", False),
        "url_count": url_count,
    }


def _junk_signal_block(email_data):
    """Build junk-signal hint block for the classification prompt."""
    assessment = _junk_signal_assessment(email_data)
    families = ", ".join(assessment.get("families") or []) or "(none)"
    return (
        "Junk signal summary:\n"
        f"- automated_sender: {'yes' if assessment.get('sender_automated') else 'no'}\n"
        f"- personal_sender_domain: {'yes' if assessment.get('sender_personal_domain') else 'no'}\n"
        f"- bulk_or_newsletter: {'yes' if assessment.get('bulk_signal') else 'no'}\n"
        f"- transactional_context: {'yes' if assessment.get('transactional_like') else 'no'}\n"
        f"- commercial_promotion_pattern: {'yes' if assessment.get('commercial_promotion') else 'no'}\n"
        f"- editorial_newsletter_pattern: {'yes' if assessment.get('editorial_like') else 'no'}\n"
        f"- url_count: {assessment.get('url_count', 0)}\n"
        f"- junk_signal_score: {assessment.get('score', 0)}\n"
        f"- junk_signal_families: {families}\n"
    )


def _prefer_junk_uncertain_for_commercial_promotion(junk_assessment):
    """Return True when retail-style promotion should prefer junk confirmation."""
    families = set(junk_assessment.get("families") or ())
    if not junk_assessment.get("commercial_promotion"):
        return False
    if junk_assessment.get("editorial_like") or junk_assessment.get("transactional_like"):
        return False
    if families.intersection({"strong_terms", "money_bait", "account_bait", "leadgen"}):
        return False
    return bool(junk_assessment.get("bulk_signal") or "promotion_cta" in families)


def _with_commercial_promotion_guardrail(result, junk_assessment):
    """Attach a merge guardrail when heuristics identify commercial promo mail."""
    if not junk_assessment.get("commercial_promotion"):
        return result
    if junk_assessment.get("editorial_like") or junk_assessment.get("transactional_like"):
        return result
    guarded = dict(result)
    guarded["guardrail_reason"] = "commercial_promotion"
    return guarded


def _heuristic_classification(email_data):
    """Heuristic classification.
    """
    # Keep labels aligned with the mailbox triage buckets.
    # Heuristics give us a deterministic fallback when the model is missing or unsure.
    actionable = _looks_actionable(email_data)
    bulk_signal = _looks_bulk_or_newsletter(email_data)
    junk_assessment = _junk_signal_assessment(email_data)
    junk_signal = junk_assessment.get("level")
    junk_families = set(junk_assessment.get("families") or [])
    promotion_prefers_uncertain = _prefer_junk_uncertain_for_commercial_promotion(
        junk_assessment
    )
    if junk_signal == "strong":
        if promotion_prefers_uncertain:
            return _with_commercial_promotion_guardrail({
                "category": "junk",
                "needs_response": False,
                "priority": 1,
                "confidence": 0.74,
                "email_type": "junk-uncertain",
            }, junk_assessment)
        return _with_commercial_promotion_guardrail({
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.92,
            "email_type": "junk",
        }, junk_assessment)

    if junk_signal == "soft":
        return _with_commercial_promotion_guardrail({
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.68,
            "email_type": "junk-uncertain",
        }, junk_assessment)

    if (
        bulk_signal
        and not actionable
        and "promotion_cta" in junk_families
        and not junk_assessment.get("editorial_like")
        and not junk_assessment.get("transactional_like")
    ):
        return _with_commercial_promotion_guardrail({
            "category": "junk",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.61,
            "email_type": "junk-uncertain",
        }, junk_assessment)

    if bulk_signal and not actionable:
        return {
            "category": "informational",
            "needs_response": False,
            "priority": 1,
            "confidence": 0.88,
            "email_type": "read-only",
        }

    if actionable:
        return {
            "category": "urgent",
            "needs_response": True,
            "priority": 2,
            "confidence": 0.7,
            "email_type": "response-needed",
        }

    return {
        "category": "informational",
        "needs_response": False,
        "priority": 1,
        "confidence": 0.55,
        "email_type": "read-only",
    }


def _merge_with_heuristics(model_classification, heuristic_classification):
    """Merge with heuristics.
    """
    if not model_classification:
        return dict(heuristic_classification)

    merged = dict(model_classification)
    try:
        model_confidence = float(merged.get("confidence") or 0.0)
    except (TypeError, ValueError):
        model_confidence = 0.0
    heuristic_type = heuristic_classification.get("email_type")
    heuristic_guardrail = heuristic_classification.get("guardrail_reason") == "commercial_promotion"

    # The model gets first pass, but these guardrails keep low-confidence mailbox moves
    # from doing something a human would likely find reckless or surprising.
    # Nudge uncertain model outputs toward the safer mailbox behavior.
    if heuristic_type == "read-only":
        if bool(merged.get("needs_response")) and model_confidence < 0.9:
            merged["category"] = "informational"
            merged["needs_response"] = False
            merged["priority"] = min(int(merged.get("priority") or 1), 1)
            merged["email_type"] = "read-only"

    if heuristic_guardrail and str(merged.get("category") or "").strip().lower() != "junk":
        merged = dict(heuristic_classification)
        model_confidence = float(merged.get("confidence") or 0.0)

    if heuristic_type == "junk-uncertain":
        model_is_junk = str(merged.get("category") or "").strip().lower() == "junk"
        if not model_is_junk and model_confidence < 0.93:
            merged = dict(heuristic_classification)
            model_confidence = float(merged.get("confidence") or 0.0)
        elif model_is_junk:
            merged["category"] = "junk"
            merged["needs_response"] = False
            merged["priority"] = 1
            merged["confidence"] = min(model_confidence, JUNK_LOW_CONFIDENCE_THRESHOLD - 0.01)
            merged["email_type"] = "junk-uncertain"
            model_confidence = float(merged.get("confidence") or 0.0)

    if heuristic_type == "junk":
        model_is_junk = str(merged.get("category") or "").strip().lower() == "junk"
        # If heuristics strongly point to junk and the model is unsure, trust the heuristics.
        if not model_is_junk and model_confidence < 0.85:
            merged = dict(heuristic_classification)
            model_confidence = float(merged.get("confidence") or 0.0)

    if heuristic_type in {"junk", "junk-uncertain"}:
        model_is_junk = str(merged.get("category") or "").strip().lower() == "junk"
        if model_is_junk and heuristic_type == "junk-uncertain":
            merged["email_type"] = "junk-uncertain"

    if str(merged.get("category") or "").strip().lower() == "junk":
        if model_confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
            merged["email_type"] = "junk-uncertain"
        else:
            merged["email_type"] = "junk"

    return merged


def _extract_json_block(text):
    """Extract JSON block.
    """
    # Some payloads leave this field out, so read it carefully.
    stripped = str(text or "").strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    fenced_match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", stripped)
    if fenced_match:
        return fenced_match.group(1).strip()
    match = re.search(r"\{[\s\S]*\}", stripped)
    return match.group(0).strip() if match else None


def _parse_bool(value):
    """Parse bool.
    """
    # Validate this before we trust it.
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    return False


def _normalize_classification(raw_value):
    """Normalize classification.
    """
    # Keep classification in a consistent format across the app.
    category = str(raw_value.get("category") or "").strip().lower()
    try:
        priority = int(raw_value.get("priority"))
    except (TypeError, ValueError):
        priority = 1
    priority = max(1, min(3, priority))

    try:
        confidence = float(raw_value.get("confidence"))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    needs_response = _parse_bool(raw_value.get("needs_response"))
    email_type = str(raw_value.get("email_type") or "").strip().lower()
    if email_type not in VALID_EMAIL_TYPES:
        email_type = ""

    if category not in VALID_CATEGORIES:
        if needs_response or priority >= 3:
            category = "urgent"
        else:
            category = "informational"

    # Keep category and type consistent so downstream DB writes stay canonical.
    if email_type == "response-needed":
        needs_response = True
    elif email_type == "read-only":
        needs_response = False
        if category == "junk":
            category = "informational"
    elif email_type in {"junk", "junk-uncertain"}:
        category = "junk"
        needs_response = False
        if priority > 1:
            priority = 1

    if category == "junk":
        needs_response = False
        if priority > 1:
            priority = 1
        if not email_type:
            if confidence < JUNK_LOW_CONFIDENCE_THRESHOLD:
                email_type = "junk-uncertain"
            else:
                email_type = "junk"

    normalized = {
        "category": category,
        "needs_response": bool(needs_response),
        "priority": priority,
        "confidence": confidence,
    }
    if email_type:
        normalized["email_type"] = email_type
    return normalized


def _call_ollama(task, messages, email_id=None, temperature=0.1, num_predict=320):
    """Call Ollama chat API and return response content or None on failure."""
    # Send one non-streaming chat request to Ollama and return the model text.
    started_at = time.perf_counter()
    api_urls = _api_url_candidates()
    model_selection = _resolve_model_selection(task=task, api_urls=api_urls)
    requested_model_name = model_selection["requested_model"]
    model_name = model_selection["resolved_model"]
    request_timeout = _timeout_seconds(task=task)
    keep_alive = _keep_alive_value(task=task)
    image_count = sum(len(message.get("images") or ()) for message in messages or ())
    prompt_chars = sum(len(str(message.get("content") or "")) for message in messages or ())
    if not model_name:
        available_models = ",".join(model_selection["available_models"]) or "(none)"
        _log_action(
            task=task,
            status="error",
            email_id=email_id,
            detail=(
                "requested_ollama_model_unavailable "
                f"requested={requested_model_name} strict={int(model_selection['strict'])} "
                f"available={available_models}"
            ),
        )
        return None
    if model_selection["substituted"]:
        _log_action(
            task=task,
            status="fallback",
            email_id=email_id,
            detail=(
                "ollama_model_substitution "
                f"requested={requested_model_name} resolved={model_name} reason={model_selection['reason']}"
            ),
        )
    _log_action(
        task=task,
        status="call_start",
        email_id=email_id,
        detail=(
            f"ollama_chat requested_model={requested_model_name} model={model_name} timeout={int(request_timeout)}s "
            f"keep_alive={keep_alive or '-'} url={api_urls[0]} images={image_count} prompt_chars={prompt_chars}"
        ),
    )

    if not _endpoint_allowed():
        _log_action(
            task=task,
            status="error",
            email_id=email_id,
            detail=f"Blocked non-local Ollama endpoint: {_api_url()}",
        )
        return None

    payload = {
        "model": model_name,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
        },
    }
    if keep_alive:
        payload["keep_alive"] = keep_alive
    body = json.dumps(payload).encode("utf-8")
    raw = None
    last_error = None
    for candidate_url in api_urls:
        request_obj = urllib.request.Request(
            candidate_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request_obj, timeout=request_timeout) as response:
                raw = response.read().decode("utf-8")
            if candidate_url != api_urls[0]:
                _log_action(
                    task=task,
                    status="fallback",
                    email_id=email_id,
                    detail=f"ollama_endpoint_fallback_success url={candidate_url}",
                )
            break
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            continue

    if raw is None:
        _log_action(
            task=task,
            status="error",
            email_id=email_id,
            detail=(
                f"request_failed: {last_error} "
                f"elapsed_ms={int((time.perf_counter() - started_at) * 1000)}"
            ),
        )
        return None

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        _log_action(task=task, status="error", email_id=email_id, detail=f"json_decode_failed: {exc}")
        return None

    content = ((parsed.get("message") or {}).get("content") or "").strip()
    if not content:
        _log_action(task=task, status="error", email_id=email_id, detail="empty_response_content")
        return None

    _log_action(
        task=task,
        status="call_success",
        email_id=email_id,
        detail=f"chars={len(content)} elapsed_ms={int((time.perf_counter() - started_at) * 1000)}",
    )
    return content


def classify_email(email_data, email_id=None):
    """Classify email.
    """
    started_at = time.perf_counter()
    # Keep labels aligned with the mailbox triage buckets.
    normalized_email = _normalized_email_for_classification(email_data)
    body = (normalized_email.get("body") or "").strip()
    title = (normalized_email.get("title") or "").strip()
    if not body and not title:
        return None

    heuristic = _heuristic_classification(normalized_email)
    user_message = _vision_user_message(
        normalized_email,
        (
            "Classify this email for triage. Return JSON only."
        ),
        email_id=email_id,
        task="classify",
        text_max_chars=1200,
        allow_visual=False,
    )
    if not user_message:
        return heuristic

    # Ask for strict JSON, then combine with deterministic heuristics for stability.
    system_prompt = (
        "You classify inbox emails for triage. "
        "Return valid JSON only with exactly these keys: category, needs_response, priority, confidence. "
        "category must be urgent, informational, or junk. "
        "needs_response must be true or false. "
        "priority must be an integer from 1 to 3. "
        "confidence must be a float from 0 to 1. "
        "Use the email body as primary evidence, sender second, subject third. "
        "Treat ads, coupons, sales blasts, marketing promotions, scam/phishing mail, and unsolicited lead-gen pitches as junk. "
        "Treat receipts, account notices, newsletters, and service updates as informational unless the sender clearly wants a reply."
    )
    user_message["content"] += (
        "\n\n"
        f"{_sender_hint_block(normalized_email)}\n"
        f"{_junk_signal_block(normalized_email)}"
        "Return JSON only."
    )
    response_text = _call_ollama(
        task="classify",
        messages=[
            {"role": "system", "content": system_prompt},
            user_message,
        ],
        email_id=email_id,
        temperature=0.0,
        num_predict=_num_predict_for_task("classify", CLASSIFY_NUM_PREDICT_DEFAULT),
    )
    if not response_text:
        _log_performance(
            "ollama_classification",
            int((time.perf_counter() - started_at) * 1000),
            email_id=email_id,
            used_model=_model_name(task="classify"),
            used_model_response=0,
        )
        return heuristic
    # Fall back cleanly when the model emits malformed/non-JSON output.
    json_block = _extract_json_block(response_text)
    if not json_block:
        _log_action(task="classify", status="error", email_id=email_id, detail="missing_json_block")
        _log_performance(
            "ollama_classification",
            int((time.perf_counter() - started_at) * 1000),
            email_id=email_id,
            used_model=_model_name(task="classify"),
            used_model_response=1,
            parsed_json=0,
        )
        return heuristic
    try:
        parsed = json.loads(json_block)
    except json.JSONDecodeError as exc:
        _log_action(task="classify", status="error", email_id=email_id, detail=f"invalid_json: {exc}")
        _log_performance(
            "ollama_classification",
            int((time.perf_counter() - started_at) * 1000),
            email_id=email_id,
            used_model=_model_name(task="classify"),
            used_model_response=1,
            parsed_json=0,
        )
        return heuristic
    normalized = _normalize_classification(parsed)
    merged = _merge_with_heuristics(normalized, heuristic)
    result = _normalize_classification(merged)
    _log_performance(
        "ollama_classification",
        int((time.perf_counter() - started_at) * 1000),
        email_id=email_id,
        used_model=_model_name(task="classify"),
        used_model_response=1,
        parsed_json=1,
    )
    return result


def _postprocess_model_summary(summary_text, email_data, email_id=None, structured=False):
    """Sanitize and finalize model output before returning it to the UI."""
    profile = _summary_profile(email_data)
    prepared = _prepare_model_summary(summary_text, profile["char_limit"])
    if not prepared:
        return None

    sanitized = _sanitize_model_summary(prepared, email_data, structured=structured)
    candidate = sanitized or prepared
    finalized = _finalize_summary_text(candidate, email_data)
    if (
        finalized
        and _looks_actionable(email_data)
        and not _looks_bulk_or_newsletter(email_data)
        and _uses_second_person(candidate)
        and finalized.lower().startswith(
            (
                "the email asks ",
                "the email asks for ",
                "the email recommends ",
                "the recipient needs to ",
            )
        )
    ):
        finalized = candidate
    if not finalized:
        return None
    if _looks_summary_failure(finalized):
        return None
    if _looks_generic_posture_summary(finalized):
        return None
    if _looks_summary_call_to_action(finalized, email_data):
        return None
    if _looks_bulk_summary_boilerplate_heavy(finalized, email_data):
        return None
    if _summary_uses_subject_content(finalized, email_data):
        allowed_subject_led = (
            _looks_digest_title_summary(finalized)
            or finalized.lower().startswith("a promotional update from ")
        )
        if not allowed_subject_led:
            return None
    if _looks_summary_parrot(finalized, email_data):
        return None
    if len(finalized) > profile["char_limit"]:
        finalized = f"{finalized[: profile['char_limit'] - 3].rstrip()}..."
    return finalized or None


def summarize_email(email_data, email_id=None):
    """Summarize email.
    """
    started_at = time.perf_counter()
    # Convert API data into the mailbox shape the app uses locally.
    if not should_summarize_email(email_data):
        return None
    raw_body = _email_body_text(email_data)
    if re.search(r"\b(?:read more|view in browser|open in browser)\b", raw_body, flags=re.IGNORECASE):
        fast_article_summary = _usable_summary_candidate(
            _single_article_alert_summary(email_data),
            email_data,
        )
        if fast_article_summary:
            _log_performance(
                "ollama_summary",
                int((time.perf_counter() - started_at) * 1000),
                email_id=email_id,
                model_attempts=0,
                visual_escalated=0,
                used_fallback=1,
                path="fast_article",
            )
            return fast_article_summary
    profile = _summary_profile(email_data)
    structured_summary = _should_use_structured_summary(email_data)
    extractive_fallback = _extractive_summary_fallback(email_data)
    structured_fallback = _structured_summary_fallback(email_data) if structured_summary else None
    fallback_candidates = (
        [extractive_fallback, structured_fallback]
        if _looks_bulk_or_newsletter(email_data)
        else [structured_fallback, extractive_fallback]
    )
    fallback_summary = None
    # We build the local fallback before talking to the model so there is always a
    # decent summary waiting in the wings if the model stalls, rambles, or returns noise.
    for candidate in fallback_candidates:
        finalized_candidate = _finalize_summary_text(candidate, email_data) or None
        if not finalized_candidate:
            continue
        if not fallback_summary:
            fallback_summary = finalized_candidate
        if not _looks_generic_posture_summary(finalized_candidate):
            fallback_summary = finalized_candidate
            break
    text_user_message = _vision_user_message(
        email_data,
        (
            "Analyze the provided email source context and summarize the email. "
            "Use the email body text as the main source and the metadata only as support."
        ),
        email_id=email_id,
        task="summarize",
        text_max_chars=profile["context_chars"],
        allow_visual=False,
    )
    if not text_user_message:
        _log_performance(
            "ollama_summary",
            int((time.perf_counter() - started_at) * 1000),
            email_id=email_id,
            model_attempts=0,
            visual_escalated=0,
            used_fallback=int(bool(fallback_summary)),
            path="no_prompt",
        )
        return fallback_summary
    visual_decision = _summary_visual_decision(email_data)

    system_prompt = (
        "You summarize emails for the mailbox owner. "
        "Analyze the provided email source context and write a natural plain-text summary of what the email says. "
        "Use the email content as the source of truth and use the sender and subject metadata only as support. "
        "Capture the main topic, important details, and any requested action or deadline when present. "
        "If the email has multiple sections or updates, cover the main ones. "
        + f"Aim for {profile['prompt_target']}. "
        + "Return plain text only."
    )
    text_user_message["content"] += (
        "\n\n"
        "Write the summary that best fits this email. "
        "Keep it grounded in the provided email content, concise, and readable. "
        "Mention any action item or deadline if there is one."
    )
    response_text = _call_ollama(
        task="summarize",
        messages=[
            {"role": "system", "content": system_prompt},
            text_user_message,
        ],
        email_id=email_id,
        temperature=0.2,
        num_predict=_num_predict_for_task("summarize", profile["num_predict"]),
    )
    summary = _postprocess_model_summary(
        response_text,
        email_data,
        email_id=email_id,
        structured=structured_summary,
    ) if response_text else None
    model_attempts = 1
    visual_escalated = 0

    if not summary and visual_decision.get("should_escalate"):
        visual_escalated = 1
        _log_action(
            task="summarize",
            status="fallback",
            email_id=email_id,
            detail=(
                "escalating_to_visual_summary "
                f"reason={visual_decision.get('reason')} "
                f"plain_chars={visual_decision.get('plain_chars')} "
                f"html_chars={visual_decision.get('html_chars')} "
                f"complexity_hits={visual_decision.get('complexity_hits')} "
                f"text_overlap={visual_decision.get('text_overlap')}"
            ),
        )
        visual_user_message = _vision_user_message(
            email_data,
            (
                "Analyze the provided email source context and summarize the email. "
                "Use the email body text as the main source and the metadata only as support."
            ),
            email_id=email_id,
            task="summarize",
            text_max_chars=profile["context_chars"],
            allow_visual=True,
            force_visual=True,
        )
        if visual_user_message:
            visual_user_message["content"] += (
                "\n\n"
                "Write the summary that best fits this email. "
                "Keep it grounded in the provided email content, concise, and readable. "
                "Mention any action item or deadline if there is one."
            )
            response_text = _call_ollama(
                task="summarize",
                messages=[
                    {"role": "system", "content": system_prompt},
                    visual_user_message,
                ],
                email_id=email_id,
                temperature=0.2,
                num_predict=_num_predict_for_task("summarize", profile["num_predict"]),
            )
            model_attempts += 1
            if response_text:
                summary = _postprocess_model_summary(
                    response_text,
                    email_data,
                    email_id=email_id,
                    structured=structured_summary,
                )

    if not summary and fallback_summary:
        detail = (
            "using_extractive_fallback_no_model_response"
            if not response_text
            else "using_extractive_fallback_unusable_model_output"
        )
        _log_action(
            task="summarize",
            status="fallback",
            email_id=email_id,
            detail=detail,
        )
    if summary and fallback_summary:
        summary = _prefer_richer_promotional_fallback(summary, fallback_summary)
    final_summary = summary or fallback_summary
    _log_performance(
        "ollama_summary",
        int((time.perf_counter() - started_at) * 1000),
        email_id=email_id,
        model_attempts=model_attempts,
        visual_escalated=visual_escalated,
        used_fallback=int(bool(final_summary and final_summary == fallback_summary and not summary)),
        visual_reason=visual_decision.get("reason"),
    )
    return final_summary


def _sender_display_name(sender_text):
    """Extract a readable sender name from the raw sender field."""
    # Shared helper for this file.
    sender_raw = _normalized_header_text(sender_text)
    if not sender_raw:
        return ""
    display = sender_raw.split("<", 1)[0].strip().strip('"')
    if "@" in display and " " not in display:
        display = display.split("@", 1)[0]
    display = display.replace(".", " ").replace("_", " ").replace("-", " ")
    return " ".join(display.split())


def _first_request_sentence(email_data):
    """Return the first sentence that looks like an explicit request."""
    # Shared helper for this file.
    for sentence in _extract_key_sentences(_email_body_text(email_data), max_sentences=8):
        lowered = sentence.lower()
        if "?" in sentence:
            return sentence
        if any(marker in lowered for marker in REQUEST_SENTENCE_MARKERS):
            return sentence
    return None


def _reply_plan_source_text(email_data, max_chars=3200):
    """Return compact source text for reply-plan grounding checks."""
    title = _normalized_header_text(email_data.get("title") or "(No subject)")
    body = _clean_body_for_prompt(email_data, max_chars=max_chars)
    combined = "\n".join(part for part in (title, body) if _compact_text(part))
    return _compact_text(combined)


def _dedupe_text_items(items, max_items=4, max_chars=180):
    """Return unique short text items while preserving order."""
    selected = []
    for item in items or ():
        text = _truncate_compact_text(item, max_chars=max_chars)
        if len(text) < 12:
            continue
        if any(_token_overlap_ratio(text, existing) > 0.9 for existing in selected):
            continue
        selected.append(text)
        if len(selected) >= max_items:
            break
    return selected


def _reply_plan_tone(email_data, deadline=""):
    """Infer a lightweight tone label for reply drafting."""
    combined = _compact_text(
        " ".join(
            [
                str(email_data.get("title") or ""),
                _email_body_text(email_data),
            ]
        )
    ).lower()
    if not _looks_actionable(email_data):
        return "informational"
    if deadline or re.search(r"\b(?:urgent|asap|today|tomorrow|immediately|by end of day|eod)\b", combined):
        return "urgent"
    if re.search(r"\b(?:sorry|apologize|apologies)\b", combined):
        return "apologetic"
    if re.search(r"\b(?:thanks|thank you|appreciate)\b", combined):
        return "friendly"
    return "professional"


def _reply_topic_phrase(text):
    """Return a cleaner topical phrase from a request-like subject or sentence."""
    phrase = _compact_text(text).strip(" -:;,.")
    if not phrase:
        return ""
    phrase = re.sub(
        r"^(?:please\s+)?(?:confirm|review|approve|share|send|provide|update|"
        r"let\s+me\s+know(?:\s+(?:if|whether))?|can\s+you|could\s+you|would\s+you|will\s+you)\s+",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = _compact_text(phrase).strip(" -:;,.")
    if not phrase:
        return ""
    return phrase[0].lower() + phrase[1:] if len(phrase) > 1 else phrase.lower()


def _heuristic_reply_plan(email_data):
    """Extract a compact reply plan from the email without model assistance."""
    title = _summary_title_topic(email_data.get("title") or "") or _compact_text(
        email_data.get("title") or ""
    )
    title = _reply_topic_phrase(title) or title
    request_sentence = _truncate_compact_text(_first_request_sentence(email_data), max_chars=180)
    key_sentences = _extract_key_sentences(email_data, max_sentences=6)

    topic = title
    if not topic or topic.lower() == "(no subject)":
        seed_sentence = request_sentence or (key_sentences[0] if key_sentences else "")
        topic = _reply_topic_phrase(_topic_phrase_from_sentence(seed_sentence)) or _truncate_compact_text(
            seed_sentence or "your message",
            max_chars=110,
        )

    deadline = ""
    deadline_candidates = []
    for candidate in [request_sentence] + key_sentences[:4]:
        deadline_candidate = _extract_deadline_phrase(candidate)
        if deadline_candidate:
            deadline_candidates.append(deadline_candidate)
    if deadline_candidates:
        deadline = next(
            (
                candidate
                for candidate in deadline_candidates
                if candidate.lower().startswith(("by ", "before "))
            ),
            deadline_candidates[0],
        )

    details = []
    if request_sentence:
        details.append(request_sentence)
    for sentence in key_sentences:
        detail = _compact_text(_strip_title_prefix(sentence, title))
        if not detail:
            continue
        if request_sentence and _token_overlap_ratio(detail, request_sentence) > 0.9:
            continue
        details.append(detail)
    details = _dedupe_text_items(details, max_items=4, max_chars=180)

    response_mode = "acknowledge_only"
    if _looks_actionable(email_data):
        response_mode = "clarify" if not request_sentence and len(details) < 2 else "answer_or_confirm"

    return {
        "sender_name": _sender_display_name(email_data.get("sender")),
        "topic": _truncate_compact_text(topic or "your message", max_chars=110),
        "sender_request": request_sentence or "",
        "deadline": _truncate_compact_text(deadline, max_chars=80),
        "key_details": details,
        "tone": _reply_plan_tone(email_data, deadline=deadline),
        "response_mode": response_mode,
        "should_ask_clarifying_question": response_mode == "clarify",
    }


def _grounded_reply_plan_text(value, source_text, max_chars=180):
    """Return a grounded short plan field or an empty string."""
    text = _truncate_compact_text(value, max_chars=max_chars)
    if not text:
        return ""
    lowered_text = text.lower()
    lowered_source = _compact_text(source_text).lower()
    if lowered_text in lowered_source:
        return text
    overlap = _token_overlap_ratio(text, source_text)
    if len(_content_tokens(text)) <= 3:
        return text if overlap >= 0.5 else ""
    return text if overlap >= 0.68 else ""


def _merge_reply_plan(model_plan, heuristic_plan, email_data):
    """Merge a model-extracted reply plan with deterministic heuristics."""
    merged = dict(heuristic_plan)
    if not isinstance(model_plan, dict):
        return merged

    source_text = _reply_plan_source_text(email_data)
    topic = _grounded_reply_plan_text(model_plan.get("topic"), source_text, max_chars=110)
    if topic:
        merged["topic"] = topic

    sender_request = _grounded_reply_plan_text(
        model_plan.get("sender_request"),
        source_text,
        max_chars=180,
    )
    if sender_request:
        merged["sender_request"] = sender_request

    deadline = _truncate_compact_text(model_plan.get("deadline"), max_chars=80)
    if deadline:
        extracted_deadline = _extract_deadline_phrase(deadline)
        grounded_deadline = extracted_deadline or _grounded_reply_plan_text(
            deadline,
            source_text,
            max_chars=80,
        )
        if grounded_deadline:
            merged["deadline"] = grounded_deadline

    key_details = []
    for item in model_plan.get("key_details") or ():
        grounded_item = _grounded_reply_plan_text(item, source_text, max_chars=180)
        if grounded_item:
            key_details.append(grounded_item)
    if key_details:
        merged["key_details"] = _dedupe_text_items(
            key_details + list(merged.get("key_details") or ()),
            max_items=4,
            max_chars=180,
        )

    tone = _compact_text(model_plan.get("tone")).lower()
    if tone in REPLY_PLAN_TONES:
        merged["tone"] = tone

    response_mode = _compact_text(model_plan.get("response_mode")).lower()
    if response_mode in REPLY_PLAN_RESPONSE_MODES:
        merged["response_mode"] = response_mode

    if "should_ask_clarifying_question" in model_plan:
        merged["should_ask_clarifying_question"] = bool(
            model_plan.get("should_ask_clarifying_question")
        )

    if not merged.get("sender_request"):
        merged["response_mode"] = (
            "clarify" if _looks_actionable(email_data) else "acknowledge_only"
        )
    return merged


def _extract_reply_plan(email_data, email_id=None):
    """Return a structured reply plan for drafting and revision."""
    cached_plan = email_data.get("_reply_plan_cache") if isinstance(email_data, dict) else None
    if isinstance(cached_plan, dict):
        return cached_plan

    heuristic_plan = _heuristic_reply_plan(email_data)
    if not _html_requires_visual_context(email_data):
        if isinstance(email_data, dict):
            email_data["_reply_plan_cache"] = heuristic_plan
        _log_action(
            task="draft_plan",
            status="fallback",
            email_id=email_id,
            detail="using_heuristic_reply_plan_text_source",
        )
        return heuristic_plan

    body = _clean_body_for_prompt(email_data, max_chars=2600)
    user_message = _vision_user_message(
        email_data,
        (
            "Extract a reply plan for the mailbox owner from the provided email source context. "
            "Return JSON only."
        ),
        email_id=email_id,
        task="draft_plan",
        text_max_chars=2600,
    )
    if not body or not user_message:
        if isinstance(email_data, dict):
            email_data["_reply_plan_cache"] = heuristic_plan
        return heuristic_plan

    system_prompt = (
        "You extract compact reply plans from emails. "
        "Use only facts explicitly present in the provided email content. "
        "Do not invent names, deadlines, commitments, or missing context. "
        "Return JSON only with keys: topic, sender_request, deadline, key_details, tone, "
        "response_mode, should_ask_clarifying_question. "
        "key_details must be an array of up to 4 short strings. "
        "response_mode must be one of answer_or_confirm, clarify, acknowledge_only. "
        "tone must be one of professional, friendly, urgent, informational, apologetic. "
        "Use empty strings or false when a field is unknown."
    )
    user_message["content"] += (
        "\n\n"
        "Extract the sender's request, deadline, key details, and expected response style. "
        "Use the provided email content as the source of truth. "
        "Return JSON only."
    )
    response_text = _call_ollama(
        task="draft_plan",
        messages=[
            {"role": "system", "content": system_prompt},
            user_message,
        ],
        email_id=email_id,
        temperature=0.0,
        num_predict=_num_predict_for_task("draft_plan", 180),
    )
    if not response_text:
        if isinstance(email_data, dict):
            email_data["_reply_plan_cache"] = heuristic_plan
        return heuristic_plan

    json_block = _extract_json_block(response_text)
    if not json_block:
        if isinstance(email_data, dict):
            email_data["_reply_plan_cache"] = heuristic_plan
        return heuristic_plan
    try:
        parsed = json.loads(json_block)
    except json.JSONDecodeError:
        if isinstance(email_data, dict):
            email_data["_reply_plan_cache"] = heuristic_plan
        return heuristic_plan

    merged_plan = _merge_reply_plan(parsed, heuristic_plan, email_data)
    if isinstance(email_data, dict):
        email_data["_reply_plan_cache"] = merged_plan
    return merged_plan


def _reply_plan_block(reply_plan, email_data=None):
    """Build compact plan text for draft and revise prompts."""
    plan = reply_plan or {}
    details = "\n".join(
        f"- {detail}" for detail in _dedupe_text_items(plan.get("key_details") or (), max_items=4)
    ) or "- (none)"
    owner_context = _mailbox_owner_context_block() if isinstance(email_data, dict) else ""
    sender_name = _compact_text(plan.get("sender_name")) or _sender_display_name(
        email_data.get("sender") if isinstance(email_data, dict) else ""
    )
    return (
        (f"{owner_context}\n" if owner_context else "")
        + "Reply plan:\n"
        + f"- sender_name: {sender_name or '(unknown)'}\n"
        + f"- response_mode: {_compact_text(plan.get('response_mode')) or 'answer_or_confirm'}\n"
        + f"- tone: {_compact_text(plan.get('tone')) or 'professional'}\n"
        + f"- topic: {_compact_text(plan.get('topic')) or '(none)'}\n"
        + f"- sender_request: {_compact_text(plan.get('sender_request')) or '(none explicit)'}\n"
        + f"- deadline: {_compact_text(plan.get('deadline')) or '(none)'}\n"
        + "- should_ask_clarifying_question: "
        + ("yes\n" if plan.get("should_ask_clarifying_question") else "no\n")
        + f"- key_details:\n{details}\n"
    )


def _reply_guidance_block(email_data, reply_plan=None):
    """Build compact reply guidance from the extracted reply plan."""
    return _reply_plan_block(reply_plan or _extract_reply_plan(email_data), email_data=email_data)


def _draft_reply_fallback(email_data, reply_plan=None):
    """Build a safe contextual fallback draft when model output is missing or unusable."""
    # Shared fallback for draft replies in the reply and draft flow.
    plan = reply_plan or _extract_reply_plan(email_data)
    sender_name = _compact_text(plan.get("sender_name")) or _sender_display_name(email_data.get("sender"))
    greeting = f"Hi {sender_name}," if sender_name else "Hi,"
    topic = _compact_text(plan.get("topic")) or "your message"
    request_sentence = _compact_text(plan.get("sender_request"))
    deadline = _compact_text(plan.get("deadline"))
    details = list(plan.get("key_details") or ())
    detail_candidates = [
        detail
        for detail in details
        if not request_sentence or _token_overlap_ratio(detail, request_sentence) < 0.9
    ]
    extra_detail = next(
        (
            detail
            for detail in detail_candidates
            if _token_overlap_ratio(detail, deadline) < 0.7
            and not detail.lower().startswith("let me know")
        ),
        detail_candidates[0] if detail_candidates else "",
    )

    if _looks_actionable(email_data):
        body_parts = [f"Thanks for your email about {topic}."]
        if request_sentence:
            if topic and _token_overlap_ratio(topic, request_sentence) < 0.75:
                body_parts.append(f"I understand the request about {topic}.")
            else:
                body_parts.append("I understand the request you sent over.")
        elif extra_detail:
            body_parts.append(f"I noted the request regarding {extra_detail.rstrip('?.!')}.")
        if extra_detail and (not request_sentence or _token_overlap_ratio(extra_detail, request_sentence) < 0.75):
            body_parts.append(f"I also noted {extra_detail.rstrip('?.!')}.")
        if deadline:
            body_parts.append(f"I'll send you a direct response {deadline}.")
        else:
            body_parts.append("I'll send you a direct response once I finish reviewing the details.")
        if plan.get("should_ask_clarifying_question"):
            body_parts.append("If there is a specific format or priority you want me to use, please let me know.")
    else:
        body_parts = [f"Thanks for the update about {topic}."]
        if extra_detail:
            body_parts.append(f"I noted {extra_detail.rstrip('?.!')}.")
        body_parts.append("Let me know if you want me to take any follow-up action.")

    body_text = " ".join(_ensure_sentence_ending(part) for part in body_parts if _compact_text(part))
    return f"{greeting}\n\n{body_text}\n\n{_reply_closing_text()}"


def _draft_sentences(text):
    """Split draft-like text into compact sentence units."""
    return [
        _compact_text(part)
        for part in re.split(r"(?<=[.!?])\s+", _compact_text(text))
        if _compact_text(part)
    ]


def _shared_ngram_count(left_text, right_text, n=6):
    """Return the number of shared long token phrases between two texts."""
    left_tokens = _text_tokens(left_text)
    right_tokens = _text_tokens(right_text)
    if len(left_tokens) < n or len(right_tokens) < n:
        return 0
    left_ngrams = {" ".join(left_tokens[index : index + n]) for index in range(len(left_tokens) - n + 1)}
    right_ngrams = {" ".join(right_tokens[index : index + n]) for index in range(len(right_tokens) - n + 1)}
    return len(left_ngrams & right_ngrams)


def _draft_copy_metrics(draft_text, email_data):
    """Return overlap metrics used to reject copied or lightly reworded drafts."""
    candidate_text = _reply_core_body(draft_text) or _compact_text(draft_text)
    source_text = _reply_plan_source_text(email_data, max_chars=2600)
    source_sentences = _draft_sentences(source_text)
    draft_sentences = _draft_sentences(candidate_text)
    copied_sentences = 0
    for sentence in draft_sentences:
        lowered_sentence = sentence.lower()
        if lowered_sentence and lowered_sentence in source_text.lower():
            copied_sentences += 1
            continue
        if any(
            _token_overlap_ratio(sentence, source_sentence) >= 0.88
            and not (
                source_sentence.endswith("?")
                and any(marker in lowered_sentence for marker in DIRECT_REPLY_MARKERS)
            )
            for source_sentence in source_sentences
        ):
            copied_sentences += 1
    first_sentence_copied = False
    if draft_sentences and source_sentences:
        first_sentence_lower = draft_sentences[0].lower()
        first_sentence_copied = (
            (
                _token_overlap_ratio(draft_sentences[0], source_sentences[0]) >= 0.86
                or draft_sentences[0].lower() in source_text.lower()
            )
            and not (
                source_sentences[0].endswith("?")
                and any(marker in first_sentence_lower for marker in DIRECT_REPLY_MARKERS)
            )
        )
    return {
        "source_text": source_text,
        "draft_sentences": draft_sentences,
        "copied_sentences": copied_sentences,
        "copied_ratio": copied_sentences / float(len(draft_sentences)) if draft_sentences else 0.0,
        "shared_long_phrases": _shared_ngram_count(candidate_text, source_text, n=6),
        "first_sentence_copied": first_sentence_copied,
    }


def _draft_addresses_reply_plan(draft_text, email_data, reply_plan=None):
    """Return True when a draft actually addresses the extracted request."""
    plan = reply_plan or _heuristic_reply_plan(email_data)
    cleaned = _compact_text(draft_text)
    lowered = cleaned.lower()
    metrics = _draft_specificity_metrics(cleaned, email_data, reply_plan=plan)
    actionable = _looks_actionable(email_data)
    has_response_marker = any(marker in lowered for marker in DIRECT_REPLY_MARKERS) or "?" in cleaned

    if not actionable:
        return metrics["detail_overlap"] >= 1 or not _looks_generic_draft(cleaned, email_data, reply_plan=plan)

    if plan.get("response_mode") == "clarify" and "?" in cleaned and metrics["detail_overlap"] >= 2:
        return True
    if metrics["request_overlap"] >= 2 and (metrics["detail_overlap"] >= 2 or has_response_marker):
        return True
    if plan.get("deadline") and metrics["detail_overlap"] >= 2 and has_response_marker:
        return True
    return False


def _sanitize_reply_output(draft_text):
    """Remove placeholder signature artifacts from model-generated replies."""
    text = str(draft_text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return ""
    lines = text.split("\n")
    placeholder_patterns = (
        r"^\[?\s*your name\s*\]?$",
        r"^\[?\s*i would include your name here if applicable\s*\]?$",
        r"^\[?\s*insert your name\s*\]?$",
    )
    while lines and not lines[-1].strip():
        lines.pop()
    while lines and any(
        re.match(pattern, lines[-1].strip(), flags=re.IGNORECASE)
        for pattern in placeholder_patterns
    ):
        lines.pop()
        while lines and not lines[-1].strip():
            lines.pop()
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return _normalize_owner_signature_in_draft(cleaned.strip())


def _looks_draft_failure(draft_text, email_data, reply_plan=None):
    """Detect placeholder or low-quality drafts that should be replaced."""
    # Keep this rule here so the behavior stays consistent.
    cleaned = _compact_text(draft_text)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    if len(cleaned) < DRAFT_MIN_CHARS:
        return True
    if _looks_actionable(email_data) and len(cleaned) < DRAFT_MIN_ACTIONABLE_CHARS:
        return True
    if any(marker in lowered for marker in DRAFT_FAILURE_MARKERS):
        return True
    intro = lowered[:240]
    if any(marker in intro for marker in DRAFT_OBSERVER_MARKERS):
        return True
    if any(marker in lowered for marker in ("the user", "the recipient", "mailbox owner")):
        return True
    if _is_noise_fragment(lowered):
        return True

    title = _compact_text(email_data.get("title") or "")
    if _is_near_subject_copy(cleaned, title) and len(cleaned) < 150:
        return True

    copy_metrics = _draft_copy_metrics(draft_text, email_data)
    source = copy_metrics["source_text"]
    if source:
        source_lower = source.lower()
        if len(cleaned) >= 80 and cleaned.lower() in source_lower:
            return True
        if len(cleaned) >= 100 and _token_overlap_ratio(cleaned, source) > 0.95:
            draft_tokens = set(_text_tokens(cleaned))
            source_tokens = set(_text_tokens(source))
            novel_tokens = draft_tokens - source_tokens
            if len(novel_tokens) <= 2:
                return True
        if copy_metrics["first_sentence_copied"]:
            return True
        if copy_metrics["shared_long_phrases"] >= 5 and not any(
            marker in lowered for marker in DIRECT_REPLY_MARKERS
        ):
            return True
        if copy_metrics["copied_ratio"] >= 0.5:
            return True
    if _looks_actionable(email_data) and not _draft_addresses_reply_plan(
        cleaned,
        email_data,
        reply_plan=reply_plan,
    ):
        return True
    return False


def _draft_specificity_metrics(draft_text, email_data, reply_plan=None):
    """Return overlap metrics between a draft and email-specific details."""
    plan = reply_plan or {}
    title = _normalized_header_text(plan.get("topic") or email_data.get("title") or "")
    request_sentence = _compact_text(plan.get("sender_request")) or (_first_request_sentence(email_data) or "")
    detail_sentences = list(plan.get("key_details") or ())
    if not detail_sentences:
        detail_sentences = _extract_key_sentences(_email_body_text(email_data), max_sentences=4)
    token_exclusions = {
        "thanks",
        "thank",
        "hello",
        "regards",
        "best",
        "dear",
        "hi",
        "please",
        "email",
        "message",
    }

    def _filtered_tokens(value):
        return {
            token
            for token in _content_tokens(value)
            if len(token) >= 4 and token not in token_exclusions
        }

    draft_tokens = _filtered_tokens(draft_text)
    title_tokens = _filtered_tokens(title)
    request_tokens = _filtered_tokens(request_sentence)
    detail_tokens = set()
    for sentence in detail_sentences:
        detail_tokens.update(_filtered_tokens(sentence))
    reference_tokens = set(title_tokens) | set(request_tokens) | set(detail_tokens)
    return {
        "draft_tokens": draft_tokens,
        "title_overlap": len(draft_tokens & title_tokens),
        "request_overlap": len(draft_tokens & request_tokens),
        "detail_overlap": len(draft_tokens & reference_tokens),
    }


def _looks_generic_draft(draft_text, email_data, reply_plan=None):
    """Return True when a draft is overly generic for the source email."""
    cleaned = _compact_text(draft_text)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    has_generic_marker = any(marker in lowered for marker in DRAFT_GENERIC_MARKERS)
    metrics = _draft_specificity_metrics(cleaned, email_data, reply_plan=reply_plan)
    actionable = _looks_actionable(email_data)

    if actionable:
        if has_generic_marker and metrics["request_overlap"] < 2:
            return True
        if any(
            marker in lowered
            for marker in (
                "follow up shortly",
                "get back to you",
                "send you a specific response",
                "send a complete response",
            )
        ) and metrics["request_overlap"] < 3:
            return True
        if len(cleaned) < 220 and metrics["detail_overlap"] < 2:
            return True
        return False

    if has_generic_marker and metrics["detail_overlap"] < 2:
        return True
    return False


def _rewrite_generic_draft(
    draft_text,
    email_data,
    reply_plan=None,
    to_value="",
    cc_value="",
    email_id=None,
):
    """Rewrite a weak draft into a more concrete, email-specific reply."""
    candidate = str(draft_text or "").strip()
    if not candidate:
        return None
    plan = reply_plan or _extract_reply_plan(email_data, email_id=email_id)

    system_prompt = (
        "You rewrite weak email replies into strong, send-ready replies grounded in the incoming email. "
        "Write as the mailbox owner (use I/we), not as an observer. "
        "Keep only details supported by the reply plan. "
        "If the sender asks for something, address that request directly and mention concrete details from the plan "
        "such as the topic, deliverable, date, deadline, question, or requested action. "
        "Avoid filler like 'thanks for the update' or 'I'll get back to you later' unless there is no better "
        "grounded response. "
        "If exact facts needed for a full answer are missing, give the most plausible professional next step and ask "
        "at most one focused clarifying question. "
        "Do not invent the mailbox owner's name, title, or signature. "
        "If a mailbox owner display name is provided in the context, use that exact name in any personal sign-off "
        "and never use the mailbox owner's email address as a person name. "
        "Return only the email body text with greeting and closing, no markdown and no subject."
    )
    user_prompt = (
        "Rewrite this weak draft so it directly responds to the email.\n"
        "Write from the structured plan below, not from the sender's original wording.\n\n"
        f"Weak draft:\n{candidate}\n\n"
        f"{_reply_guidance_block(email_data, reply_plan=plan)}\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Make it specific, plausible, and ready to send."
    )
    response_text = _call_ollama(
        task="draft_rewrite",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.25,
        num_predict=_num_predict_for_task("draft_rewrite", 260),
    )
    if not response_text:
        return None

    cleaned = _sanitize_reply_output(response_text)
    if _looks_draft_failure(cleaned, email_data, reply_plan=plan):
        return None
    if _looks_generic_draft(cleaned, email_data, reply_plan=plan):
        return None
    return cleaned


def _drafts_too_similar(original_text, revised_text):
    """Return True when revised draft is effectively unchanged."""
    # Keep revise behavior useful by detecting near-identical outputs.
    original = _compact_text(original_text).lower()
    revised = _compact_text(revised_text).lower()
    if not original or not revised:
        return False
    if original == revised:
        return True
    overlap = _token_overlap_ratio(original, revised)
    length_delta = abs(len(original) - len(revised))
    if len(original) <= 120 and overlap >= 0.92 and length_delta <= 20:
        return True
    if len(original) > 120 and overlap >= 0.96 and length_delta <= 40:
        return True
    return False


def _brief_reply_intent(text):
    """Return coarse intent label for very short user-entered draft instructions."""
    normalized = _compact_text(text).lower()
    if not normalized:
        return None
    if any(
        marker in normalized
        for marker in ("wrong email", "wrong person", "wrong recipient", "remove me", "unsubscribe me")
    ):
        return "wrong_recipient"
    if normalized in {"no", "nope", "nah", "not interested"} or any(
        marker in normalized for marker in ("don't", "do not", "can't", "cannot", "won't", "no thanks")
    ):
        return "decline"
    if normalized in {"yes", "yep", "sure", "ok", "okay"} or any(
        marker in normalized for marker in ("sounds good", "works for me", "that works", "i can do", "confirmed")
    ):
        return "accept"
    if any(marker in normalized for marker in ("thanks", "thank you", "thx")):
        return "thanks"
    return None


def _is_brief_reply_instruction(text):
    """Return True when the current draft is really a short instruction/stub."""
    normalized = _compact_text(text)
    tokens = _text_tokens(normalized)
    return bool(normalized) and (len(tokens) <= 5 or len(normalized) <= 28)


def _draft_matches_brief_intent(original_text, revised_text):
    """Return True when a rewritten reply preserves a short user instruction's stance."""
    intent = _brief_reply_intent(original_text)
    lowered = _compact_text(revised_text).lower()
    if not intent or not lowered:
        return False
    if intent == "wrong_recipient":
        return any(
            marker in lowered
            for marker in ("wrong email", "wrong person", "wrong recipient", "remove me", "not the right contact")
        )
    if intent == "decline":
        return any(
            marker in lowered
            for marker in ("can't", "cannot", "won't", "no thanks", "not interested", "doesn't work")
        )
    if intent == "accept":
        return any(
            marker in lowered
            for marker in ("works for me", "sounds good", "confirmed", "happy to", "yes")
        )
    if intent == "thanks":
        return any(marker in lowered for marker in ("thanks", "thank you", "appreciate"))
    return False


def _extract_deadline_phrase(text):
    """Return a short deadline/timing phrase when one is clearly present."""
    normalized = _compact_text(text)
    if not normalized:
        return ""
    patterns = (
        r"\bby [^.?!,;]+",
        r"\bbefore [^.?!,;]+",
        r"\b(?:today|tomorrow|tonight|this week|next week|end of day|eod)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        phrase = _compact_text(match.group(0)).strip(" .,:;")
        phrase = re.sub(r"\s+(?:so|because|and)\b.*$", "", phrase, flags=re.IGNORECASE)
        if phrase:
            return phrase
    return ""


def _expand_brief_reply_instruction(email_data, current_draft_text):
    """Expand short reply instructions into a send-ready draft."""
    intent = _brief_reply_intent(current_draft_text)
    if not intent:
        return None

    sender_name = _sender_display_name(email_data.get("sender"))
    greeting = f"Hi {sender_name}," if sender_name else "Hi,"
    title = _summary_title_topic(email_data.get("title") or "")
    request_sentence = _compact_text(_first_request_sentence(email_data) or "")
    deadline = _extract_deadline_phrase(
        request_sentence or _body_for_context(email_data, max_chars=600, max_sentences=4)
    )
    actionable = _looks_actionable(email_data)

    if intent == "wrong_recipient":
        lines = [
            "I think this reached the wrong person.",
            "Please remove me from this thread.",
        ]
    elif intent == "decline":
        if actionable:
            if deadline:
                lines = [
                    f"That won't work for me. I won't be able to take care of this {deadline}.",
                    "Please plan without me for now.",
                ]
            elif title:
                lines = [
                    f"That won't work for me. I won't be able to take care of the request about {title}.",
                    "Please plan without me for now.",
                ]
            else:
                lines = [
                    "That won't work for me. I won't be able to help with that request.",
                    "Please plan without me for now.",
                ]
        else:
            lines = ["No thanks."]
            if _looks_bulk_or_newsletter(email_data):
                lines.append("Please remove me from this list.")
    elif intent == "accept":
        if actionable:
            if deadline:
                lines = [
                    f"Sounds good. I'll take care of this {deadline}.",
                    "I'll let you know right away if anything changes.",
                ]
            elif title:
                lines = [
                    f"Sounds good. I'll take care of the request about {title}.",
                    "I'll let you know right away if anything changes.",
                ]
            else:
                lines = [
                    "Sounds good. I'll take care of it.",
                    "I'll let you know right away if anything changes.",
                ]
        else:
            lines = [f"Sounds good. Thanks for the update about {title}." if title else "Sounds good. Thanks for the update."]
    elif intent == "thanks":
        lines = [f"Thanks for the update about {title}." if title else "Thanks for the update."]
        lines.append("I appreciate it.")
    else:
        return None

    body = " ".join(
        _ensure_sentence_ending(line)
        for line in lines
        if _compact_text(line)
    )
    if not body:
        return None
    return f"{greeting}\n\n{body}\n\n{_reply_closing_text()}"


def _looks_like_reply_greeting(line_text):
    """Return True when a line looks like an email greeting."""
    normalized = _compact_text(line_text)
    if not normalized:
        return False
    return bool(
        re.match(
            r"^(?:hi|hello|dear|good\s+(?:morning|afternoon|evening))\b",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def _looks_like_reply_closing(line_text):
    """Return True when a line looks like an email sign-off."""
    normalized = _compact_text(line_text).lower().rstrip(",")
    return normalized in {
        "best",
        "best regards",
        "regards",
        "kind regards",
        "sincerely",
        "thanks",
        "thank you",
        "cheers",
    }


def _reply_core_body(reply_text):
    """Return reply content without obvious greeting/sign-off wrapper lines."""
    lines = [
        line.strip()
        for line in str(reply_text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
        if line.strip()
    ]
    if not lines:
        return ""
    if _looks_like_reply_greeting(lines[0]):
        lines = lines[1:]
    if lines and _looks_like_reply_closing(lines[-1]):
        lines = lines[:-1]
        if lines and len(_text_tokens(lines[-1])) <= 3 and not re.search(r"[.!?]$", lines[-1]):
            lines = lines[:-1]
    return _compact_text(" ".join(lines))


def _revise_reply_fallback(email_data, current_draft_text, reply_plan=None):
    """Build a deterministic revised reply when the model is unavailable or unusable."""
    expanded = _expand_brief_reply_instruction(email_data, current_draft_text)
    if expanded:
        return expanded

    plan = reply_plan or _extract_reply_plan(email_data)
    core_body = _reply_core_body(current_draft_text) or _compact_text(current_draft_text)
    if not core_body:
        return None

    sender_name = _compact_text(plan.get("sender_name")) or _sender_display_name(email_data.get("sender"))
    greeting = f"Hi {sender_name}," if sender_name else "Hi,"
    title = _compact_text(plan.get("topic")) or _summary_title_topic(email_data.get("title") or "")
    request_sentence = _compact_text(plan.get("sender_request"))
    key_sentences = list(plan.get("key_details") or ())
    reference_detail = request_sentence or (key_sentences[0] if key_sentences else "") or title
    detail_metrics = _draft_specificity_metrics(core_body, email_data, reply_plan=plan)
    actionable = _looks_actionable(email_data)
    bulk_like = _looks_bulk_or_newsletter(email_data)
    deadline = _compact_text(plan.get("deadline")) or _extract_deadline_phrase(
        request_sentence or _body_for_context(email_data, max_chars=600, max_sentences=4)
    )
    body_lower = core_body.lower()
    body_parts = []

    def _append_unique(text):
        normalized = _ensure_sentence_ending(text)
        if not normalized:
            return
        for existing in body_parts:
            if _token_overlap_ratio(existing, normalized) > 0.92:
                return
        body_parts.append(normalized)

    if actionable:
        if title and "thanks for your email about" not in body_lower and detail_metrics["detail_overlap"] < 2:
            _append_unique(f"Thanks for your email about {title}")
    else:
        if (
            title
            and not any(
                marker in body_lower
                for marker in (
                    "thanks for the update",
                    "thanks for sharing",
                    "thanks for your email about",
                    "thanks for your note",
                )
            )
        ):
            _append_unique(f"Thanks for the update about {title}")

    _append_unique(core_body)

    if actionable and reference_detail and detail_metrics["request_overlap"] < 2:
        _append_unique(f"I'm following up on {reference_detail}")
    elif (
        not actionable
        and not bulk_like
        and reference_detail
        and detail_metrics["detail_overlap"] < 1
    ):
        _append_unique(f"I appreciate the note about {reference_detail}")

    if actionable and len(_text_tokens(core_body)) <= 18:
        if deadline and "i'll" not in body_lower and "i will" not in body_lower:
            _append_unique(f"I'll take care of this {deadline}")
        elif not any(
            marker in body_lower
            for marker in ("i'll", "i will", "i can", "i am", "i'm", "let me know", "?")
        ):
            _append_unique("Please let me know if there is a specific deadline or format you want me to use")

    body_text = "\n\n".join(body_parts).strip()
    if not body_text:
        return None
    return f"{greeting}\n\n{body_text}\n\n{_reply_closing_text()}"


def _draft_preserves_user_context(original_text, revised_text):
    """Return True when revised draft still reflects user-provided context."""
    # Keep revise behavior anchored to whatever is already in the user's reply box.
    original = _compact_text(original_text).lower()
    revised = _compact_text(revised_text).lower()
    if not original or not revised:
        return False
    if original in revised:
        return True

    original_tokens = set(_text_tokens(original))
    revised_tokens = set(_text_tokens(revised))
    if not original_tokens or not revised_tokens:
        return False
    overlap = len(original_tokens & revised_tokens) / float(len(original_tokens))

    # Short user drafts need lighter overlap checks; long drafts should keep more of their substance.
    if len(original_tokens) <= 12:
        return overlap >= 0.24
    return overlap >= 0.34


def can_generate_reply_draft(email_data, current_draft_text=""):
    """Return True when AI should generate or revise a reply draft."""
    if str(current_draft_text or "").strip():
        return True
    if not isinstance(email_data, dict):
        return False

    email_type = str(email_data.get("type") or "").strip().lower()
    if email_type == "response-needed":
        return True

    ai_needs_response = email_data.get("ai_needs_response")
    if ai_needs_response is not None:
        return bool(ai_needs_response)

    if str(email_data.get("ai_category") or "").strip():
        return False

    return _looks_actionable(email_data)


def draft_reply(email_data, to_value="", cc_value="", email_id=None):
    """Draft reply.
    """
    # Shared draft-reply helper for the reply and draft flow.
    body = _email_body_text(email_data).strip()
    title = _compact_text(email_data.get("title") or "(No subject)")
    if not body and not title:
        return None
    reply_plan = _extract_reply_plan(email_data, email_id=email_id)

    # Generate a complete send-ready draft; the fallback template keeps the UX working on failure.
    system_prompt = (
        "You write send-ready email replies from a structured reply plan. "
        "Write as the mailbox owner using I/we and never as an observer or 'the user'. "
        "Return only the email body with greeting and closing, no markdown and no subject. "
        "Start directly with the reply itself. "
        "Address the sender's request directly when possible and use at least one concrete plan detail in the body. "
        "Avoid generic filler, summaries, or commentary about the draft. "
        "If exact facts are missing, give the safest plausible next step and ask at most one focused clarifying question. "
        "Do not invent the mailbox owner's name, title, or signature. "
        "If a mailbox owner display name is provided in the context, use that exact name in any personal sign-off "
        "and never use the mailbox owner's email address as a person name. "
        "If no name is provided, use a generic closing. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Draft a complete response email from the structured reply plan below.\n"
        "Do not reuse long phrases from the sender's email.\n\n"
        f"{_reply_guidance_block(email_data, reply_plan=reply_plan)}\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Keep it clear, specific, and ready to send."
    )
    response_text = _call_ollama(
        task="draft",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.2,
        num_predict=_num_predict_for_task("draft", 280),
    )
    if not response_text:
        fallback_draft = _draft_reply_fallback(email_data, reply_plan=reply_plan)
        _log_action(
            task="draft",
            status="fallback",
            email_id=email_id,
            detail="using_contextual_fallback_no_model_response",
        )
        return fallback_draft

    cleaned = _sanitize_reply_output(response_text)
    if _looks_generic_draft(cleaned, email_data, reply_plan=reply_plan):
        rewritten = _rewrite_generic_draft(
            cleaned,
            email_data=email_data,
            reply_plan=reply_plan,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        if rewritten:
            return rewritten
    if _looks_draft_failure(cleaned, email_data, reply_plan=reply_plan):
        fallback_draft = _draft_reply_fallback(email_data, reply_plan=reply_plan)
        _log_action(
            task="draft",
            status="fallback",
            email_id=email_id,
            detail="using_contextual_fallback_unusable_model_output",
        )
        return fallback_draft
    return cleaned or _draft_reply_fallback(email_data, reply_plan=reply_plan)


def revise_reply(
    email_data,
    current_draft_text,
    to_value="",
    cc_value="",
    email_id=None,
):
    """Revise reply.
    """
    # Shared revise-reply helper for the reply and draft flow.
    current_draft_text = str(current_draft_text or "").strip()
    if not current_draft_text:
        return None
    reply_plan = _extract_reply_plan(email_data, email_id=email_id)
    fallback_revision = _revise_reply_fallback(
        email_data,
        current_draft_text,
        reply_plan=reply_plan,
    )
    current_draft_is_weak = _looks_draft_failure(
        current_draft_text,
        email_data,
        reply_plan=reply_plan,
    ) or _looks_generic_draft(current_draft_text, email_data, reply_plan=reply_plan)

    # Preserve the user's intent while improving clarity and completeness against the source context.
    system_prompt = (
        "You revise email drafts using a structured reply plan. "
        "Write as the mailbox owner using I/we and never as an observer or 'the user'. "
        "Return only the revised send-ready email with greeting and closing, no markdown and no subject. "
        "Keep the current draft's intent, constraints, commitments, and questions. "
        "If the current draft is a short instruction like 'nope', 'sounds good', 'thanks', or 'wrong email', "
        "expand it into a complete reply with the same stance. "
        "Replace weak wording with a direct answer and use at least one concrete plan detail. "
        "Do not invent the mailbox owner's name, title, or signature. "
        "If a mailbox owner display name is provided in the context, use that exact name in any personal sign-off "
        "and never use the mailbox owner's email address as a person name. "
        "If no name is provided, use a generic closing. "
        "Treat plain person names as names, not email addresses."
    )
    user_prompt = (
        "Revise this draft response using the structured reply plan below.\n"
        "Keep the user's intent, but replace weak or generic wording with a direct answer.\n\n"
        f"{_reply_guidance_block(email_data, reply_plan=reply_plan)}\n"
        f"Reply To: {to_value}\n"
        f"Reply Cc: {cc_value}\n"
        "Current draft (between markers):\n"
        "---BEGIN CURRENT DRAFT---\n"
        f"{current_draft_text}\n"
        "---END CURRENT DRAFT---\n\n"
        "Keep intent, improve clarity, and make it complete enough to send. "
        "When the current draft is very short, treat it as instructions for the reply rather than final wording."
    )
    response_text = _call_ollama(
        task="revise",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        email_id=email_id,
        temperature=0.1,
        num_predict=_num_predict_for_task("revise", 280),
    )
    if not response_text:
        # Prefer a deterministic local rewrite over doing nothing when the model is down.
        _log_action(
            task="revise",
            status="fallback",
            email_id=email_id,
            detail="using_local_revision_fallback_no_model_response",
        )
        return fallback_revision or current_draft_text
    cleaned = _sanitize_reply_output(response_text)
    if _looks_generic_draft(cleaned, email_data, reply_plan=reply_plan):
        rewritten = _rewrite_generic_draft(
            cleaned,
            email_data=email_data,
            reply_plan=reply_plan,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        if rewritten and not _drafts_too_similar(current_draft_text, rewritten):
            cleaned = rewritten
    if _looks_draft_failure(cleaned, email_data, reply_plan=reply_plan):
        # Recover from unusable revisions by forcing one fresh draft-generation pass.
        regenerated = draft_reply(
            email_data=email_data,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
        regenerated = str(regenerated or "").strip()
        if (
            regenerated
            and not _drafts_too_similar(current_draft_text, regenerated)
            and (
                current_draft_is_weak
                or _draft_preserves_user_context(current_draft_text, regenerated)
            )
            and not _looks_generic_draft(regenerated, email_data, reply_plan=reply_plan)
        ):
            return regenerated
        _log_action(
            task="revise",
            status="fallback",
            email_id=email_id,
            detail="using_local_revision_fallback_unusable_model_output",
        )
        return fallback_revision or current_draft_text
    if not _draft_preserves_user_context(current_draft_text, cleaned):
        if _is_brief_reply_instruction(current_draft_text) and _draft_matches_brief_intent(
            current_draft_text,
            cleaned,
        ):
            return cleaned or current_draft_text
        if current_draft_is_weak and _draft_addresses_reply_plan(
            cleaned,
            email_data,
            reply_plan=reply_plan,
        ):
            return cleaned or fallback_revision or current_draft_text
        # Never replace user-provided context with unrelated model output.
        if fallback_revision and _draft_preserves_user_context(current_draft_text, fallback_revision):
            return fallback_revision
        return current_draft_text
    if _drafts_too_similar(current_draft_text, cleaned):
        if fallback_revision and not _drafts_too_similar(current_draft_text, fallback_revision):
            _log_action(
                task="revise",
                status="fallback",
                email_id=email_id,
                detail="using_local_revision_fallback_near_identical_model_output",
            )
            return fallback_revision
        return cleaned or current_draft_text
    return cleaned or current_draft_text


def generate_reply_draft(
    email_data,
    to_value="",
    cc_value="",
    current_draft_text="",
    current_reply_text="",
    email_id=None,
):
    """Generate reply draft.
    """
    # Backward-compatible adapter for older AI client callers.
    # Support both the old and new parameter names.
    if not current_draft_text and current_reply_text:
        current_draft_text = current_reply_text
    current_draft_text = str(current_draft_text or "").strip()
    if not can_generate_reply_draft(email_data, current_draft_text=current_draft_text):
        return current_draft_text
    if current_draft_text:
        return revise_reply(
            email_data=email_data,
            current_draft_text=current_draft_text,
            to_value=to_value,
            cc_value=cc_value,
            email_id=email_id,
        )
    return draft_reply(
        email_data=email_data,
        to_value=to_value,
        cc_value=cc_value,
        email_id=email_id,
    )


def summary_looks_unusable(email_data):
    """Check if current summary is likely placeholder/noise text."""
    raw_summary = str(email_data.get("summary") or "")
    summary = " ".join(raw_summary.split()).lower().strip()
    if not summary:
        return False
    bad_phrases = (
        "summary unavailable",
        "summary generation failed",
        "unable to summarize",
        "view in browser",
    )
    for phrase in bad_phrases:
        if phrase in summary:
            return True
    if _looks_utility_sentence(raw_summary):
        return True
    return _looks_summary_failure(raw_summary)


def should_auto_analyze_email(email_data, non_main_types=frozenset({"sent", "draft"})):
    """Return True when classify/summary work should run automatically."""
    if not ai_enabled() or not email_data:
        return False
    if email_data.get("type") in non_main_types:
        return False
    if bool(email_data.get("is_archived")):
        return False
    if not _email_body_text(email_data).strip():
        return False

    # Run this when triage fields or the long-email summary are missing.
    missing_classification = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )
    needs_summary = should_summarize_email(email_data) and (
        not str(email_data.get("summary") or "").strip()
        or summary_looks_unusable(email_data)
    )
    return missing_classification or needs_summary


def run_ai_analysis(email_data, force=False):
    """Run classification + summary, then save updated fields."""
    started_at = time.perf_counter()
    changed = False
    missing_classification = (
        not str(email_data.get("ai_category") or "").strip()
        or email_data.get("ai_needs_response") is None
        or email_data.get("ai_confidence") is None
    )

    # Save classification and summary independently so partial success still counts.
    if force or missing_classification:
        classification = classify_email(email_data, email_id=email_data.get("id"))
        if classification:
            update_email_ai_fields(
                email_id=email_data["id"],
                email_type=classification_to_email_type(classification),
                priority=classification.get("priority"),
                ai_category=classification.get("category"),
                ai_needs_response=classification.get("needs_response"),
                ai_confidence=classification.get("confidence"),
                lock_existing_classification=not force,
            )
            changed = True

    missing_summary = not str(email_data.get("summary") or "").strip()
    should_make_summary = should_summarize_email(email_data) and (
        force or missing_summary or summary_looks_unusable(email_data)
    )
    if should_make_summary:
        summary = summarize_email(email_data, email_id=email_data.get("id"))
        if summary:
            update_email_ai_fields(email_id=email_data["id"], summary=summary)
            changed = True
    _log_performance(
        "analysis_total",
        int((time.perf_counter() - started_at) * 1000),
        email_id=email_data.get("id"),
        force=int(bool(force)),
        classification_attempted=int(bool(force or missing_classification)),
        summary_attempted=int(bool(should_make_summary)),
        changed=int(bool(changed)),
    )
    return changed


AI_TASK_MAX_ITEMS = 200
AI_TASK_ACTIVE_STATUSES = {"queued", "running"}
# In-memory async task registry for polling endpoints; cleanup keeps it bounded.
AI_TASKS = {}
AI_TASK_INDEX = {}
AI_TASK_LOCK = threading.Lock()


def _cleanup_tasks_locked():
    """Remove older completed tasks if in-memory cache grows too large."""
    if len(AI_TASKS) <= AI_TASK_MAX_ITEMS:
        return

    # Keep only recent tasks in memory so this list does not grow forever.
    done_tasks = [
        task for task in AI_TASKS.values() if task.get("status") not in AI_TASK_ACTIVE_STATUSES
    ]
    # Drop the oldest completed items first and leave active tasks alone.
    done_tasks.sort(key=lambda task: float(task.get("created_at") or 0))
    while len(AI_TASKS) > AI_TASK_MAX_ITEMS and done_tasks:
        task = done_tasks.pop(0)
        task_id = task["id"]
        key = (task["type"], task["email_id"], bool(task.get("force")))
        if AI_TASK_INDEX.get(key) == task_id:
            AI_TASK_INDEX.pop(key, None)
        AI_TASKS.pop(task_id, None)


def _create_or_get_ai_task(task_type, email_id, force=False):
    """Create task unless active task already exists for same type+email."""
    key = (task_type, int(email_id), bool(force))
    with AI_TASK_LOCK:
        existing_id = AI_TASK_INDEX.get(key)
        existing_task = AI_TASKS.get(existing_id) if existing_id else None
        # Reuse an active task so the frontend sees one task id per action.
        if existing_task and existing_task.get("status") in AI_TASK_ACTIVE_STATUSES:
            return dict(existing_task), False

        now = time.time()
        task = {
            "id": uuid4().hex,
            "type": task_type,
            "email_id": int(email_id),
            "status": "queued",
            "result": None,
            "error": None,
            "force": bool(force),
            "created_at": now,
            "updated_at": now,
        }
        AI_TASKS[task["id"]] = task
        AI_TASK_INDEX[key] = task["id"]
        _cleanup_tasks_locked()
        return dict(task), True


def _set_ai_task_status(task_id, status, result=None, error=None):
    """Update task status and optional payload fields."""
    with AI_TASK_LOCK:
        task = AI_TASKS.get(task_id)
        if not task:
            return
        task["status"] = status
        task["updated_at"] = time.time()
        if result is not None:
            task["result"] = result
        if error is not None:
            task["error"] = error
        if status not in AI_TASK_ACTIVE_STATUSES:
            # Release the dedupe slot so the next task of this type can be scheduled.
            key = (task["type"], task["email_id"], bool(task.get("force")))
            if AI_TASK_INDEX.get(key) == task_id:
                AI_TASK_INDEX.pop(key, None)


def get_ai_task(task_id):
    """Return task snapshot dict by id."""
    with AI_TASK_LOCK:
        task = AI_TASKS.get(task_id)
        return dict(task) if task else None


def serialize_ai_task(task):
    """Return API-friendly task payload."""
    payload = {
        "task_id": task["id"],
        "task_type": task["type"],
        "email_id": task["email_id"],
        "status": task["status"],
    }
    if task.get("result") is not None:
        payload["result"] = task["result"]
    if task.get("error"):
        payload["error"] = task["error"]
    return payload


def _analysis_task_worker(task_id, email_id, force=False):
    """Background worker: run analysis and write task result."""
    _set_ai_task_status(task_id, "running")
    try:
        email_data = fetch_email_by_id(email_id)
        if not email_data:
            raise ValueError("Email not found.")

        # Force mode guarantees a fresh pass when the user explicitly asks for one.
        run_ai_analysis(email_data, force=bool(force))
        refreshed = fetch_email_by_id(email_id) or email_data
        _set_ai_task_status(
            task_id,
            "completed",
            result={
                "summary": refreshed.get("summary"),
                "ai_category": refreshed.get("ai_category"),
                "ai_needs_response": refreshed.get("ai_needs_response"),
                "ai_confidence": refreshed.get("ai_confidence"),
                "priority": refreshed.get("priority"),
                "type": refreshed.get("type"),
            },
        )
    except Exception as exc:
        log_ai_event(
            task="analyze",
            status="error",
            email_id=email_id,
            detail=f"task_exception: {exc}",
        )
        _set_ai_task_status(task_id, "error", error=str(exc))


def _draft_task_worker(task_id, email_id, to_value, cc_value, current_reply_text):
    """Background worker: generate draft and save it."""
    _set_ai_task_status(task_id, "running")
    try:
        email_data = fetch_email_by_id(email_id)
        if not email_data:
            raise ValueError("Email not found.")

        if not can_generate_reply_draft(email_data, current_draft_text=current_reply_text):
            raise ValueError("AI draft is only available for emails that need a response.")

        # Draft generation works with both blank replies and user-edited starting text.
        draft_text = generate_reply_draft(
            email_data=email_data,
            to_value=to_value or "",
            cc_value=cc_value or "",
            current_draft_text=current_reply_text or "",
            email_id=email_id,
        )
        update_draft(email_id, draft_text)
        _set_ai_task_status(task_id, "completed", result={"draft": draft_text})
    except Exception as exc:
        log_ai_event(
            task="draft",
            status="error",
            email_id=email_id,
            detail=f"task_exception: {exc}",
        )
        _set_ai_task_status(task_id, "error", error=str(exc))


def start_analysis_task(email_id, force=False):
    """Create/start analysis task and return its metadata."""
    task, created = _create_or_get_ai_task("analyze", email_id, force=force)
    if created:
        threading.Thread(
            target=_analysis_task_worker,
            args=(task["id"], int(email_id), bool(force)),
            daemon=True,
        ).start()
    return task


def start_draft_task(email_id, to_value, cc_value, current_reply_text):
    """Create/start draft task and return its metadata."""
    task, created = _create_or_get_ai_task("draft", email_id)
    if created:
        threading.Thread(
            target=_draft_task_worker,
            args=(
                task["id"],
                int(email_id),
                to_value or "",
                cc_value or "",
                current_reply_text or "",
            ),
            daemon=True,
        ).start()
    return task
