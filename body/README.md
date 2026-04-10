# 2025f_project

Flask email assistant with Gmail sync into SQLite.

## Setup

Run commands from repo root (`C:\Users\ycshk\2025f_project`), not inside the nested app folder.

1. Create/update the virtualenv and install dependencies:
   - `make install`
2. Place your Google OAuth desktop client file at:
   - `2025f_project/credentials.json`
   - or set `GMAIL_CREDENTIALS_FILE` to a custom path.
3. Run the app:
   - `make run`
4. On first Gmail-backed request, OAuth opens a browser flow and stores a token at:
   - `instance/gmail_token.json`

Manual fallback without `make` (Windows):

- `python -m venv venv`
- `venv\Scripts\python.exe -m pip install -r 2025f_project\requirements.txt`
- `cd 2025f_project`
- `..\venv\Scripts\python.exe run.py`

## Gmail behavior

- App imports recent Gmail messages automatically into `instance/app.sqlite`.
- Auto-sync runs in a background thread so pages stay responsive.
- Request-triggered Gmail sync is throttled so mailbox navigation and email open views do less background work.
- Read/unread state is mirrored with Gmail labels (`UNREAD`).
- Junk/inbox moves are mirrored with Gmail labels (`SPAM`/`INBOX`).
- Sending a reply uses Gmail API when the source email has a Gmail `external_id`.
- Sent mail is stored as type `sent` and shown only in the `Sent` tab.
- Drafts are synced from Gmail Drafts and shown in the `Drafts` tab.
- Compose supports attachments and saves drafts to Gmail/local DB.
- If Gmail is unavailable, the app falls back to local SQLite-only behavior.
- Structured debug logging writes to `instance/debug_log.txt` with fields such as timestamp, action type, action, status, and details.

## Optional env vars

- `GMAIL_CREDENTIALS_FILE`: custom path to `credentials.json`
- `GMAIL_SYNC_INTERVAL_SECONDS`: minimum background sync interval (default `10`)
- `GMAIL_SYNC_MAX_RESULTS`: recent messages to pull per sync (default `15`)
- `GMAIL_BEFORE_REQUEST_SYNC_INTERVAL_SECONDS`: throttle for request-triggered sync on mailbox pages (default `120`)
- `GMAIL_BEFORE_REQUEST_SYNC_MAX_RESULTS`: messages fetched by request-triggered sync (default `10`)
- `LIVE_EMAIL_POLL_INTERVAL_MS`: mailbox live-refresh interval in ms (default `10000`)
- `LIVE_EMAIL_SYNC_MAX_RESULTS`: messages fetched by each live-refresh sync (default `8`)
- `LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS`: how often live polling runs a deeper sync pass (default `120`)
- `LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS`: messages fetched by each deeper live sync pass (default `30`)
- `GMAIL_AI_TRIAGE_PER_SYNC`: max newly-synced emails to auto-classify per sync (default `0`)
- `OLLAMA_MODEL`: shared fallback model for tasks without a task-specific default
- `OLLAMA_CLASSIFY_MODEL`: classification model (default `qwen2.5:7b-instruct`)
- `OLLAMA_DRAFT_MODEL`: draft/revise model (default `mistral-small3.2:24b`)
- `OLLAMA_SUMMARY_MODEL`: summary model (default `mistral-small3.2:24b`)
- `OLLAMA_CLASSIFY_NUM_PREDICT`: token budget for classification calls (default `96`)
- `OLLAMA_DRAFT_NUM_PREDICT`: token budget for draft/revise calls
- `OLLAMA_SUMMARY_NUM_PREDICT`: token budget for summary calls
- `OLLAMA_STRICT_MODEL_RESOLUTION`: when truthy, missing requested models fail instead of silently substituting
- `OLLAMA_API_URL`: defaults to `http://127.0.0.1:11434/api/chat`
- `OLLAMA_TIMEOUT_SECONDS`: base AI request timeout in seconds (default `45`)
- `OLLAMA_LONG_TASK_TIMEOUT_SECONDS`: timeout for draft/revise/summarize requests (default `180`)
- `OLLAMA_SUMMARY_MIN_CHARS`: summary threshold (default `200`)
- `OLLAMA_VISUAL_SUMMARY_ENABLED`: allow summary escalation to Playwright screenshots (default `1`)
- `OLLAMA_VISUAL_SUMMARY_TEXT_FAILURE_CHARS`: text length below this counts as failed extraction (default `120`)
- `OLLAMA_VISUAL_SUMMARY_MIN_HTML_CHARS`: minimum HTML-derived text size before visual fallback is considered (default `180`)
- `OLLAMA_VISUAL_SUMMARY_COMPLEXITY_THRESHOLD`: layout-signal count required before a visual summary is considered (default `2`)
- `OLLAMA_VISUAL_SUMMARY_TEXT_OVERLAP_THRESHOLD`: HTML/text overlap below this can trigger visual escalation (default `0.72`)
- `OLLAMA_VISION_MAX_CHARS`: max cleaned body chars included in the text prompt (default `6000`)
- `OLLAMA_VISION_MAX_PAGES`: max real HTML screenshot pages attached to a model call (default `2`)
- `OLLAMA_VISION_PAGE_HEIGHT`: max pixel height per attached screenshot page (default `1800`)
- `OLLAMA_VISION_VIEWPORT_WIDTH`: browser viewport width for rendered HTML screenshots (default `1365`)
- `OLLAMA_VISION_BROWSER_TIMEOUT_SECONDS`: timeout for HTML screenshot rendering (default `12`)
- `OLLAMA_VISION_BROWSER_WAIT_MS`: post-load settle delay before screenshot capture (default `750`)
- `OLLAMA_VISION_BROWSER_CHANNEL`: optional Playwright browser channel such as `msedge` or `chrome`
- `OLLAMA_VISION_BROWSER_EXECUTABLE_PATH`: optional explicit browser executable path for Playwright
- `AI_ACTION_LOG_PATH`: path to `.txt` AI action log (default `instance/ai_actions.txt`)
- `APP_DEBUG_LOG_PATH`: path to structured debug log (default `instance/debug_log.txt`)

## Local Ollama mode (no external AI calls)

AI calls are local-only and go to Ollama chat API (`http://127.0.0.1:11434/api/chat`) by default. Non-loopback endpoints are blocked.

1. Start Ollama:
   - `ollama serve`
2. Confirm the main models are available:
   - `ollama pull qwen2.5:7b-instruct`
   - `ollama pull mistral-small3.2:24b`
3. Optional: enable real HTML screenshots for visually complex emails:
   - `pip install -r requirements.txt`
   - `playwright install chromium`
4. Start Flask app:
   - `make run`

## Local frontend assets

- Bootstrap and Bootstrap Icons are vendored under `app/static/vendor/`.
- Templates use local `url_for('static', ...)` paths instead of CDN URLs.
- This removes frontend CDN dependency, so UI assets work without internet.

## Ollama capabilities wired in

- Lightbulb icon in email reply calls Ollama to draft or revise a reply.
- Opening an email without a summary auto-triggers AI analysis for:
  - classification JSON (`category`, `needs_response`, `priority`, `confidence`)
  - summary generation only for long emails
  - category mapping into app type (`response-needed`, `read-only`, `junk`)
- Classification now defaults to the faster `qwen2.5:7b-instruct` model and stays text-only.
- Summary generation is text-first and only escalates to Playwright screenshots when text extraction clearly fails or the HTML layout materially changes the message.
- AI analysis and draft generation now run in background tasks, so opening an email does not block on model inference.
- Email page shows loading indicators while AI is running and types generated text into the UI when complete.
- Draft generation is restricted to response-needed emails.
- Every AI call/error is appended to `instance/ai_actions.txt` (or `AI_ACTION_LOG_PATH`).
- Timing logs now include Gmail sync, Playwright render, classification, summary, and total analysis durations.
- Summary panel has a manual re-analyze button (`stars` icon).
- Archive is local-only: archived emails are hidden from main mailboxes and moved to Archive without deleting from Gmail.
