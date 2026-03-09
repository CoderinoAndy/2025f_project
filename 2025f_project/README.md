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
- `GMAIL_SYNC_INTERVAL_SECONDS`: minimum sync interval (default `20`)
- `GMAIL_SYNC_MAX_RESULTS`: recent messages to pull per sync (default `25`)
- `LIVE_EMAIL_POLL_INTERVAL_MS`: mailbox live-refresh interval in ms (default `2000`)
- `LIVE_EMAIL_SYNC_MAX_RESULTS`: messages fetched by each live-refresh sync (default `15`)
- `LIVE_EMAIL_DEEP_SYNC_INTERVAL_SECONDS`: how often live polling runs a deeper sync pass (default `30`)
- `LIVE_EMAIL_DEEP_SYNC_MAX_RESULTS`: messages fetched by each deeper live sync pass (default `60`)
- `GMAIL_AI_TRIAGE_PER_SYNC`: max newly-synced emails to auto-classify per sync (default `0`)
- `OLLAMA_MODEL`: defaults to `qwen2.5:7b`
- `OLLAMA_CLASSIFY_MODEL`: optional override for classification requests
- `OLLAMA_DRAFT_MODEL`: optional override for draft/revise requests
- `OLLAMA_SUMMARY_MODEL`: optional override for summary requests
- `OLLAMA_API_URL`: defaults to `http://127.0.0.1:11434/api/chat`
- `OLLAMA_TIMEOUT_SECONDS`: base AI request timeout in seconds (default `45`)
- `OLLAMA_LONG_TASK_TIMEOUT_SECONDS`: timeout for draft/revise/summarize requests (default `180`)
- `OLLAMA_SUMMARY_MIN_CHARS`: summary threshold (default `200`)
- `AI_ACTION_LOG_PATH`: path to `.txt` AI action log (default `instance/ai_actions.txt`)
- `APP_DEBUG_LOG_PATH`: path to structured debug log (default `instance/debug_log.txt`)

## Local Ollama mode (no external AI calls)

AI calls are local-only and go to Ollama chat API (`http://127.0.0.1:11434/api/chat`) by default. Non-loopback endpoints are blocked.

1. Start Ollama:
   - `ollama serve`
2. Confirm model is available:
   - `ollama pull qwen2.5:7b`
3. Start Flask app:
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
- AI analysis and draft generation now run in background tasks, so opening an email does not block on model inference.
- Email page shows loading indicators while AI is running and types generated text into the UI when complete.
- Draft generation is restricted to response-needed emails.
- Every AI call/error is appended to `instance/ai_actions.txt` (or `AI_ACTION_LOG_PATH`).
- Summary panel has a manual re-analyze button (`stars` icon).
- Archive is local-only: archived emails are hidden from main mailboxes and moved to Archive without deleting from Gmail.
