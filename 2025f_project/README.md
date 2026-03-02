# 2025f_project

Flask email assistant with Gmail sync into SQLite.

## Setup

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Place your Google OAuth desktop client file at:
   - `2025f_project/credentials.json`
   - or set `GMAIL_CREDENTIALS_FILE` to a custom path.
3. Run the app:
   - `python run.py`
4. On first Gmail-backed request, OAuth opens a browser flow and stores a token at:
   - `instance/gmail_token.json`

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

## Optional env vars

- `GMAIL_CREDENTIALS_FILE`: custom path to `credentials.json`
- `GMAIL_SYNC_INTERVAL_SECONDS`: minimum sync interval (default `20`)
- `GMAIL_SYNC_MAX_RESULTS`: recent messages to pull per sync (default `25`)
- `OLLAMA_MODEL`: defaults to `qwen2.5:7b`
- `OLLAMA_API_URL`: defaults to `http://localhost:11434/api/chat`
- `OLLAMA_TIMEOUT_SECONDS`: AI request timeout in seconds (default `12`)
- `OLLAMA_SUMMARY_MIN_CHARS`: summary threshold (default `200`)
- `AI_ACTION_LOG_PATH`: path to `.txt` AI action log (default `instance/ai_actions.txt`)

## Local Ollama mode (no external AI calls)

AI calls are local-only and go to Ollama chat API (`http://localhost:11434/api/chat`) by default. Non-loopback endpoints are blocked.

1. Start Ollama:
   - `ollama serve`
2. Confirm model is available:
   - `ollama pull qwen2.5:7b`
3. Start Flask app:
   - `python run.py`

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
