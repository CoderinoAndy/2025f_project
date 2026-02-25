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
- `QWEN_API_KEY` (or `HF_TOKEN`): token for the Qwen API provider
- `QWEN_MODEL`: defaults to `Qwen/Qwen2.5-14B-Instruct`
- `QWEN_API_BASE_URL`: OpenAI-compatible base URL (default `https://router.huggingface.co/v1`)
- `QWEN_TIMEOUT_SECONDS`: AI request timeout in seconds (default `25`)

## Qwen capabilities wired in

- Lightbulb icon in email reply now calls Qwen to generate/improve a draft reply.
- Opening an email without a summary auto-triggers AI analysis for:
  - summary generation
  - category prediction (`read-only`, `junk-uncertain`, `junk`, `response-needed`)
  - priority assignment (`1` to `3`)
- Summary panel has a manual re-analyze button (`stars` icon).
