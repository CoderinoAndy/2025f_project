PRAGMA foreign_keys = ON;

-- Rebuild for local dev; removes old mock schema.
DROP TABLE IF EXISTS email_label_links;
DROP TABLE IF EXISTS email_labels;
DROP TABLE IF EXISTS emails;
DROP TABLE IF EXISTS accounts;

-- Email accounts (e.g., Outlook, Gmail). Store tokens elsewhere; this table keeps linkage metadata.
CREATE TABLE IF NOT EXISTS accounts (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  provider      TEXT NOT NULL,                     -- e.g., outlook, gmail
  email_address TEXT NOT NULL UNIQUE,
  display_name  TEXT,
  auth_type     TEXT,                              -- oauth2, basic, etc.
  created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Core email records.
CREATE TABLE IF NOT EXISTS emails (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id           INTEGER NOT NULL,
  external_id          TEXT,                       -- provider-specific message ID
  thread_id            TEXT,                       -- conversation/thread identifier
  title                TEXT NOT NULL,              -- subject line
  sender               TEXT NOT NULL,
  recipients           TEXT,                       -- comma-separated for now
  cc                   TEXT,
  bcc                  TEXT,
  body                 TEXT,
  type                 TEXT NOT NULL DEFAULT 'read-only'
                       CHECK (type IN ('response-needed','read-only','junk','junk-uncertain')),
  priority             INTEGER NOT NULL DEFAULT 1
                       CHECK (priority BETWEEN 1 AND 3),
  is_read              INTEGER NOT NULL DEFAULT 0
                       CHECK (is_read IN (0,1)),
  received_at          TEXT NOT NULL,              -- ISO timestamp (UTC preferred)
  summary              TEXT,
  summary_generated_at TEXT,
  draft                TEXT,
  draft_updated_at     TEXT,
  created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
);

-- Optional label mapping (keeps UI categories extensible).
CREATE TABLE IF NOT EXISTS email_labels (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  name      TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS email_label_links (
  email_id INTEGER NOT NULL,
  label_id INTEGER NOT NULL,
  PRIMARY KEY (email_id, label_id),
  FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE,
  FOREIGN KEY (label_id) REFERENCES email_labels(id) ON DELETE CASCADE
);

-- Seed data to mirror current UI while you wire up Outlook sync.
INSERT INTO accounts (provider, email_address, display_name)
VALUES ('outlook', 'you@example.com', 'You') ON CONFLICT(email_address) DO NOTHING;

INSERT INTO emails (account_id, external_id, thread_id, title, sender, recipients, type, priority, is_read, received_at, body, summary, draft)
SELECT a.id, 'outlook-1', 'thread-1', 'Meeting follow-up', 'teacher@school.org', 'you@example.com',
       'response-needed', 3, 0, '2026-01-10 22:58:00',
       'Can you send your draft by Friday?', 'Teacher asking for draft by Friday.',
       'Hi, thanks for the reminder! I will send it by Friday.'
FROM accounts a WHERE a.email_address = 'you@example.com'
ON CONFLICT DO NOTHING;

INSERT INTO emails (account_id, external_id, thread_id, title, sender, recipients, type, priority, is_read, received_at, body)
SELECT a.id, 'outlook-2', 'thread-2', 'Newsletter', 'news@service.com', 'you@example.com',
       'read-only', 1, 0, '2026-01-09 14:12:00',
       'This is an informational newsletter.'
FROM accounts a WHERE a.email_address = 'you@example.com'
ON CONFLICT DO NOTHING;

INSERT OR IGNORE INTO email_labels (name) VALUES ('important'), ('follow-up'), ('work');
