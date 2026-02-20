PRAGMA foreign_keys = ON;

-- Normalized email message table (one row per message)
CREATE TABLE IF NOT EXISTS email_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT UNIQUE,                        -- provider-specific message ID (optional)
  thread_id TEXT,                               -- conversation/thread identifier
  title TEXT NOT NULL,                      -- subject line
  sender TEXT NOT NULL,                      -- single sender address
  body TEXT,
  type TEXT NOT NULL DEFAULT 'read-only'
    CHECK (type IN ('response-needed','read-only','junk','junk-uncertain')),
  priority INTEGER NOT NULL DEFAULT 1
    CHECK (priority BETWEEN 1 AND 3),
  is_read INTEGER NOT NULL DEFAULT 0
    CHECK (is_read IN (0,1)),
  received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  summary TEXT,
  draft TEXT
);

-- Recipient rows (one row per address per email), instead of comma-separated columns
CREATE TABLE IF NOT EXISTS email_recipients (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email_id INTEGER NOT NULL,
  recipient_type TEXT NOT NULL
    CHECK (recipient_type IN ('to','cc')),
  address TEXT NOT NULL,
  FOREIGN KEY (email_id) REFERENCES email_messages(id) ON DELETE CASCADE,
  UNIQUE (email_id, recipient_type, address)
);

-- Seed messages
INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read,
  received_at, body, summary, draft
)
VALUES (
  'mail-1',
  'thread-1',
  'Meeting follow-up',
  'teacher@school.org',
  'response-needed',
  3,
  0,
  '2026-01-10 22:58:00',
  'Can you send your draft by Friday?',
  'Teacher asking for draft by Friday.',
  'Hi, thanks for the reminder! I will send it by Friday.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body
)
VALUES (
  'mail-1-reply',
  'thread-1',
  'Re: Meeting follow-up',
  'you@example.com',
  'read-only',
  2,
  1,
  '2026-01-10 23:10:00',
  'Thanks for the reminder. I will send the draft by Friday morning.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body
)
VALUES (
  'mail-2',
  'thread-2',
  'Newsletter',
  'news@service.com',
  'read-only',
  1,
  0,
  '2026-01-09 14:12:00',
  'This is an informational newsletter.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body, summary
)
VALUES (
  'mail-3',
  'thread-3',
  'Project update',
  'teammate@work.com',
  'read-only',
  2,
  1,
  '2026-01-08 09:15:00',
  'Quick update: milestone A is done, milestone B is at 60%.',
  'Milestone A done; milestone B at 60%.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read,
  received_at, body, summary, draft
)
VALUES (
  'mail-4',
  'thread-4',
  'Interview scheduling',
  'recruiter@company.com',
  'response-needed',
  3,
  0,
  '2026-01-07 16:40:00',
  'Can you share availability for next week? We are flexible on Tue-Thu.',
  'Recruiter asking for availability next week.',
  'Hi, thanks for reaching out. I am available Tue 10-12 and Wed 2-4.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body
)
VALUES (
  'mail-4-reply',
  'thread-4',
  'Re: Interview scheduling',
  'you@example.com',
  'read-only',
  2,
  1,
  '2026-01-07 17:05:00',
  'Happy to connect. Tuesday 10-12 or Wednesday 2-4 both work for me.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body
)
VALUES (
  'mail-5',
  'thread-5',
  'Security alert',
  'no-reply@service.com',
  'junk-uncertain',
  2,
  0,
  '2026-01-06 03:20:00',
  'New sign-in from a new device. If this was not you, reset your password.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body
)
VALUES (
  'mail-6',
  'thread-6',
  'Family plans',
  'parent@family.com',
  'response-needed',
  2,
  0,
  '2026-01-05 19:05:00',
  'Dinner on Sunday? Let me know if 6 pm works.'
)
ON CONFLICT(external_id) DO NOTHING;

INSERT INTO email_messages (
  external_id, thread_id, title, sender, type, priority, is_read, received_at, body
)
VALUES (
  'mail-7',
  'thread-7',
  'Sale confirmation',
  'store@shop.com',
  'junk',
  1,
  1,
  '2026-01-04 11:30:00',
  'Thanks for your purchase. Your order will ship in 2-3 days.'
)
ON CONFLICT(external_id) DO NOTHING;

-- Seed recipients
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-1';
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'cc', 'advisor@school.org' FROM email_messages WHERE external_id = 'mail-1';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'teacher@school.org' FROM email_messages WHERE external_id = 'mail-1-reply';
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'cc', 'advisor@school.org' FROM email_messages WHERE external_id = 'mail-1-reply';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-2';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-3';
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'cc', 'manager@work.com' FROM email_messages WHERE external_id = 'mail-3';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-4';
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'cc', 'hiring-manager@company.com' FROM email_messages WHERE external_id = 'mail-4';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'recruiter@company.com' FROM email_messages WHERE external_id = 'mail-4-reply';
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'cc', 'hiring-manager@company.com' FROM email_messages WHERE external_id = 'mail-4-reply';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-5';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-6';
INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'cc', 'sibling@family.com' FROM email_messages WHERE external_id = 'mail-6';

INSERT OR IGNORE INTO email_recipients (email_id, recipient_type, address)
SELECT id, 'to', 'you@example.com' FROM email_messages WHERE external_id = 'mail-7';
