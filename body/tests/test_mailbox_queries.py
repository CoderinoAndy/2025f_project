import tempfile
import unittest
from pathlib import Path

from app import db, mailbox


class MailboxQueryTests(unittest.TestCase):
    def _fresh_db_path(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        db_path = str(Path(temp_dir.name) / "app.sqlite")
        db.init_db(db_path=db_path)
        with db.db_session(db_path) as conn:
            conn.execute("DELETE FROM email_recipients")
            conn.execute("DELETE FROM email_messages")
        return db_path

    def _insert_email(
        self,
        db_path,
        *,
        external_id,
        title,
        sender,
        recipients,
        cc="",
        body="",
        email_type="read-only",
        priority=1,
        is_read=False,
        received_at="2026-01-01 09:00:00",
    ):
        db.upsert_email_from_provider(
            {
                "external_id": external_id,
                "thread_id": f"thread-{external_id}",
                "title": title,
                "sender": sender,
                "recipients": recipients,
                "cc": cc,
                "body": body,
                "type": email_type,
                "priority": priority,
                "is_read": is_read,
                "received_at": received_at,
            },
            db_path=db_path,
        )

    def test_fetch_mailbox_page_applies_sort_limit_and_lightweight_columns(self):
        db_path = self._fresh_db_path()
        self._insert_email(
            db_path,
            external_id="msg-low",
            title="Low priority",
            sender="alpha@example.com",
            recipients="you@example.com",
            priority=1,
            email_type="response-needed",
            received_at="2026-01-01 08:00:00",
        )
        self._insert_email(
            db_path,
            external_id="msg-high",
            title="High priority",
            sender="beta@example.com",
            recipients="you@example.com",
            priority=3,
            email_type="response-needed",
            received_at="2026-01-01 10:00:00",
        )
        self._insert_email(
            db_path,
            external_id="msg-mid",
            title="Mid priority",
            sender="gamma@example.com",
            recipients="you@example.com",
            priority=2,
            email_type="response-needed",
            received_at="2026-01-01 09:00:00",
        )

        rows = db.fetch_mailbox_page(
            email_type="response-needed",
            sort_code="priority_desc",
            limit=2,
            db_path=db_path,
        )

        self.assertEqual([row["title"] for row in rows], ["High priority", "Mid priority"])
        self.assertNotIn("body", rows[0])
        self.assertEqual(rows[0]["date"], "2026-01-01 10:00:00")

    def test_mailbox_search_counts_and_matches_body_and_recipients(self):
        db_path = self._fresh_db_path()
        self._insert_email(
            db_path,
            external_id="msg-family",
            title="Weekend dinner",
            sender="parent@family.com",
            recipients="you@example.com",
            cc="sibling@family.com",
            body="Dinner on Sunday at 6 pm works for me.",
            email_type="response-needed",
            received_at="2026-01-03 18:00:00",
        )
        self._insert_email(
            db_path,
            external_id="msg-work",
            title="Sprint update",
            sender="manager@work.com",
            recipients="you@example.com",
            cc="teammate@work.com",
            body="Milestone B is at 60 percent.",
            email_type="read-only",
            received_at="2026-01-02 10:00:00",
        )

        recipient_matches = db.count_mailbox_emails(
            search_query="sibling@family.com",
            db_path=db_path,
        )
        body_matches = db.fetch_mailbox_page(
            search_query="dinner on sunday",
            sort_code="date_desc",
            limit=5,
            db_path=db_path,
        )

        self.assertEqual(recipient_matches, 1)
        self.assertEqual([row["title"] for row in body_matches], ["Weekend dinner"])

    def test_build_mailbox_pagination_clamps_and_preserves_query_params(self):
        pagination = mailbox.build_mailbox_pagination(
            "/allemails?sort=priority_desc&q=family&page=9",
            page=9,
            page_size=100,
            total_count=220,
        )

        self.assertEqual(pagination["current_page"], 3)
        self.assertEqual(pagination["total_pages"], 3)
        self.assertEqual(pagination["start_index"], 201)
        self.assertEqual(pagination["end_index"], 220)
        self.assertEqual(
            pagination["current_url"],
            "/allemails?sort=priority_desc&q=family&page=3",
        )
        self.assertEqual(
            pagination["prev_url"],
            "/allemails?sort=priority_desc&q=family&page=2",
        )
        self.assertIsNone(pagination["next_url"])


if __name__ == "__main__":
    unittest.main()
