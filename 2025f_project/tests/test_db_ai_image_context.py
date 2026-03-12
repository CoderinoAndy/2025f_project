import tempfile
import unittest
from pathlib import Path

from app import db


class DbAiImageContextTests(unittest.TestCase):
    def test_update_email_ai_fields_persists_image_context(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "app.sqlite")
            db.init_db(db_path=db_path)

            email = db.fetch_email_by_id(1, db_path=db_path)
            db.update_email_ai_fields(
                email_id=email["id"],
                ai_image_context='- Attached image file "invoice.png" (image/png, about 120 KB).',
                ai_image_context_status="ready",
                db_path=db_path,
            )

            refreshed = db.fetch_email_by_id(1, db_path=db_path)
            self.assertIn("invoice.png", refreshed["ai_image_context"])
            self.assertEqual(refreshed["ai_image_context_status"], "ready")
            self.assertIsNotNone(refreshed["ai_image_context_updated_at"])

    def test_provider_upsert_clears_cached_image_context_when_body_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "app.sqlite")
            db.init_db(db_path=db_path)

            email = db.fetch_email_by_id(1, db_path=db_path)
            db.update_email_ai_fields(
                email_id=email["id"],
                ai_image_context='- Inline image: label "draft screenshot".',
                ai_image_context_status="ready",
                db_path=db_path,
            )

            db.upsert_email_from_provider(
                {
                    "external_id": email["external_id"],
                    "thread_id": email["thread_id"],
                    "title": email["title"],
                    "sender": email["sender"],
                    "recipients": email["recipients"],
                    "cc": email["cc"],
                    "body": f'{email["body"]} Updated with a new screenshot.',
                    "body_html": email["body_html"],
                    "type": email["type"],
                    "priority": email["priority"],
                    "is_read": email["is_read"],
                    "received_at": email["received_at"],
                },
                db_path=db_path,
            )

            refreshed = db.fetch_email_by_id(1, db_path=db_path)
            self.assertIsNone(refreshed["ai_image_context"])
            self.assertIsNone(refreshed["ai_image_context_status"])
            self.assertIsNone(refreshed["ai_image_context_updated_at"])


if __name__ == "__main__":
    unittest.main()
