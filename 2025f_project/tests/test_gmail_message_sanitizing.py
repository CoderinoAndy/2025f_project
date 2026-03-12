import unittest

from app.gmail_service import _build_email_message


class GmailMessageSanitizingTests(unittest.TestCase):
    def test_build_email_message_normalizes_subject_and_body(self):
        message = _build_email_message(
            "friend@example.com",
            "",
            "Status update \u2014 ready \u2192",
            "We\u2019re set to go\u2026",
        )

        self.assertEqual(message["Subject"], "Status update - ready ->")
        self.assertIn("We're set to go...", message.get_content())


if __name__ == "__main__":
    unittest.main()
