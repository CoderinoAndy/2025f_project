import unittest
from unittest import mock

from app import ollama_client


def _email(sender, title, body):
    return {
        "sender": sender,
        "title": title,
        "body": body,
        "recipients": "",
        "cc": "",
    }


class OllamaClassificationTests(unittest.TestCase):
    def test_promotional_store_blast_is_junk(self):
        email = _email(
            "Deals Team <deals@brand-mail.example>",
            "Last chance: 30% off today plus free shipping",
            (
                "Use code SPRING30 at checkout. Shop now for member exclusive savings. "
                "Manage preferences or unsubscribe."
            ),
        )

        assessment = ollama_client._junk_signal_assessment(email)
        result = ollama_client._heuristic_classification(email)

        self.assertTrue(assessment["commercial_promotion"])
        self.assertFalse(assessment["editorial_like"])
        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk")

    def test_editorial_digest_stays_read_only(self):
        email = _email(
            "Morning Briefing <dailybrief@news-digest.example>",
            "Morning briefing: top stories",
            (
                "Today's headlines and analysis. Read more in the newsletter. "
                "Manage preferences if you want fewer updates."
            ),
        )

        assessment = ollama_client._junk_signal_assessment(email)
        result = ollama_client._heuristic_classification(email)

        self.assertFalse(assessment["commercial_promotion"])
        self.assertTrue(assessment["editorial_like"])
        self.assertEqual(result["category"], "informational")
        self.assertEqual(result["email_type"], "read-only")

    def test_transactional_receipt_stays_read_only(self):
        email = _email(
            "Billing <billing@service.example>",
            "Your monthly statement is ready",
            (
                "Your statement is ready to view online. No action required unless there is an issue. "
                "This payment receipt is for your records."
            ),
        )

        assessment = ollama_client._junk_signal_assessment(email)
        result = ollama_client._heuristic_classification(email)

        self.assertTrue(assessment["transactional_like"])
        self.assertFalse(assessment["commercial_promotion"])
        self.assertEqual(result["category"], "informational")
        self.assertEqual(result["email_type"], "read-only")

    def test_phishing_message_is_junk(self):
        email = _email(
            "Security Check <security-check@pay-verify.example>",
            "Urgent action required: verify your account",
            (
                "We noticed unusual activity. Click here to confirm your identity "
                "or your access may be suspended."
            ),
        )

        result = ollama_client._heuristic_classification(email)

        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk")

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    def test_classifier_prompt_explicitly_defines_promotional_junk(self, mock_call):
        email = _email(
            "Deals Team <deals@brand-mail.example>",
            "30% off today plus free shipping",
            "Use code SPRING30 at checkout. Shop now. Manage preferences or unsubscribe.",
        )

        result = ollama_client.classify_email(email, email_id="promo-1")
        messages = mock_call.call_args.kwargs["messages"]
        system_prompt = messages[0]["content"]
        user_prompt = messages[1]["content"]

        self.assertEqual(result["category"], "junk")
        self.assertIn("main purpose is advertising, promotion, sales conversion", system_prompt)
        self.assertIn("If the email is mainly a commercial promotion", system_prompt)
        self.assertIn("commercial_promotion_pattern", user_prompt)


if __name__ == "__main__":
    unittest.main()
