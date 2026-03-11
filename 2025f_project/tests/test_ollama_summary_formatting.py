import unittest
from unittest import mock

from app import ollama_client


def _digest_email():
    return {
        "sender": "Morning Briefing <briefing@news-digest.example>",
        "title": "Morning Briefing: top stories",
        "body": (
            "Top stories\n\n"
            "Markets slide after new tariff threats raise fears of higher prices. 5 min read.\n\n"
            "Hospitals prepare for a rough flu season as cases climb across several states. 4 min read.\n\n"
            "Cities debate zoning changes after rents rose again in major metro areas. 6 min read.\n\n"
            "Voters head to the polls after a late campaign dispute over budget cuts. 5 min read.\n\n"
            "Read more"
        ),
        "recipients": "",
        "cc": "",
    }


class OllamaSummaryFormattingTests(unittest.TestCase):
    def test_multistory_digest_uses_structured_summary_mode(self):
        email = _digest_email()

        self.assertTrue(ollama_client._should_use_structured_summary(email))

    def test_structured_summary_fallback_returns_spaced_bullets(self):
        email = _digest_email()

        summary = ollama_client._structured_summary_fallback(email)

        self.assertIsNotNone(summary)
        self.assertTrue(summary.startswith("- "))
        self.assertIn("\n\n- ", summary)

    def test_rewrite_summary_for_second_person_preserves_bullets(self):
        rewritten = ollama_client._rewrite_summary_for_second_person(
            "- The user can read the politics update.\n\n- The recipient should watch the health story."
        )

        self.assertIn("\n\n- ", rewritten)
        self.assertIn("you can read the politics update.", rewritten.lower())
        self.assertIn("you should watch the health story.", rewritten.lower())

    def test_rewrite_summary_for_second_person_splits_inline_bullets(self):
        rewritten = ollama_client._rewrite_summary_for_second_person(
            "- First story covers tariff threats. - Second story follows hospitals preparing for flu season."
        )

        self.assertTrue(rewritten.startswith("- First story"))
        self.assertIn("\n\n- Second story", rewritten)

    def test_multiline_summary_is_not_marked_unusable_just_for_newlines(self):
        email = _digest_email()
        email["summary"] = (
            "- Tariff worries push stocks lower in the lead item.\n\n"
            "- Another section focuses on hospitals preparing for a tougher flu season."
        )

        self.assertFalse(ollama_client.summary_looks_unusable(email))

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    def test_summarize_email_requests_bullets_for_structured_emails(self, mock_call):
        email = _digest_email()

        summary = ollama_client.summarize_email(email, email_id="digest-1")
        messages = mock_call.call_args.kwargs["messages"]
        system_prompt = messages[0]["content"]
        user_prompt = messages[1]["content"]

        self.assertIsNotNone(summary)
        self.assertTrue(summary.startswith("- "))
        self.assertIn("bullet list", system_prompt)
        self.assertIn("split them into separate bullets", user_prompt)


if __name__ == "__main__":
    unittest.main()
