import unittest
from unittest import mock

from app import ollama_client


def _bestbuy_like_email():
    entity_noise = " ".join(["&#847;", "&zwnj;", "&#8199;", "&#65279;"] * 12)
    return {
        "sender": '"BestBuy.ca" <newsletter@e.bestbuy.ca>',
        "title": "Available now: MacBook Neo, iPad Air, MacBook Air and more.",
        "body": (
            f"{entity_noise} Top Deals &rsaquo; Outlet &rsaquo; "
            "Yes, Best Buy sells that &rsaquo; "
            "Get MacBook Neo for as low as $399.99 with a qualifying trade-in."
        ),
        "body_html": (
            "<html><body>"
            "<p>The Latest &amp; Greatest from Apple is here. Grab yours today!</p>"
            "<p>Get MacBook Neo for as low as $399.99 with a qualifying trade-in.</p>"
            "</body></html>"
        ),
        "type": "junk",
        "recipients": "",
        "cc": "",
    }


class OllamaDraftFallbackTests(unittest.TestCase):
    def test_revise_fallback_does_not_echo_html_entity_noise_for_bulk_email(self):
        draft = ollama_client._revise_reply_fallback(
            _bestbuy_like_email(),
            "Yep let me buy it!",
        )

        self.assertIsNotNone(draft)
        self.assertIn("Yep let me buy it!", draft)
        self.assertNotIn("&#847;", draft)
        self.assertNotIn("I appreciate the note about", draft)

    @mock.patch("app.ollama_client.get_user_display_name", return_value="Casey Nguyen")
    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    def test_draft_reply_uses_saved_display_name_in_context_and_fallback(
        self,
        mock_call,
        _mock_name,
    ):
        email = {
            "sender": "Manager <manager@example.com>",
            "title": "Please confirm the staffing plan",
            "body": (
                "Can you confirm whether you can cover the client meeting tomorrow afternoon? "
                "Let me know by noon so we can lock the roster."
            ),
            "recipients": "you@example.com",
            "cc": "",
            "type": "response-needed",
        }

        draft = ollama_client.draft_reply(email, to_value="manager@example.com", email_id="reply-1")
        messages = mock_call.call_args.kwargs["messages"]

        self.assertIn("Mailbox owner profile:", messages[1]["content"])
        self.assertIn("Casey Nguyen", messages[1]["content"])
        self.assertIn("use that exact name in any personal sign-off", messages[0]["content"])
        self.assertTrue(draft.endswith("Best regards,\nCasey Nguyen"))


if __name__ == "__main__":
    unittest.main()
