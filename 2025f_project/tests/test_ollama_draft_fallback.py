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


def _action_email():
    return {
        "sender": "Manager <manager@example.com>",
        "title": "Please confirm the staffing plan",
        "body": (
            "Can you confirm whether you can cover the client meeting tomorrow afternoon? "
            "Let me know by noon so we can lock the roster. "
            "The client presentation starts at 2 PM, and we still need someone to handle the deck handoff "
            "and room setup before the team arrives."
        ),
        "recipients": "you@example.com",
        "cc": "",
        "type": "response-needed",
    }


def _mock_vision_message(*_args, **_kwargs):
    return {
        "role": "user",
        "content": (
            "Sender and subject metadata:\n"
            "- From: Manager <manager@example.com>\n"
            "- Subject: Please confirm the staffing plan"
        ),
        "images": ["fake-vision-image"],
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
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_draft_reply_uses_saved_display_name_in_context_and_fallback(
        self,
        _mock_vision,
        mock_call,
        _mock_name,
    ):
        email = _action_email()

        draft = ollama_client.draft_reply(email, to_value="manager@example.com", email_id="reply-1")
        messages = mock_call.call_args.kwargs["messages"]

        self.assertIn("Mailbox owner profile:", messages[1]["content"])
        self.assertIn("Casey Nguyen", messages[1]["content"])
        self.assertIn("use that exact name in any personal sign-off", messages[0]["content"])
        self.assertTrue(draft.endswith("Best regards,\nCasey Nguyen"))

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    def test_draft_reply_fallback_uses_contextual_request_deadline_and_detail(self, _mock_call):
        draft = ollama_client.draft_reply(_action_email(), to_value="manager@example.com")

        self.assertIn("staffing plan", draft.lower())
        self.assertIn("deck handoff", draft.lower())
        self.assertIn("by noon", draft.lower())
        self.assertNotIn("I'll review the details and send you a specific response as soon as I can.", draft)

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    @mock.patch("app.ollama_client._render_email_image_pages")
    def test_draft_reply_prompt_uses_text_reply_plan_context_for_plain_text_email(
        self,
        mock_render,
        mock_call,
    ):
        ollama_client.draft_reply(_action_email(), to_value="manager@example.com", email_id="reply-plan-1")

        self.assertEqual(mock_call.call_count, 1)
        mock_render.assert_not_called()
        draft_messages = mock_call.call_args_list[0].kwargs["messages"]
        self.assertIn("Reply plan:", draft_messages[1]["content"])
        self.assertNotIn("Body excerpts:", draft_messages[1]["content"])
        self.assertIn("deck handoff and room setup", draft_messages[1]["content"])

    @mock.patch("app.ollama_client._call_ollama")
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_draft_reply_rejects_copied_request_sentence(self, _mock_vision, mock_call):
        mock_call.side_effect = [
            (
                "Hi Manager,\n\n"
                "Can you confirm whether you can cover the client meeting tomorrow afternoon?\n\n"
                "Best regards,"
            ),
            None,
        ]

        draft = ollama_client.draft_reply(_action_email(), to_value="manager@example.com")

        self.assertNotIn(
            "Can you confirm whether you can cover the client meeting tomorrow afternoon?",
            draft,
        )
        self.assertIn("direct response", draft.lower())

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "Hi Manager,\n\n"
            "Yes, I can cover the client meeting tomorrow afternoon. I'll handle the deck handoff and room setup "
            "and confirm the roster before noon.\n\n"
            "Best regards,"
        ),
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_revise_reply_can_replace_weak_stub_with_plan_grounded_reply(self, _mock_vision, mock_call):
        revised = ollama_client.revise_reply(
            _action_email(),
            "Thanks, I'll review and get back to you.",
            to_value="manager@example.com",
        )

        self.assertIn("I can cover the client meeting tomorrow afternoon", revised)
        self.assertIn("before noon", revised)


if __name__ == "__main__":
    unittest.main()
