import unittest
from unittest import mock

from app import ollama_client
from app.email_content import repair_body_text


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


def _question_digest_email():
    return {
        "sender": "Quora Digest <digest@quora.example>",
        "title": "War Alert",
        "body": (
            "Question: Ob-Gyns, does it smell bad when delivering babies? "
            "Question: Fired. Do I need to return my company car? It's a small company that had me finance it myself and they reimbursed monthly. "
            "Question: Have you ever caught your child doing something that shocked you?"
        ),
        "recipients": "",
        "cc": "",
    }


def _long_action_email():
    return {
        "sender": "Manager <manager@example.com>",
        "title": "Client meeting coverage",
        "body": (
            "Please confirm whether you can cover the client meeting tomorrow afternoon. "
            "Let me know by noon if you can take it so we can finalize the staffing plan. "
            "The client presentation starts at 2 PM, and we still need someone to handle the deck handoff "
            "and room setup before the team arrives."
        ),
        "recipients": "",
        "cc": "",
    }


def _spotify_alert_email():
    return {
        "sender": "Spotify <notifications@spotify.example>",
        "title": "Your Fave, On Tour",
        "body": (
            "GET THE DETAILS\n\n"
            "Star border Checkerboard artist image 1\n\n"
            "You're one of Joji's top fans, so we just had to let you know: "
            "they just dropped tour dates - and yep, they're headed your way.\n\n"
            "Read more\n\n"
            "Manage preferences"
        ),
        "recipients": "",
        "cc": "",
    }


def _wsj_alert_email():
    return {
        "sender": "The Wall Street Journal <alerts@wsj.example>",
        "title": "Retailers Brace for a New Shopping Reality",
        "body": (
            "A judge ruled that Amazon can keep outside AI bots off its site for now, "
            "but retailers are preparing for a new normal in shopping.\n\n"
            "The report also looks at how other sellers are responding to automated scraping "
            "and what that could mean for future shopping tactics.\n\n"
            "Read More"
        ),
        "recipients": "",
        "cc": "",
    }


def _ted_digest_email():
    return {
        "sender": "TED Recommends <recommends@ted.example>",
        "title": "How to build (and rebuild) trust",
        "body": (
            "Your weekly curated newsletter\n\n"
            "Burger's weekly curated newsletter\n\n"
            "How to build (and rebuild) trust\n\n"
            "Read transcript\n\n"
            "Watch now\n\n"
            "There's more to life than being happy\n\n"
            "Read transcript\n\n"
            "Watch now\n\n"
            "How boredom can lead to your most brilliant ideas\n\n"
            "Read transcript\n\n"
            "Watch now\n\n"
            "What is an AI anyway?\n\n"
            "Read transcript\n\n"
            "Watch now\n\n"
            "Your brand-new Cheat Sheet\n\n"
            "Cheat Sheet, brand-new from TED and Water Cooler Trivia, is trivia with a twist.\n\n"
            "Play now\n\n"
            "View in browser"
        ),
        "recipients": "",
        "cc": "",
    }


class OllamaSummaryFormattingTests(unittest.TestCase):
    def test_multistory_digest_gets_digest_overview_summary(self):
        email = _digest_email()
        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertIn("news digest", summary.lower())
        self.assertIn("it covers", summary.lower())
        self.assertNotIn("\n", summary)

    def test_question_digest_gets_structured_question_summary(self):
        email = _question_digest_email()
        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertIn("question digest", summary.lower())
        self.assertNotIn("\n", summary)
        self.assertIn("It highlights questions about", summary)

    def test_card_style_newsletter_extracts_multiple_item_titles(self):
        titles = ollama_client._extract_digest_item_titles(_ted_digest_email(), max_items=4)

        self.assertGreaterEqual(len(titles), 3)
        self.assertIn("How to build (and rebuild) trust", titles)
        self.assertIn("There's more to life than being happy", titles)
        self.assertIn("How boredom can lead to your most brilliant ideas", titles)

    def test_card_style_newsletter_does_not_fall_back_to_single_article_alert(self):
        email = _ted_digest_email()

        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertIn("newsletter", summary.lower())
        self.assertNotIn("article alert", summary.lower())
        self.assertIn("There's more to life than being happy", summary)
        self.assertIn("How boredom can lead to your most brilliant ideas", summary)

    def test_rewrite_summary_for_second_person_flattens_bullets(self):
        rewritten = ollama_client._rewrite_summary_for_second_person(
            "- The user can read the politics update.\n\n- The recipient should watch the health story."
        )

        self.assertNotIn("\n", rewritten)
        self.assertNotIn("- ", rewritten)
        self.assertIn("the recipient can read the politics update.", rewritten.lower())
        self.assertIn("the recipient should watch the health story.", rewritten.lower())
        self.assertNotIn(" you ", f" {rewritten.lower()} ")

    def test_rewrite_summary_for_second_person_flattens_inline_bullets(self):
        rewritten = ollama_client._rewrite_summary_for_second_person(
            "- First story covers tariff threats. - Second story follows hospitals preparing for flu season."
        )

        self.assertTrue(rewritten.startswith("First story"))
        self.assertNotIn("- Second story", rewritten)
        self.assertIn("Second story", rewritten)

    def test_multiline_summary_is_not_marked_unusable_just_for_newlines(self):
        email = _digest_email()
        email["summary"] = (
            "- Tariff worries push stocks lower in the lead item.\n\n"
            "- Another section focuses on hospitals preparing for a tougher flu season."
        )

        self.assertFalse(ollama_client.summary_looks_unusable(email))

    def test_rewrite_summary_repairs_mojibake_punctuation(self):
        mojibake = (
            "The recipient saw the Justice Department"
            + chr(0x00E2)
            + chr(0x0080)
            + chr(0x0099)
            + "s reversal."
        )
        rewritten = ollama_client._rewrite_summary_for_second_person(mojibake)

        self.assertIn("the recipient saw the justice department", rewritten.lower())
        self.assertIn("'", rewritten)
        self.assertNotIn(chr(0x00E2) + chr(0x0080) + chr(0x0099), rewritten)

    def test_repair_body_text_fixes_mojibake_before_summarization(self):
        mojibake = (
            "The Justice Department"
            + chr(0x00E2)
            + chr(0x0080)
            + chr(0x0099)
            + "s surprise reversal"
        )

        repaired = repair_body_text(mojibake, None)

        self.assertIn("'", repaired)
        self.assertNotIn(chr(0x00E2) + chr(0x0080) + chr(0x0099), repaired)

    def test_article_alert_summary_uses_natural_teaser_sentence(self):
        email = {
            "sender": "The Wall Street Journal <alerts@wsj.example>",
            "title": "Nvidia to Invest $2 Billion in Nebius to Expand AI Cloud Infrastructure",
            "body": (
                "Nvidia will invest $2 billion in Nebius Group to expand AI cloud infrastructure. "
                "Read more"
            ),
            "recipients": "",
            "cc": "",
        }

        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertNotIn("It focuses on", summary)
        self.assertIn("Nvidia", summary)
        self.assertNotIn("nvidia will invest", summary)

    def test_article_alert_summary_rewrites_marketing_teaser_instead_of_copying(self):
        email = _spotify_alert_email()

        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertIn("Joji", summary)
        self.assertIn("tour dates", summary.lower())
        self.assertNotIn("GET THE DETAILS", summary)
        self.assertNotIn(
            "You're one of Joji's top fans, so we just had to let you know: "
            "they just dropped tour dates - and yep, they're headed your way.",
            summary,
        )

    def test_article_alert_summary_does_not_copy_exact_body_sentence(self):
        email = _wsj_alert_email()

        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertIn("The Wall Street Journal", summary)
        self.assertIn("Retailers Brace for a New Shopping Reality", summary)
        self.assertNotIn(
            "A judge ruled that Amazon can keep outside AI bots off its site for now, "
            "but retailers are preparing for a new normal in shopping.",
            summary,
        )

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    def test_summarize_email_uses_paragraph_prompt_for_digest_emails(self, mock_call):
        email = _digest_email()

        summary = ollama_client.summarize_email(email, email_id="digest-1")
        self.assertIsNotNone(summary)
        self.assertIn("news digest", summary.lower())
        self.assertNotIn("\n", summary)
        mock_call.assert_not_called()

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value="- Markets fall after tariff threats.\n\n- Hospitals prepare for a rough flu season.",
    )
    def test_summarize_email_uses_structured_prompt_for_multi_item_newsletters(
        self,
        mock_call,
        _mock_bulk,
    ):
        email = _digest_email()

        summary = ollama_client.summarize_email(email, email_id="digest-2")

        self.assertIsNotNone(summary)
        self.assertNotIn("\n", summary)
        summarize_call = next(
            call for call in mock_call.call_args_list if call.kwargs.get("task") == "summarize"
        )
        messages = summarize_call.kwargs["messages"]
        self.assertNotIn("bullet list", messages[0]["content"].lower())
        self.assertIn("one compact paragraph", messages[0]["content"].lower())

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "You need to confirm whether you can cover the client meeting by noon while preparing for the "
            "2 PM deck handoff and room setup."
        ),
    )
    def test_summarize_email_uses_key_evidence_block_in_prompt(self, mock_call, _mock_bulk):
        email = _long_action_email()

        ollama_client.summarize_email(email, email_id="summary-evidence-1")

        summarize_call = next(
            call for call in mock_call.call_args_list if call.kwargs.get("task") == "summarize"
        )
        user_prompt = summarize_call.kwargs["messages"][1]["content"]
        self.assertIn("Key evidence:", user_prompt)
        self.assertNotIn("Body excerpts:", user_prompt)
        self.assertIn("deck handoff", user_prompt)

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value="Please confirm whether you can cover the client meeting tomorrow afternoon.",
    )
    def test_summarize_email_rejects_copied_opening_sentence(self, _mock_call):
        email = _long_action_email()

        summary = ollama_client.summarize_email(email, email_id="copy-1")

        self.assertIsNotNone(summary)
        self.assertNotIn(
            "Please confirm whether you can cover the client meeting tomorrow afternoon.",
            summary,
        )
        self.assertLess(len(summary), len(email["body"]))

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "GET THE DETAILS Star border Checkerboard artist image 1 "
            "You're one of Joji's top fans, so we just had to let you know: "
            "they just dropped tour dates - and yep, they're headed your way."
        ),
    )
    def test_summarize_email_rewrites_copied_marketing_teaser(self, _mock_call, _mock_bulk):
        email = _spotify_alert_email()

        summary = ollama_client.summarize_email(email, email_id="copy-spotify-1")

        self.assertIsNotNone(summary)
        self.assertIn("Joji", summary)
        self.assertIn("tour dates", summary.lower())
        self.assertNotIn("GET THE DETAILS", summary)
        self.assertNotIn("just had to let you know", summary.lower())

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "A judge ruled that Amazon can keep outside AI bots off its site for now, "
            "but retailers are preparing for a new normal in shopping."
        ),
    )
    def test_summarize_email_fallback_does_not_repeat_copied_article_sentence(
        self,
        _mock_call,
        _mock_bulk,
    ):
        email = _wsj_alert_email()

        summary = ollama_client.summarize_email(email, email_id="copy-wsj-1")

        self.assertIsNotNone(summary)
        self.assertIn("shopping reality", summary.lower())
        self.assertNotIn(
            "A judge ruled that Amazon can keep outside AI bots off its site for now, "
            "but retailers are preparing for a new normal in shopping.",
            summary,
        )


if __name__ == "__main__":
    unittest.main()
