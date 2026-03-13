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


def _promo_offer_email():
    return {
        "sender": "Walmart <offers@walmart.example>",
        "title": "Member Week deals",
        "body": (
            "Member Week highlights extra store savings for subscribers. "
            "Enjoy benefits such as easy store pickup, fast free shipping, and a low price guarantee. "
            "Offers are valid from March 6 to 12, 2026, and the message encourages members to browse "
            "qualifying items across home goods, electronics, and everyday essentials while supplies last."
        ),
        "recipients": "",
        "cc": "",
    }


def _retail_roundup_email():
    return {
        "sender": "TechMart <deals@retail.example>",
        "title": "The Ultimate Smartphone Sale is here.",
        "body": (
            "Plus, the new Arc Phone and more Top Deals.\n\n"
            "Save up to $400 on select laptops.\n\n"
            "Save up to $1,100 on select big screen TVs.\n\n"
            "Get up to 25% off when you buy multiple select appliances.\n\n"
            "Save up to 40% on select headphones and portable speakers.\n\n"
            "Shop Now\n\n"
            "Free Shipping\n\n"
            "Unsubscribe"
        ),
        "recipients": "",
        "cc": "",
    }


def _science_digest_with_marketing_word():
    return {
        "sender": "Science Briefing <digest@science.example>",
        "title": "Today in Science: How moon-base planning is evolving",
        "body": (
            "Discover how moon-base planning is evolving.\n"
            "Today in Science\n"
            "Researchers are testing new construction approaches for future lunar habitats. | 3 min read\n"
            "Another report examines how ocean temperatures are reshaping storm forecasting. | 4 min read"
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

    def test_retail_roundup_summary_uses_offer_phrases_without_digest_boilerplate(self):
        summary = ollama_client._bulk_newsletter_summary(_retail_roundup_email())

        self.assertIsNotNone(summary)
        self.assertIn("promotional update", summary.lower())
        self.assertNotIn("news digest", summary.lower())
        self.assertNotIn("it highlights", summary.lower())
        self.assertIn("up to $400 off select laptops", summary)

    def test_summary_looks_unusable_flags_boilerplate_heavy_bulk_summary(self):
        email = _retail_roundup_email()
        email["summary"] = (
            "A news digest from TechMart. It highlights Plus, the new Arc Phone and more Top Deals. "
            "It highlights Explore more deals. It highlights by category Laptops TVs Audio Appliances Smart Home. "
            "It highlights Free Shipping and shop now."
        )

        self.assertTrue(ollama_client.summary_looks_unusable(email))

    def test_extractive_fallback_for_digest_avoids_repeated_it_highlights(self):
        email = _digest_email()

        with mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None):
            summary = ollama_client._extractive_summary_fallback(email)

        self.assertIsNotNone(summary)
        self.assertIn("it covers", summary.lower())
        self.assertEqual(summary.lower().count("it highlights"), 0)

    def test_single_weak_promo_word_does_not_flip_editorial_digest_into_promo(self):
        email = _science_digest_with_marketing_word()

        self.assertIsNone(ollama_client._promotion_summary(email))
        summary = ollama_client._bulk_newsletter_summary(email)

        self.assertIsNotNone(summary)
        self.assertIn("news digest", summary.lower())

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

    def test_finalize_summary_text_adds_source_intro_and_removes_leading_also(self):
        finalized = ollama_client._finalize_summary_text(
            "It also mentions benefits such as easy store pickup, fast free shipping, and a low price guarantee. "
            "Offers are valid from March 6 to 12, 2026.",
            _promo_offer_email(),
        )

        self.assertTrue(finalized.startswith("A promotional update from Walmart."))
        self.assertNotIn("It also mentions", finalized)
        self.assertIn("It mentions benefits such as easy store pickup", finalized)
        self.assertIn("Offers are valid from March 6 to 12, 2026.", finalized)

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
        self.assertNotIn(email["title"], summary)
        self.assertIn("nvidia", summary.lower())
        self.assertNotIn("about Nvidia to Invest", summary)

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
        self.assertNotIn(email["title"], summary)
        self.assertIn("shopping", summary.lower())
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
        self.assertIn("news digest", summary.lower())
        mock_call.assert_not_called()

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
        self.assertNotIn("Subject:", user_prompt)
        self.assertIn("deck handoff", user_prompt)

    def test_subject_led_summary_is_marked_unusable(self):
        email = {
            "sender": "The Wall Street Journal <alerts@wsj.example>",
            "title": "Latest in Artificial Intelligence: The Pentagon Dealmaker Who Has Become Anthropic's Nemesis",
            "body": (
                "The newsletter profiles Michael Brown's Pentagon contracting role and how his "
                "data-labeling company Scale AI became a rival to Anthropic. It also describes "
                "defense-tech customers, model-evaluation work, and the company's expanding role "
                "in military AI programs."
            ),
            "recipients": "",
            "cc": "",
        }
        email["summary"] = (
            "The Wall Street Journal sent a newsletter about The Pentagon Dealmaker Who Has Become "
            "Anthropic's Nemesis. It features Latest in Artificial Intelligence and The Pentagon "
            "Dealmaker Who Has Become Anthropic's Nemesis."
        )

        self.assertTrue(ollama_client.summary_looks_unusable(email))

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "The Wall Street Journal sent a newsletter about The Pentagon Dealmaker Who Has Become "
            "Anthropic's Nemesis. It features Latest in Artificial Intelligence and The Pentagon "
            "Dealmaker Who Has Become Anthropic's Nemesis."
        ),
    )
    def test_summarize_email_rejects_subject_led_summary(self, _mock_call, _mock_bulk):
        email = {
            "sender": "The Wall Street Journal <alerts@wsj.example>",
            "title": "Latest in Artificial Intelligence: The Pentagon Dealmaker Who Has Become Anthropic's Nemesis",
            "body": (
                "The newsletter profiles Michael Brown's Pentagon contracting role and how his "
                "data-labeling company Scale AI became a rival to Anthropic. It also describes "
                "defense-tech customers, model-evaluation work, and the company's expanding role "
                "in military AI programs."
            ),
            "recipients": "",
            "cc": "",
        }

        summary = ollama_client.summarize_email(email, email_id="subject-copy-1")

        self.assertIsNotNone(summary)
        self.assertNotIn(email["title"], summary)
        self.assertIn("Scale AI", summary)
        self.assertIn("Anthropic", summary)

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._sanitize_model_summary",
        return_value=(
            "It also mentions benefits such as easy store pickup, fast free shipping, and a low price guarantee. "
            "Offers are valid from March 6 to 12, 2026."
        ),
    )
    @mock.patch("app.ollama_client._call_ollama", return_value="raw summary")
    def test_summarize_email_polishes_promotional_summary_lead(
        self,
        _mock_call,
        _mock_sanitize,
        _mock_bulk,
    ):
        summary = ollama_client.summarize_email(_promo_offer_email(), email_id="promo-lead-1")

        self.assertIsNotNone(summary)
        self.assertTrue(summary.startswith("A promotional update from Walmart."))
        self.assertNotIn("It also mentions", summary)
        self.assertIn("benefits such as easy store pickup", summary)
        self.assertIn("offers are valid from march 6 to 12, 2026", summary.lower())
        self.assertNotIn("the email notes that", summary.lower())

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
        self.assertIn("shopping", summary.lower())
        self.assertIn("automated scraping", summary.lower())
        self.assertNotIn(
            "A judge ruled that Amazon can keep outside AI bots off its site for now, "
            "but retailers are preparing for a new normal in shopping.",
            summary,
        )


if __name__ == "__main__":
    unittest.main()
