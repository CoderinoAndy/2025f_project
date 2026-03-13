import unittest
from unittest import mock

from app import ollama_client
from app.email_content import repair_body_text


def _mock_vision_message(*_args, **_kwargs):
    return {
        "role": "user",
        "content": (
            "Sender and subject metadata:\n"
            "- From: Manager <manager@example.com>\n"
            "- Subject: Client meeting coverage"
        ),
        "images": ["fake-vision-image"],
    }


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


def _wsj_split_teaser_email():
    return {
        "sender": "The Wall Street Journal <alerts@wsj.example>",
        "title": "Latest in Artificial Intelligence: Musk Says xAI Must Be Rebuilt as Co-Founders Exit",
        "body": (
            "LATEST IN ARTIFICIAL INTELLIGENCE\n\n"
            "Musk Says xAI Must Be Rebuilt as Co-Founders Exit\n\n"
            "Weeks\n"
            "after merger with SpaceX, Elon Musk says xAI is being rebuilt from the foundations up.\n\n"
            "The alert says the company is resetting its strategy after the leadership shake-up.\n\n"
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


def _image_promo_email():
    return {
        "sender": "Uber Eats <uber@uber.com>",
        "title": "Score big with Lonzo's Kitchen today.",
        "body": "GAME CHANGING DEALS",
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


def _html_fragment_newsletter_email():
    return {
        "sender": "Morning Brew <crew@morningbrew.com>",
        "title": "A very 'Dora' update",
        "body": (
            'TSA gets desperate amid partial shutdown...<a href="http://www.morningbrew.com">'
            '<img src="https://cdn.example.com/story.gif" alt="" border="0" /></a>\n\n'
            "March 13, 2026View Online | Sign Up | Shop\n"
            "Morning Brew\n"
            "Happy Friday the 13th.\n"
            "Markets: Stocks fell sharply after attacks on two oil tankers.\n"
        ),
        "recipients": "",
        "cc": "",
    }


def _sobeys_paralympic_email():
    return {
        "sender": "Sobeys <sobeys@em.sobeys.com>",
        "title": "The excitement of the Paralympic Games continues!",
        "body": (
            "Cheer on Team Canada at the Paralympic Games!\n\n"
            "Andy, Cheer on Team Canada at the Paralympic Games!\n\n"
            "Discover member exclusive grocery offers and inspiration from Sobeys through the Feed The Dream campaign.\n\n"
            "Team Canada's Paralympic athletes are currently competing, backed by the communities "
            "and supporters who help Feed The Dream. Let's come together to celebrate their dedication "
            "and cheer them on at the Milano-Cortina 2026 Paralympic Games.\n\n"
            "My Grocery Offers | Flyer | Inspiration | Preferences\n\n"
            "Terms & Conditions | Privacy Policy | Unsubscribe"
        ),
        "recipients": "",
        "cc": "",
    }


def _html_layout_email():
    return {
        "sender": "Retail Brand <offers@retail.example>",
        "title": "Weekend offers",
        "body": "Weekend offers with a hero banner and product grid.",
        "body_html": (
            "<html><body><table><tr><td><img src='cid:hero-banner'></td></tr>"
            "<tr><td><div style='display:grid'>Tap to shop the weekend offers.</div></td></tr>"
            "</table></body></html>"
        ),
        "recipients": "",
        "cc": "",
    }


class OllamaSummaryFormattingTests(unittest.TestCase):
    def test_vision_render_body_text_keeps_full_email_content(self):
        email = {
            "sender": "Sender <sender@example.com>",
            "title": "Status update",
            "body": "Line one.\n\nLine two with details.\n\nLine three.",
            "recipients": "you@example.com",
            "cc": "",
        }

        rendered = ollama_client._vision_render_body_text(email)

        self.assertIn("Line one.", rendered)
        self.assertIn("Line two with details.", rendered)
        self.assertIn("Line three.", rendered)

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
        self.assertIn("It features questions about", summary)

    def test_retail_roundup_summary_uses_offer_phrases_without_digest_boilerplate(self):
        summary = ollama_client._bulk_newsletter_summary(_retail_roundup_email())

        self.assertIsNotNone(summary)
        self.assertIn("promotional update", summary.lower())
        self.assertNotIn("news digest", summary.lower())
        self.assertNotIn("it highlights", summary.lower())
        self.assertIn("up to $400 off select laptops", summary)

    def test_summary_looks_unusable_does_not_reject_bulk_style_summary(self):
        email = _retail_roundup_email()
        email["summary"] = (
            "A news digest from TechMart. It highlights Plus, the new Arc Phone and more Top Deals. "
            "It highlights Explore more deals. It highlights by category Laptops TVs Audio Appliances Smart Home. "
            "It highlights Free Shipping and shop now."
        )

        self.assertFalse(ollama_client.summary_looks_unusable(email))

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

    def test_clean_body_for_prompt_strips_inline_html_fragments(self):
        cleaned = ollama_client._clean_body_for_prompt(_html_fragment_newsletter_email(), max_chars=800)

        self.assertNotIn("<a href", cleaned)
        self.assertNotIn("src=", cleaned)
        self.assertIn("TSA gets desperate amid partial shutdown", cleaned)

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
        self.assertIn("Key details include benefits such as easy store pickup", finalized)
        self.assertIn("Offers are valid from March 6 to 12, 2026.", finalized)

    def test_promo_summary_avoids_subject_copy_and_lazy_promo_lead(self):
        summary = ollama_client._bulk_newsletter_summary(_image_promo_email())

        self.assertIsNotNone(summary)
        self.assertTrue(summary.startswith("A promotional update from Uber Eats."))
        self.assertIn("Lonzo's Kitchen", summary)
        self.assertIn("special deals", summary.lower())
        self.assertNotIn("It includes", summary)
        self.assertNotIn("Score big with", summary)
        self.assertNotIn("today.", summary)

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

    def test_topic_phrase_from_sentence_keeps_detail_for_temporal_leadins(self):
        phrase = ollama_client._topic_phrase_from_sentence(
            "Weeks after merger with SpaceX, Elon Musk says xAI is being rebuilt from the foundations up."
        )

        self.assertNotEqual(phrase, "Weeks")
        self.assertIn("xAI", phrase)

    @mock.patch(
        "app.ollama_client._vision_user_message",
        side_effect=AssertionError("single article fast path should not build a model prompt"),
    )
    @mock.patch(
        "app.ollama_client._call_ollama",
        side_effect=AssertionError("single article fast path should not call the model"),
    )
    def test_summarize_email_uses_fast_article_path_for_split_teaser_alert(
        self,
        _mock_call,
        _mock_vision,
    ):
        summary = ollama_client.summarize_email(
            _wsj_split_teaser_email(),
            email_id="wsj-split-teaser-1",
        )

        self.assertIsNotNone(summary)
        self.assertIn("The Wall Street Journal", summary)
        self.assertIn("xAI", summary)
        self.assertIn("SpaceX", summary)
        self.assertNotIn("The article focuses on Weeks", summary)
        self.assertNotEqual(
            summary,
            "An article alert from The Wall Street Journal. The article focuses on Weeks.",
        )

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "Morning Briefing covers tariff worries in the markets, hospitals preparing for flu season, "
            "and cities debating zoning changes after rent increases."
        ),
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_returns_model_summary_for_digest_emails(self, _mock_vision, mock_call):
        email = _digest_email()

        summary = ollama_client.summarize_email(email, email_id="digest-1")
        self.assertIsNotNone(summary)
        self.assertIn("Morning Briefing covers tariff worries", summary)
        self.assertNotIn("\n", summary)
        self.assertEqual(mock_call.call_count, 1)

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "The digest highlights tariff-related market pressure and hospitals preparing for a rough flu season."
        ),
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_uses_structured_prompt_for_multi_item_newsletters(
        self,
        _mock_vision,
        mock_call,
        _mock_bulk,
    ):
        email = _digest_email()

        summary = ollama_client.summarize_email(email, email_id="digest-2")

        self.assertIsNotNone(summary)
        self.assertNotIn("\n", summary)
        self.assertIn("tariff", summary.lower())
        self.assertEqual(mock_call.call_count, 1)

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "You need to confirm whether you can cover the client meeting by noon while preparing for the "
            "2 PM deck handoff and room setup."
        ),
    )
    @mock.patch("app.ollama_client._render_email_image_pages")
    def test_summarize_email_uses_text_payload_and_metadata_for_plain_text_email(
        self,
        mock_render,
        mock_call,
        _mock_bulk,
    ):
        email = _long_action_email()

        ollama_client.summarize_email(email, email_id="summary-evidence-1")

        summarize_call = next(
            call for call in mock_call.call_args_list if call.kwargs.get("task") == "summarize"
        )
        user_message = summarize_call.kwargs["messages"][1]
        self.assertIn("Sender and subject metadata", user_message["content"])
        self.assertIn("Source email text", user_message["content"])
        self.assertNotIn("Key evidence:", user_message["content"])
        self.assertNotIn("images", user_message)
        mock_render.assert_not_called()

    @mock.patch("app.ollama_client._render_email_image_pages", return_value=["fake-vision-image"])
    def test_vision_user_message_keeps_images_for_html_heavy_email(self, mock_render):
        user_message = ollama_client._vision_user_message(
            _html_layout_email(),
            "Summarize this email.",
            task="summarize",
        )

        self.assertIsNotNone(user_message)
        self.assertEqual(user_message["images"], ["fake-vision-image"])
        self.assertIn("Rendered screenshots of the original HTML email are attached.", user_message["content"])
        mock_render.assert_called_once()

    @mock.patch("app.ollama_client._render_email_image_pages", return_value=[])
    def test_vision_user_message_falls_back_to_text_when_real_render_is_unavailable(self, mock_render):
        user_message = ollama_client._vision_user_message(
            _html_layout_email(),
            "Summarize this email.",
            task="summarize",
        )

        self.assertIsNotNone(user_message)
        self.assertNotIn("images", user_message)
        self.assertNotIn("Rendered screenshots of the original HTML email are attached.", user_message["content"])
        mock_render.assert_called_once()

    def test_summary_looks_unusable_flags_placeholder_summary(self):
        email = _retail_roundup_email()
        email["summary"] = "Summary unavailable."

        self.assertTrue(ollama_client.summary_looks_unusable(email))

    def test_summary_looks_unusable_allows_subject_led_summary(self):
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

        self.assertFalse(ollama_client.summary_looks_unusable(email))

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "The Wall Street Journal sent a newsletter about The Pentagon Dealmaker Who Has Become "
            "Anthropic's Nemesis. It features Latest in Artificial Intelligence and The Pentagon "
            "Dealmaker Who Has Become Anthropic's Nemesis."
        ),
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_keeps_model_subject_led_summary(self, _mock_vision, _mock_call, _mock_bulk):
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
        self.assertIn("The Pentagon Dealmaker Who Has Become Anthropic's Nemesis", summary)
        self.assertIn("The Wall Street Journal sent a newsletter", summary)

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "Summary: Walmart is promoting Member Week savings with easy store pickup, fast free shipping, "
            "and a low price guarantee from March 6 to 12, 2026."
        ),
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_only_lightly_cleans_model_output(
        self,
        _mock_vision,
        _mock_call,
        _mock_bulk,
    ):
        summary = ollama_client.summarize_email(_promo_offer_email(), email_id="promo-lead-1")

        self.assertIsNotNone(summary)
        self.assertEqual(
            summary,
            "Walmart is promoting Member Week savings with easy store pickup, fast free shipping, "
            "and a low price guarantee from March 6 to 12, 2026.",
        )

    @mock.patch("app.ollama_client._extractive_summary_fallback", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value="Summary: also, the client meeting still needs coverage by noon.",
    )
    @mock.patch(
        "app.ollama_client._sanitize_model_summary",
        return_value="also, the client meeting still needs coverage by noon.",
    )
    @mock.patch(
        "app.ollama_client._finalize_summary_text",
        return_value="The client meeting still needs coverage by noon.",
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_uses_sanitize_and_finalize_pipeline(
        self,
        _mock_vision,
        mock_finalize,
        mock_sanitize,
        _mock_call,
        _mock_fallback,
    ):
        email = _long_action_email()

        summary = ollama_client.summarize_email(email, email_id="cleanup-1")

        self.assertEqual(summary, "The client meeting still needs coverage by noon.")
        mock_sanitize.assert_called_once()
        self.assertTrue(
            any(
                call.args[0] == "also, the client meeting still needs coverage by noon."
                for call in mock_finalize.call_args_list
            )
        )

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "You need to confirm whether you can cover the client meeting by noon while preparing for the "
            "2 PM deck handoff and room setup."
        ),
    )
    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_preserves_model_wording(self, _mock_vision, _mock_bulk, _mock_call):
        email = _long_action_email()

        summary = ollama_client.summarize_email(email, email_id="copy-1")

        self.assertIsNotNone(summary)
        self.assertEqual(
            summary,
            "You need to confirm whether you can cover the client meeting by noon while preparing for the "
            "2 PM deck handoff and room setup.",
        )

    @mock.patch("app.ollama_client._bulk_newsletter_summary", return_value=None)
    @mock.patch("app.ollama_client._single_article_alert_summary", return_value=None)
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value="Summary unavailable.",
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    @mock.patch(
        "app.ollama_client._extractive_summary_fallback",
        return_value="The article also explains how sellers are responding to automated scraping in online shopping.",
    )
    def test_summarize_email_uses_fallback_for_placeholder_model_response(
        self,
        _mock_fallback,
        _mock_vision,
        _mock_call,
        _mock_single_article,
        _mock_bulk,
    ):
        email = _wsj_alert_email()

        summary = ollama_client.summarize_email(email, email_id="copy-wsj-1")

        self.assertIsNotNone(summary)
        self.assertEqual(
            summary,
            "The article also explains how sellers are responding to automated scraping in online shopping.",
        )
        self.assertIn("automated scraping", summary.lower())

    def test_activity_summary_does_not_trigger_for_promotional_campaign_newsletter(self):
        self.assertIsNone(ollama_client._activity_notification_summary(_sobeys_paralympic_email()))

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value=(
            "A promotional update from Sobeys. It encourages support for Team Canada's Paralympic athletes "
            "at the Milano-Cortina 2026 Paralympic Games as part of the Feed the Dream campaign."
        ),
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_summarize_email_does_not_force_activity_template_for_campaign_promo(
        self,
        _mock_vision,
        mock_call,
    ):
        summary = ollama_client.summarize_email(
            _sobeys_paralympic_email(),
            email_id="sobeys-campaign-1",
        )

        self.assertIsNotNone(summary)
        self.assertIn("sobeys", summary.lower())
        self.assertIn("feed the dream", summary.lower())
        self.assertNotIn("activity update", summary.lower())
        self.assertNotIn("likes, comments, or follows", summary.lower())
        self.assertEqual(mock_call.call_count, 1)


if __name__ == "__main__":
    unittest.main()
