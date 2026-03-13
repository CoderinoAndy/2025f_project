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


def _mock_vision_message(*_args, **_kwargs):
    return {
        "role": "user",
        "content": (
            "Sender and subject metadata:\n"
            "- From: Deals Team <deals@brand-mail.example>\n"
            "- Subject: 30% off today plus free shipping"
        ),
        "images": ["fake-vision-image"],
    }


class OllamaClassificationTests(unittest.TestCase):
    def test_promotional_store_blast_prefers_junk_uncertain(self):
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
        self.assertEqual(result["email_type"], "junk-uncertain")

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

    def test_general_brand_advertising_becomes_junk_uncertain(self):
        email = _email(
            "Style Desk <hello@fashion-brand.example>",
            "New arrivals for spring",
            (
                "Discover fresh styles and featured picks from the new collection. "
                "Shop the collection online for weekend looks."
            ),
        )

        assessment = ollama_client._junk_signal_assessment(email)
        result = ollama_client._heuristic_classification(email)

        self.assertIn("promotion_cta", assessment["families"])
        self.assertFalse(assessment["editorial_like"])
        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk-uncertain")

    def test_promo_title_uses_shared_promo_guardrails(self):
        email = _email(
            "Uber Eats <uber@uber.com>",
            "Score big with Lonzo's Kitchen today.",
            "GAME CHANGING DEALS",
        )

        assessment = ollama_client._junk_signal_assessment(email)
        result = ollama_client._heuristic_classification(email)

        self.assertTrue(assessment["bulk_signal"])
        self.assertTrue(assessment["commercial_promotion"])
        self.assertIn("promotion_title", assessment["families"])
        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk-uncertain")

    def test_campaign_newsletter_with_footer_noise_stays_promotional_junk(self):
        email = _email(
            "Sobeys <sobeys@em.sobeys.com>",
            "The excitement of the Paralympic Games continues!",
            (
                "Cheer on Team Canada at the Paralympic Games! "
                "Discover member exclusive grocery offers and inspiration from Sobeys through the Feed The Dream campaign. "
                "Team Canada's Paralympic athletes are currently competing, backed by communities and supporters "
                "who help Feed The Dream. Let's celebrate their dedication and cheer them on at the "
                "Milano-Cortina 2026 Paralympic Games. "
                "My Grocery Offers Flyer Inspiration Preferences Terms & Conditions Privacy Policy Unsubscribe."
            ),
        )

        assessment = ollama_client._junk_signal_assessment(email)
        result = ollama_client._heuristic_classification(email)

        self.assertTrue(assessment["commercial_promotion"])
        self.assertFalse(assessment["editorial_like"])
        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk-uncertain")

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

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value='{"category":"junk","needs_response":false,"priority":1,"confidence":0.96}',
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_promotional_model_output_stays_junk_uncertain(self, _mock_vision, mock_call):
        email = _email(
            "Deals Team <deals@brand-mail.example>",
            "30% off today plus free shipping",
            "Use code SPRING30 at checkout. Shop now. Manage preferences or unsubscribe.",
        )

        result = ollama_client.classify_email(email, email_id="promo-merge-1")

        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk-uncertain")
        self.assertLess(result["confidence"], ollama_client.JUNK_LOW_CONFIDENCE_THRESHOLD)
        self.assertEqual(mock_call.call_count, 1)

    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value='{"category":"informational","needs_response":false,"priority":1,"confidence":0.99}',
    )
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_promotional_guardrail_overrides_high_confidence_read_only_model_output(
        self,
        _mock_vision,
        mock_call,
    ):
        email = _email(
            "Uber Eats <uber@uber.com>",
            "Score big with Lonzo's Kitchen today.",
            "GAME CHANGING DEALS",
        )

        result = ollama_client.classify_email(email, email_id="promo-title-guardrail-1")

        self.assertEqual(result["category"], "junk")
        self.assertEqual(result["email_type"], "junk-uncertain")
        self.assertEqual(mock_call.call_count, 1)

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    @mock.patch("app.ollama_client._vision_user_message", side_effect=_mock_vision_message)
    def test_classifier_prompt_explicitly_defines_promotional_junk(self, _mock_vision, mock_call):
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
        self.assertIn("still use category=junk and lower confidence", system_prompt)
        self.assertIn("routine retail promotions and brand advertising", system_prompt)
        self.assertIn("Sender and subject metadata", user_prompt)
        self.assertIn("commercial_promotion_pattern", user_prompt)
        self.assertEqual(messages[1]["images"], ["fake-vision-image"])


if __name__ == "__main__":
    unittest.main()
