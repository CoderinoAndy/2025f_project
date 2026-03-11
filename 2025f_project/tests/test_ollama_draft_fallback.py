import unittest

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


if __name__ == "__main__":
    unittest.main()
