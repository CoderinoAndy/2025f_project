import unittest

from app.email_content import (
    normalize_outgoing_text,
    repair_body_text,
    repair_header_text,
    repair_html_content,
)


class EmailContentRepairTests(unittest.TestCase):
    def test_repair_body_text_fixes_common_mojibake(self):
        clean_text = "McDonald's and EY say don't miss it."
        mojibake_text = clean_text.encode("utf-8").decode("cp1252")

        repaired = repair_body_text(mojibake_text)

        self.assertEqual(repaired, clean_text)

    def test_repair_body_text_prefers_html_over_entity_noise(self):
        noisy_plain = " ".join(["&#847;", "&zwnj;", "&#8199;", "&#65279;"] * 12)
        html_body = (
            "<html><body><p>The Latest &amp; Greatest from Apple is here. Grab yours today!</p>"
            "<p>Get MacBook Neo for as low as $399.99 with a qualifying trade-in.</p></body></html>"
        )

        repaired = repair_body_text(noisy_plain, html_body)

        self.assertIn("The Latest & Greatest from Apple is here.", repaired)
        self.assertNotIn("&#847;", repaired)

    def test_repair_header_text_fixes_common_mojibake(self):
        clean_text = "don't miss today's briefing"
        mojibake_text = clean_text.encode("utf-8").decode("cp1252")

        repaired = repair_header_text(mojibake_text)

        self.assertEqual(repaired, clean_text)

    def test_repair_html_content_fixes_broken_arrow_cta(self):
        clean_html = "<p><a href='https://example.com/play'>Play now -&gt;</a></p>"
        broken_html = clean_html.replace("-&gt;", "\u2192").encode("utf-8").decode("cp1252")

        repaired = repair_html_content(broken_html)

        self.assertIn("Play now ->", repaired)
        self.assertNotIn("\u00e2", repaired)

    def test_normalize_outgoing_text_replaces_risky_punctuation(self):
        raw_text = "Thanks for the update\u2014we\u2019ll review it\u2026"

        normalized = normalize_outgoing_text(raw_text)

        self.assertEqual(normalized, "Thanks for the update-we'll review it...")


if __name__ == "__main__":
    unittest.main()
