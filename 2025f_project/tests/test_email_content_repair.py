import unittest

from app.email_content import repair_body_text, repair_header_text


class EmailContentRepairTests(unittest.TestCase):
    def test_repair_body_text_fixes_common_mojibake(self):
        clean_text = "McDonald\u2019s and EY say don\u2019t miss it."
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
        clean_text = "don\u2019t miss today\u2019s briefing"
        mojibake_text = clean_text.encode("utf-8").decode("cp1252")

        repaired = repair_header_text(mojibake_text)

        self.assertEqual(repaired, clean_text)


if __name__ == "__main__":
    unittest.main()
