import unittest

from app.email_content import (
    decode_transfer_encoded_text,
    normalize_outgoing_text,
    prepare_html_email_document,
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

    def test_decode_transfer_encoded_text_preserves_8bit_html(self):
        html_body = (
            '<!doctype html><html><body>'
            '<p>Stock up on everyday essentials today \U0001f3c3\u200d\u2642\ufe0f\u200d\u27a1\ufe0f</p>'
            '<a href="https://click.example.com/?promo=E2&layout=20">Open in browser</a>'
            "</body></html>"
        )

        decoded = decode_transfer_encoded_text(
            html_body.encode("utf-8"),
            content_type='text/html; charset="utf-8"',
            transfer_encoding="8bit",
        )

        self.assertEqual(decoded, html_body)
        self.assertIn("\U0001f3c3", decoded)
        self.assertNotIn("\u00f0\x9f", decoded)

    def test_decode_transfer_encoded_text_skips_qp_decode_for_already_decoded_html(self):
        html_body = (
            '<!doctype html><html><head>'
            '<meta http-equiv="X-UA-Compatible" content="IE=edge">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            "</head><body><p>Hello</p></body></html>"
        )

        decoded = decode_transfer_encoded_text(
            html_body.encode("utf-8"),
            content_type='text/html; charset="utf-8"',
            transfer_encoding="quoted-printable",
        )

        self.assertEqual(decoded, html_body)
        self.assertIn("IE=edge", decoded)
        self.assertIn("device-width", decoded)

    def test_decode_transfer_encoded_text_skips_qp_decode_for_html_with_query_params(self):
        html_body = (
            '<!doctype html><html><head>'
            '<meta http-equiv="X-UA-Compatible" content="IE=edge">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '</head><body><a href="https://www.quora.com/qemail/tc?id=a6c0f428df8a482783edf43edfd36891&et=2">'
            "Read more</a></body></html>"
        )

        decoded = decode_transfer_encoded_text(
            html_body.encode("utf-8"),
            content_type='text/html; charset="utf-8"',
            transfer_encoding="quoted-printable",
        )

        self.assertEqual(decoded, html_body)
        self.assertIn("IE=edge", decoded)
        self.assertIn("device-width", decoded)
        self.assertIn("id=a6c0f428df8a482783edf43edfd36891", decoded)

    def test_prepare_html_email_document_wraps_fragment_and_drops_scripts(self):
        raw_html = (
            '<div class="story">Top story</div>'
            '<script>alert("x")</script>'
            '<a href="https://example.com/read">Read more</a>'
        )

        prepared = prepare_html_email_document(raw_html)

        self.assertIn("<!doctype html>", prepared.lower())
        self.assertIn("<base target=\"_blank\">", prepared)
        self.assertIn("Top story", prepared)
        self.assertNotIn("<script>", prepared.lower())
        self.assertIn("Read more", prepared)


if __name__ == "__main__":
    unittest.main()
