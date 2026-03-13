import unittest
from unittest import mock

from app import email_image_context, ollama_client


class HtmlImageContextTests(unittest.TestCase):
    def test_extract_html_image_candidates_filters_decorative_logo(self):
        html = """
        <div>Quarterly revenue update</div>
        <img alt="Company logo" src="https://example.com/logo.png" width="32" height="32">
        <p>Revenue grew 18 percent year over year.</p>
        <img alt="Q4 revenue chart" src="https://example.com/chart.png" width="640" height="360">
        """

        candidates = email_image_context.extract_html_image_candidates(html)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["alt"], "Q4 revenue chart")
        self.assertIn("Revenue grew 18 percent", candidates[0]["nearby_text"])

    @mock.patch("app.ollama_client._call_ollama", return_value=None)
    def test_classify_email_prompt_includes_cached_image_context(self, mock_call):
        email = {
            "sender": "Billing <billing@example.com>",
            "title": "Invoice attached",
            "body": "",
            "body_html": '<img alt="Invoice screenshot" src="https://example.com/invoice.png">',
            "recipients": "",
            "cc": "",
            "ai_image_context": (
                'Detected 1 potentially relevant email image(s).\n'
                '- Inline image: label "Invoice screenshot".'
            ),
            "ai_image_context_status": "ready",
        }

        ollama_client.classify_email(email, email_id="img-classify-1")

        messages = mock_call.call_args.kwargs["messages"]
        self.assertIn("Image context:", messages[1]["content"])
        self.assertIn("Invoice screenshot", messages[1]["content"])

    @mock.patch("app.ollama_client._call_ollama", return_value="The invoice image shows a balance due of $18.")
    def test_summarize_email_runs_for_short_email_when_image_context_exists(self, mock_call):
        email = {
            "sender": "Billing <billing@example.com>",
            "title": "Invoice attached",
            "body": "",
            "body_html": '<img alt="Invoice screenshot" src="https://example.com/invoice.png">',
            "recipients": "",
            "cc": "",
            "ai_image_context": (
                'Detected 1 potentially relevant email image(s).\n'
                '- Inline image: label "Invoice screenshot".\n'
                "- Vision/OCR notes: the invoice shows a balance due of $18."
            ),
            "ai_image_context_status": "ready",
        }

        summary = ollama_client.summarize_email(email, email_id="img-summary-1")

        self.assertIsNotNone(summary)
        self.assertIn("invoice", summary.lower())
        messages = mock_call.call_args.kwargs["messages"]
        self.assertIn("Image evidence:", messages[1]["content"])
        self.assertIn("balance due of $18", messages[1]["content"])

    @mock.patch("app.ollama_client.update_email_ai_fields")
    @mock.patch(
        "app.gmail_service.fetch_message_attachments",
        return_value=[
            {
                "filename": "invoice.png",
                "content": b"x" * 120_000,
                "content_type": "image/png",
            }
        ],
    )
    @mock.patch(
        "app.gmail_service.fetch_message_attachment_metadata",
        return_value=[
            {
                "filename": "invoice.png",
                "content_type": "image/png",
                "size": 120_000,
            }
        ],
    )
    @mock.patch(
        "app.ollama_client._call_ollama",
        return_value="- The image looks like an invoice showing $124.63 due on April 30.",
    )
    @mock.patch("app.ollama_client._vision_model_requested", return_value="llama3.2-vision")
    def test_ensure_email_image_context_builds_context_from_attachment_and_vision(
        self,
        _mock_vision_model,
        mock_call,
        _mock_attachment_metadata,
        _mock_attachments,
        mock_update,
    ):
        email = {
            "id": 42,
            "external_id": "ext-1",
            "sender": "Billing <billing@example.com>",
            "title": "Invoice attached",
            "body": "Please see attached invoice.",
            "recipients": "",
            "cc": "",
        }

        enriched = ollama_client._ensure_email_image_context(dict(email), force=True)

        self.assertEqual(enriched["ai_image_context_status"], "ready")
        self.assertIn("invoice.png", enriched["ai_image_context"])
        self.assertIn("Vision/OCR notes", enriched["ai_image_context"])
        self.assertIn("$124.63", enriched["ai_image_context"])
        self.assertIn("images", mock_call.call_args.kwargs["messages"][1])
        self.assertEqual(mock_update.call_args.kwargs["email_id"], 42)
        self.assertEqual(mock_update.call_args.kwargs["ai_image_context_status"], "ready")


if __name__ == "__main__":
    unittest.main()
