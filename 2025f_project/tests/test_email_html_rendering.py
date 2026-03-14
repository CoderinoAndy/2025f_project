import unittest
from unittest import mock

from app import create_app


class EmailHtmlRenderingTests(unittest.TestCase):
    @mock.patch("app.routes.trigger_background_sync")
    @mock.patch("app.routes._get_ai_task")
    def test_ai_task_status_does_not_trigger_background_sync(
        self,
        mock_get_ai_task,
        mock_background_sync,
    ):
        mock_get_ai_task.return_value = {
            "id": "task-123",
            "type": "analyze",
            "email_id": 123,
            "status": "running",
        }

        app = create_app()
        client = app.test_client()

        response = client.get("/api/ai-task/task-123")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "running")
        mock_background_sync.assert_not_called()

    @mock.patch("app.routes.trigger_background_sync")
    @mock.patch("app.routes.fetch_thread_emails", return_value=[])
    @mock.patch("app.routes._should_auto_analyze_email", return_value=False)
    @mock.patch("app.routes._can_generate_reply_draft", return_value=False)
    @mock.patch("app.routes.ai_enabled", return_value=False)
    @mock.patch("app.routes.fetch_email_by_id")
    def test_email_route_renders_prepared_html_document(
        self,
        mock_fetch_email,
        _mock_ai_enabled,
        _mock_can_draft,
        _mock_should_auto_analyze,
        _mock_thread_emails,
        _mock_background_sync,
    ):
        mock_fetch_email.return_value = {
            "id": 123,
            "external_id": "ext-123",
            "thread_id": "thread-123",
            "title": "Quora Digest",
            "sender": "Quora Digest <english-quora-digest@quora.com>",
            "recipients": "you@example.com",
            "cc": "",
            "body": "Plain-text fallback body",
            "body_html": (
                '<!doctype html><html><head>'
                '<meta http-equiv="X-UA-Compatible" content="IE=edge">'
                '<meta name="viewport" content="width=device-width,initial-scale=1">'
                '</head><body><div>Top story</div><script>alert("x")</script>'
                '<a href="https://www.quora.com/qemail/tc?id=a6c0f428df8a482783edf43edfd36891&et=2">Read more</a>'
                "</body></html>"
            ),
            "type": "read-only",
            "priority": 1,
            "is_read": True,
            "is_archived": True,
            "received_at": "2026-03-12 22:22:12",
            "date": "2026-03-12 22:22:12",
            "summary": "",
            "draft": "",
        }

        app = create_app()
        client = app.test_client()

        response = client.get("/email/123")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("message-html-frame", html)
        self.assertIn("message-text-fallback", html)
        self.assertIn("IE=edge", html)
        self.assertIn("device-width", html)
        self.assertIn("id=a6c0f428df8a482783edf43edfd36891", html)
        self.assertNotIn('<script>alert("x")</script>', html)
        _mock_background_sync.assert_not_called()


if __name__ == "__main__":
    unittest.main()
