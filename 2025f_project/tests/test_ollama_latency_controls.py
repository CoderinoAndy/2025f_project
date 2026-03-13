import json
import unittest
from unittest import mock

from app import ollama_client


def _email_with_ai_fields():
    return {
        "id": 101,
        "sender": "Manager <manager@example.com>",
        "title": "Please confirm the schedule",
        "body": (
            "Please confirm whether you can cover the client meeting tomorrow afternoon. "
            "Let me know if you need anything else before then."
        ),
        "summary": "You need to confirm whether you can cover the client meeting tomorrow afternoon.",
        "ai_category": "urgent",
        "ai_needs_response": True,
        "ai_confidence": 0.93,
        "recipients": "",
        "cc": "",
    }


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self._payload).encode("utf-8")


class OllamaLatencyControlTests(unittest.TestCase):
    def setUp(self):
        self._saved_tasks = dict(ollama_client.AI_TASKS)
        self._saved_index = dict(ollama_client.AI_TASK_INDEX)
        ollama_client.AI_TASKS.clear()
        ollama_client.AI_TASK_INDEX.clear()

    def tearDown(self):
        ollama_client.AI_TASKS.clear()
        ollama_client.AI_TASK_INDEX.clear()
        ollama_client.AI_TASKS.update(self._saved_tasks)
        ollama_client.AI_TASK_INDEX.update(self._saved_index)

    @mock.patch("app.ollama_client.update_email_ai_fields")
    @mock.patch("app.ollama_client.summarize_email")
    @mock.patch("app.ollama_client.classify_email")
    def test_run_ai_analysis_skips_existing_fields_when_not_forced(
        self,
        mock_classify,
        mock_summarize,
        mock_update,
    ):
        changed = ollama_client.run_ai_analysis(_email_with_ai_fields(), force=False)

        self.assertFalse(changed)
        mock_classify.assert_not_called()
        mock_summarize.assert_not_called()
        mock_update.assert_not_called()

    def test_call_ollama_includes_keep_alive_hint(self):
        captured = {}

        def _fake_urlopen(request_obj, timeout=0):
            captured["timeout"] = timeout
            captured["payload"] = json.loads(request_obj.data.decode("utf-8"))
            return _FakeResponse({"message": {"content": "ok"}})

        with mock.patch("app.ollama_client._api_url_candidates", return_value=["http://127.0.0.1:11434/api/chat"]), mock.patch(
            "app.ollama_client._endpoint_allowed",
            return_value=True,
        ), mock.patch("app.ollama_client._model_name", return_value="mistral-small3.2:24b"), mock.patch(
            "app.ollama_client._resolved_model_name",
            return_value="mistral-small3.2:24b",
        ), mock.patch("app.ollama_client._timeout_seconds", return_value=12.0), mock.patch(
            "app.ollama_client._keep_alive_value",
            return_value="15m",
        ), mock.patch(
            "app.ollama_client.urllib.request.urlopen",
            side_effect=_fake_urlopen,
        ):
            result = ollama_client._call_ollama(
                task="summarize",
                messages=[{"role": "user", "content": "Summarize this."}],
                email_id="latency-1",
                num_predict=320,
            )

        self.assertEqual(result, "ok")
        self.assertEqual(captured["timeout"], 12.0)
        self.assertEqual(captured["payload"]["keep_alive"], "15m")
        self.assertEqual(captured["payload"]["options"]["num_predict"], 320)

    def test_force_analysis_uses_separate_task_slot(self):
        task_auto, created_auto = ollama_client._create_or_get_ai_task("analyze", 7, force=False)
        task_force, created_force = ollama_client._create_or_get_ai_task("analyze", 7, force=True)
        task_auto_repeat, created_auto_repeat = ollama_client._create_or_get_ai_task(
            "analyze",
            7,
            force=False,
        )

        self.assertTrue(created_auto)
        self.assertTrue(created_force)
        self.assertFalse(created_auto_repeat)
        self.assertNotEqual(task_auto["id"], task_force["id"])
        self.assertEqual(task_auto["id"], task_auto_repeat["id"])


if __name__ == "__main__":
    unittest.main()
