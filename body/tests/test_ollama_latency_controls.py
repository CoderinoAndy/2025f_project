import os
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
        ), mock.patch(
            "app.ollama_client._resolve_model_selection",
            return_value={
                "requested_model": "mistral-small3.2:24b",
                "resolved_model": "mistral-small3.2:24b",
                "available_models": ("mistral-small3.2:24b",),
                "substituted": False,
                "reason": "",
                "strict": False,
            },
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
        self.assertEqual(captured["payload"]["model"], "mistral-small3.2:24b")

    def test_call_ollama_uses_model_task_for_model_selection(self):
        captured = {}

        def _fake_urlopen(request_obj, timeout=0):
            captured["payload"] = json.loads(request_obj.data.decode("utf-8"))
            return _FakeResponse({"message": {"content": "ok"}})

        with mock.patch(
            "app.ollama_client._api_url_candidates",
            return_value=["http://127.0.0.1:11434/api/chat"],
        ), mock.patch(
            "app.ollama_client._endpoint_allowed",
            return_value=True,
        ), mock.patch(
            "app.ollama_client._resolve_model_selection",
            return_value={
                "requested_model": "qwen2.5:7b-instruct",
                "resolved_model": "qwen2.5:7b-instruct",
                "available_models": ("qwen2.5:7b-instruct",),
                "substituted": False,
                "reason": "",
                "strict": False,
            },
        ) as mock_resolve, mock.patch(
            "app.ollama_client._timeout_seconds",
            return_value=12.0,
        ), mock.patch(
            "app.ollama_client._keep_alive_value",
            return_value="15m",
        ), mock.patch(
            "app.ollama_client.urllib.request.urlopen",
            side_effect=_fake_urlopen,
        ):
            result = ollama_client._call_ollama(
                task="draft",
                model_task="classify",
                messages=[{"role": "user", "content": "Draft this."}],
                email_id="latency-model-task-1",
                num_predict=280,
            )

        self.assertEqual(result, "ok")
        mock_resolve.assert_called_once_with(
            task="classify",
            api_urls=["http://127.0.0.1:11434/api/chat"],
        )
        self.assertEqual(captured["payload"]["model"], "qwen2.5:7b-instruct")

    def test_classify_model_defaults_to_global_model_when_no_override_is_set(self):
        with mock.patch.dict(
            os.environ,
            {
                "OLLAMA_MODEL": "mistral-small3.2:24b",
            },
            clear=True,
        ):
            self.assertEqual(
                ollama_client._model_name(task="classify"),
                "mistral-small3.2:24b",
            )
            self.assertEqual(
                ollama_client._model_name(task="summarize"),
                "mistral-small3.2:24b",
            )

    def test_classify_model_defaults_to_fast_model_when_env_is_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                ollama_client._model_name(task="classify"),
                "qwen2.5:7b-instruct",
            )
            self.assertEqual(
                ollama_client._model_name(task="summarize"),
                "mistral-small3.2:24b",
            )

    def test_classify_model_uses_explicit_override_when_configured(self):
        with mock.patch.dict(
            os.environ,
            {
                "OLLAMA_MODEL": "mistral-small3.2:24b",
                "OLLAMA_CLASSIFY_MODEL": "phi4-mini:latest",
            },
            clear=True,
        ):
            self.assertEqual(
                ollama_client._model_name(task="classify"),
                "phi4-mini:latest",
            )
            self.assertEqual(
                ollama_client._model_name(task="summarize"),
                "mistral-small3.2:24b",
            )

    def test_num_predict_can_be_overridden_per_task(self):
        with mock.patch.dict(
            os.environ,
            {
                "OLLAMA_CLASSIFY_NUM_PREDICT": "77",
                "OLLAMA_SUMMARY_NUM_PREDICT": "210",
                "OLLAMA_DRAFT_NUM_PREDICT": "340",
            },
            clear=True,
        ):
            self.assertEqual(ollama_client._num_predict_for_task("classify", 96), 77)
            self.assertEqual(ollama_client._num_predict_for_task("summarize", 120), 210)
            self.assertEqual(ollama_client._num_predict_for_task("draft", 280), 340)

    def test_call_ollama_logs_model_substitution_when_requested_model_missing(self):
        captured = {}

        def _fake_urlopen(request_obj, timeout=0):
            captured["payload"] = json.loads(request_obj.data.decode("utf-8"))
            return _FakeResponse({"message": {"content": "ok"}})

        with mock.patch.dict(
            os.environ,
            {
                "OLLAMA_CLASSIFY_MODEL": "missing-model:latest",
            },
            clear=True,
        ), mock.patch(
            "app.ollama_client._api_url_candidates",
            return_value=["http://127.0.0.1:11434/api/chat"],
        ), mock.patch(
            "app.ollama_client._available_ollama_models",
            return_value=("llama3.2:3b", "mistral-small3.2:24b"),
        ), mock.patch(
            "app.ollama_client._endpoint_allowed",
            return_value=True,
        ), mock.patch(
            "app.ollama_client._timeout_seconds",
            return_value=12.0,
        ), mock.patch(
            "app.ollama_client._keep_alive_value",
            return_value="15m",
        ), mock.patch(
            "app.ollama_client.urllib.request.urlopen",
            side_effect=_fake_urlopen,
        ), mock.patch(
            "app.ollama_client._log_action",
        ) as mock_log:
            result = ollama_client._call_ollama(
                task="classify",
                messages=[{"role": "user", "content": "Classify this."}],
                email_id="latency-substitution-1",
            )

        self.assertEqual(result, "ok")
        self.assertEqual(captured["payload"]["model"], "llama3.2:3b")
        substitution_logs = [
            call.kwargs["detail"]
            for call in mock_log.call_args_list
            if call.kwargs.get("status") == "fallback"
        ]
        self.assertTrue(
            any(
                "ollama_model_substitution requested=missing-model:latest resolved=llama3.2:3b"
                in detail
                for detail in substitution_logs
            )
        )

    def test_call_ollama_returns_none_when_requested_model_missing_in_strict_mode(self):
        with mock.patch.dict(
            os.environ,
            {
                "OLLAMA_CLASSIFY_MODEL": "missing-model:latest",
                "OLLAMA_STRICT_MODEL_RESOLUTION": "1",
            },
            clear=True,
        ), mock.patch(
            "app.ollama_client._api_url_candidates",
            return_value=["http://127.0.0.1:11434/api/chat"],
        ), mock.patch(
            "app.ollama_client._available_ollama_models",
            return_value=("llama3.2:3b",),
        ), mock.patch(
            "app.ollama_client._endpoint_allowed",
            return_value=True,
        ), mock.patch(
            "app.ollama_client._log_action",
        ) as mock_log, mock.patch(
            "app.ollama_client.urllib.request.urlopen",
        ) as mock_urlopen:
            result = ollama_client._call_ollama(
                task="classify",
                messages=[{"role": "user", "content": "Classify this."}],
                email_id="latency-strict-1",
            )

        self.assertIsNone(result)
        mock_urlopen.assert_not_called()
        error_logs = [
            call.kwargs["detail"]
            for call in mock_log.call_args_list
            if call.kwargs.get("status") == "error"
        ]
        self.assertTrue(
            any("requested_ollama_model_unavailable requested=missing-model:latest" in detail for detail in error_logs)
        )

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
