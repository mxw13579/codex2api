"""Offline probe regressions: stdlib only, synthetic events and fake websocket.

Run from the repository root:
    python -m unittest discover -s scripts -p test_codex_ws_probe.py

No websocket-client installation, real credentials or network is needed.
"""

from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import types
import unittest
from unittest import mock
import uuid


_spec = importlib.util.spec_from_file_location(
    "codex_ws_probe_under_test", Path(__file__).with_name("codex_ws_probe.py")
)
assert _spec is not None and _spec.loader is not None
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)


ACCOUNT = {
    "label": "fixture-account",
    "token": "fixture-token-secret",
    "account_id": "fixture-account-id",
    "proxy": "socks5h://fixture-user:fixture%20password@proxy.invalid:1080",
}


def delta(text: str, output_index: int = 0, content_index: int = 0) -> dict:
    return {
        "type": "response.output_text.delta",
        "output_index": output_index,
        "content_index": content_index,
        "delta": text,
    }


def done(text: str, output_index: int = 0, content_index: int = 0) -> dict:
    return {
        "type": "response.output_text.done",
        "output_index": output_index,
        "content_index": content_index,
        "text": text,
    }


def completed(text: str | None = None) -> dict:
    output = [] if text is None else [{
        "type": "message",
        "id": "msg-987",
        "content": [{"type": "output_text", "text": text}],
    }]
    return {
        "type": "response.completed",
        "response": {"id": "resp-654", "model": "fixture-model", "output": output},
    }


class FakeWebSocket:
    def __init__(self, events):
        self.events = iter(events)
        self.sent = []
        self.recv_calls = 0
        self.closed = False

    def send(self, payload):
        self.sent.append(json.loads(payload))

    def recv(self):
        self.recv_calls += 1
        # Also bound regressions that fail to treat an empty close as terminal.
        if self.recv_calls > 100:
            raise AssertionError("fake receive loop exceeded its event budget")
        event = next(self.events, "")
        if isinstance(event, Exception):
            raise event
        if event is None or isinstance(event, (str, bytes)):
            return event
        return json.dumps(event)

    def close(self):
        self.closed = True


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.network_guard = mock.patch(
            "socket.create_connection", side_effect=AssertionError("network is forbidden")
        )
        self.network_guard.start()
        self.addCleanup(self.network_guard.stop)

    def install_fake(self, events, connect_error=None):
        self.ws = FakeWebSocket(events)
        self.connector = mock.Mock(return_value=self.ws, side_effect=connect_error)
        module = types.ModuleType("websocket")
        module.create_connection = self.connector
        return mock.patch.dict(sys.modules, {"websocket": module})

    def run_probe(self, events, *, account=None, connect_error=None, **options):
        args = argparse.Namespace(
            url="wss://upstream.invalid/responses",
            model="fixture-model",
            effort="low",
            prompt="Synthetic probe prompt.",
            timeout=0.1,
            html_dir=None,
        )
        vars(args).update(options)
        with self.install_fake(events, connect_error):
            return probe._probe(dict(ACCOUNT if account is None else account), args, 1)

    def assert_no_body(self, result, body):
        self.assertNotIn("answer_text", result)
        self.assertNotIn(body, json.dumps(result))

    def test_default_does_not_return_or_write_body(self):
        body = "synthetic-private-response-body"
        with mock.patch.object(Path, "open", side_effect=AssertionError("unexpected file write")) as file_open, \
                mock.patch.object(Path, "mkdir", side_effect=AssertionError("unexpected directory creation")) as mkdir:
            result = self.run_probe([delta(body), done(body), completed(body)])
        self.assertEqual(result["status"], "completed")
        self.assert_no_body(result, body)
        self.assertNotIn("html_file", result)
        self.assertNotIn("answer_candidates", result)
        file_open.assert_not_called()
        mkdir.assert_not_called()
        self.assertTrue(self.ws.closed)

    def test_body_that_looks_like_routing_metadata_is_not_echoed(self):
        body = "gpt-5.6-sol-codex-synthetic-private-abuse-id"
        result = self.run_probe([delta(body), done(body), completed(body)])
        self.assertEqual(result["status"], "completed")
        self.assert_no_body(result, body)
        self.assertNotIn("abuse_model_ids", result)
        self.assertNotIn("abuse_detected", result)

    def test_routing_metadata_is_still_reported(self):
        model = "gpt-5.6-sol-codex-fixture-abuse"
        event = {
            "type": "response.created",
            "response": {"model": model},
            "engine_ids": ["gpt56sol-codex-fixture"],
            "x-codex-primary-used-percent": 12,
        }
        result = self.run_probe([event, completed()])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["engine_family"], "sol")
        self.assertEqual(result["engine_ids"], ["gpt56sol-codex-fixture"])
        self.assertEqual(result["x-codex-primary-used-percent"], 12)
        self.assertEqual(result["abuse_model_ids"], [model])
        self.assertTrue(result["abuse_detected"])
        self.assertNotIn("answer_candidates", result)

    def test_default_cli_jsonl_contains_only_metadata(self):
        body = "synthetic-private-cli-response"
        exported = [{
            "id": ACCOUNT["label"],
            "credentials": {"access_token": ACCOUNT["token"]},
            "proxy_url": ACCOUNT["proxy"],
        }]
        stdout, stderr = io.StringIO(), io.StringIO()
        argv = [
            "codex_ws_probe.py", "--accounts", "-", "--concurrency", "1",
            "--url", "wss://upstream.invalid/responses",
        ]
        with self.install_fake([delta(body), completed(body)]), \
                mock.patch.object(sys, "argv", argv), \
                mock.patch.object(sys, "stdin", io.StringIO(json.dumps(exported))), \
                redirect_stdout(stdout), redirect_stderr(stderr), \
                mock.patch.object(Path, "open") as file_open:
            self.assertEqual(probe.main(), 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        result = json.loads(lines[0])
        self.assertEqual(result["status"], "completed")
        self.assert_no_body(result, body)
        for secret in (ACCOUNT["token"], ACCOUNT["proxy"], "fixture password"):
            self.assertNotIn(secret, stdout.getvalue() + stderr.getvalue())
        self.assertEqual(stderr.getvalue(), "")
        file_open.assert_not_called()

    def test_opt_in_html_deduplicates_deltas_done_and_terminal(self):
        body = "<html><body>synthetic answer 42</body></html>"
        events = [
            delta("<html><body>synthetic answer 4"), delta("2</body></html>"),
            done(body), done(body), delta("late duplicate must not be appended"),
            completed(body),
        ]
        with tempfile.TemporaryDirectory() as directory:
            html_dir = Path(directory) / "html"
            result = self.run_probe(events, html_dir=html_dir)
            self.assertEqual(result["status"], "completed")
            self.assert_no_body(result, body)
            self.assertEqual(result["answer_candidates"], ["42"])
            path = html_dir / result["html_file"]
            self.assertEqual(path.read_text(encoding="utf-8"), body)
            self.assertEqual(list(html_dir.iterdir()), [path])

    def test_numeric_candidates_are_extracted_after_chunk_aggregation(self):
        cases = [
            (["4", "2"], ["42"]),
            (["1", "23"], ["123"]),
            (["12", "34"], []),
            (["9", ".", "5"], []),
            (["7", " then ", "8 and 7"], ["7", "8"]),
            (["synthetic answer 42"], ["42"]),
        ]
        for chunks, expected in cases:
            with self.subTest(chunks=chunks):
                result = self.run_probe([*(delta(chunk) for chunk in chunks), completed()])
                self.assertEqual(result["status"], "completed")
                self.assertEqual(result.get("answer_candidates", []), expected)

    def test_done_is_authoritative_and_late_deltas_do_not_duplicate(self):
        result = self.run_probe([
            delta("41"), done("42"), done("42"), delta("99"), completed(),
        ])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["answer_candidates"], ["42"])

    def test_independent_content_parts_are_not_joined_as_one_number(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_probe([
                delta("2", output_index=1),
                delta("4", output_index=0),
                done("6", output_index=0, content_index=1),
                completed(),
            ], html_dir=Path(directory))
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["answer_candidates"], ["2", "4", "6"])
            self.assertEqual(
                (Path(directory) / result["html_file"]).read_text(encoding="utf-8"),
                "4\n6\n2",
            )

    def test_terminal_fallback_uses_only_output_text(self):
        body = "synthetic terminal answer 42"
        terminal = completed()
        terminal["response"]["output"] = [
            {"type": "reasoning", "id": "reason-321", "summary": [{"text": "111"}]},
            {"type": "function_call", "id": "call-654", "arguments": '{"answer": 222}'},
            {
                "type": "message", "id": "msg-987",
                "content": [
                    {"type": "output_text", "text": body, "annotations": [{"id": "555"}]},
                    {"type": "refusal", "refusal": "333"},
                    {"type": "input_text", "text": "444"},
                ],
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_probe([terminal], html_dir=Path(directory))
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["answer_candidates"], ["42"])
            self.assert_no_body(result, body)
            self.assertEqual(
                (Path(directory) / result["html_file"]).read_text(encoding="utf-8"), body,
            )

    def test_terminal_text_replaces_partial_stream(self):
        result = self.run_probe([delta("4"), completed("synthetic answer 42")])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["answer_candidates"], ["42"])

    def test_terminal_non_text_data_is_not_a_numeric_answer(self):
        outputs = [
            None, "id-123", {"text": "234"},
            [None, "345", {"content": "456"}, {"content": [None, {"text": "567"}]}],
            [{"content": [{"type": "output_text", "text": 678}]}],
        ]
        for output in outputs:
            with self.subTest(output=output):
                terminal = completed()
                terminal["response"]["output"] = output
                with mock.patch.object(probe, "_save_html") as save:
                    result = self.run_probe([terminal], html_dir=Path("unused-fixture-dir"))
                self.assertEqual(result["status"], "completed")
                self.assertNotIn("answer_candidates", result)
                self.assertNotIn("html_file", result)
                save.assert_not_called()

    def test_labels_cannot_traverse_paths_and_repeated_saves_are_unique(self):
        labels = [
            "../victim", "..\\victim", "/absolute/label", "C:\\absolute\\label",
            "../../outside", "CON", "nul\x00/../label", "账号/../../label", "x" * 300,
            "same-label", "same-label",
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            victim = root / "victim-1.html"
            victim.write_text("keep existing synthetic file", encoding="utf-8")
            html_dir = root / "html"
            names = set()
            for label in labels:
                with self.subTest(label=label):
                    result = self.run_probe(
                        [completed("synthetic saved text")],
                        account={**ACCOUNT, "label": label}, html_dir=html_dir,
                    )
                    self.assertEqual(result["status"], "completed")
                    name = result["html_file"]
                    self.assertRegex(name, r"\Aprobe-[0-9a-f]{32}\.html\Z")
                    self.assertNotIn(name, names)
                    names.add(name)
                    self.assertEqual((html_dir / name).resolve().parent, html_dir.resolve())
                    self.assertEqual((html_dir / name).read_text(encoding="utf-8"), "synthetic saved text")
            self.assertEqual(victim.read_text(encoding="utf-8"), "keep existing synthetic file")
            self.assertEqual(len(list(html_dir.iterdir())), len(labels))
            self.assertEqual(set(root.iterdir()), {victim, html_dir})

    def test_save_collision_never_overwrites_existing_file(self):
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(probe.uuid, "uuid4", return_value=uuid.UUID(int=1)):
            html_dir = Path(directory)
            name = probe._save_html(html_dir, "first synthetic body")
            with self.assertRaises(FileExistsError):
                probe._save_html(html_dir, "second synthetic body")
            self.assertEqual((html_dir / name).read_text(encoding="utf-8"), "first synthetic body")

    def test_explicit_proxy_tls_and_no_bypass_options(self):
        result = self.run_probe([completed()])
        self.assertEqual(result["status"], "completed")
        self.connector.assert_called_once()
        options = self.connector.call_args.kwargs
        self.assertEqual(options["http_proxy_host"], "proxy.invalid")
        self.assertEqual(options["http_proxy_port"], 1080)
        self.assertEqual(options["proxy_type"], "socks5h")
        self.assertEqual(options["http_proxy_auth"], ("fixture-user", "fixture password"))
        self.assertEqual(options["http_no_proxy"], [""])
        self.assertEqual(options["sslopt"], {"cert_reqs": ssl.CERT_REQUIRED, "check_hostname": True})
        self.assertEqual(options["redirect_limit"], 0)

    def test_proxy_schemes_and_default_ports(self):
        for scheme, port in (("http", 80), ("socks5", 1080), ("socks5h", 1080)):
            with self.subTest(scheme=scheme):
                options = probe._proxy_kwargs(f"{scheme}://proxy.invalid")
                self.assertEqual(options["proxy_type"], scheme)
                self.assertEqual(options["http_proxy_port"], port)

    def test_explicit_routes_override_environment_without_mutating_it(self):
        environment = {"https_proxy": "http://synthetic-secret.invalid:3128", "NO_PROXY": "*"}
        for proxy_url, expected in ((ACCOUNT["proxy"], [""]), ("", ["*"])):
            with self.subTest(proxy_url=proxy_url), mock.patch.dict(os.environ, environment):
                result = self.run_probe([completed()], account={**ACCOUNT, "proxy": proxy_url})
                self.assertEqual(result["status"], "completed")
                self.assertEqual(self.connector.call_args.kwargs["http_no_proxy"], expected)
                for key, value in environment.items():
                    self.assertEqual(os.environ[key], value)
                    self.assertNotIn(value, json.dumps(result))

    def test_missing_proxy_selects_direct_route(self):
        result = self.run_probe([completed()], account={**ACCOUNT, "proxy": ""})
        self.assertEqual(result["status"], "completed")
        options = self.connector.call_args.kwargs
        self.assertEqual(options["http_no_proxy"], ["*"])
        self.assertNotIn("http_proxy_host", options)
        self.assertNotIn("proxy_id", result)

    def test_invalid_proxy_never_falls_back_to_direct(self):
        for proxy_url in ("https://proxy.invalid", "file://proxy.invalid", "http://", "http://proxy.invalid:bad"):
            with self.subTest(proxy_url=proxy_url):
                result = self.run_probe([completed()], account={**ACCOUNT, "proxy": proxy_url})
                self.assertEqual(result["status"], "error")
                self.connector.assert_not_called()

    def test_insecure_or_credential_bearing_endpoint_is_rejected(self):
        for url in ("ws://upstream.invalid/responses", "wss:///missing-host", "wss://fixture-user:password@upstream.invalid"):
            with self.subTest(url=url):
                result = self.run_probe([completed()], url=url)
                self.assertEqual(result["status"], "error")
                self.connector.assert_not_called()

    def test_exception_messages_are_never_echoed(self):
        secrets = [
            ACCOUNT["token"], ACCOUNT["proxy"], ACCOUNT["account_id"],
            "fixture-user", "fixture password", "fixture%20password", "unrelated-sensitive-body",
        ]
        stderr, stdout = io.StringIO(), io.StringIO()
        with redirect_stderr(stderr), redirect_stdout(stdout):
            result = self.run_probe([], connect_error=RuntimeError(" ".join(secrets)))
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "RuntimeError")
        self.assertEqual(result["error"], "probe_failed")
        for secret in secrets:
            self.assertNotIn(secret, json.dumps(result) + stdout.getvalue() + stderr.getvalue())

    def test_exception_is_not_even_stringified(self):
        class UnprintableError(Exception):
            def __str__(self):
                raise AssertionError("exception text must not be inspected")

        result = self.run_probe([], connect_error=UnprintableError())
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "UnprintableError")
        self.assertEqual(result["error"], "probe_failed")

    def test_upstream_error_messages_and_partial_body_are_omitted(self):
        body = "synthetic partial response 42"
        for event_type in ("error", "response.failed"):
            with self.subTest(event_type=event_type), mock.patch.object(probe, "_save_html") as save:
                result = self.run_probe([
                    delta(body),
                    {"type": event_type, "error": {
                        "code": ACCOUNT["token"], "type": "fixture password",
                        "message": ACCOUNT["proxy"],
                    }},
                ], html_dir=Path("unused-fixture-dir"))
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error_type"], event_type)
                self.assert_no_body(result, body)
                for secret in (ACCOUNT["token"], ACCOUNT["proxy"], "fixture password"):
                    self.assertNotIn(secret, json.dumps(result))
                for key in ("error_keys", "error_message", "error_code", "error_reason", "answer_candidates", "html_file"):
                    self.assertNotIn(key, result)
                save.assert_not_called()
                self.assertTrue(self.ws.closed)

    def test_empty_close_frames_stop_without_spinning_or_saving(self):
        for frame in (None, "", b""):
            with self.subTest(frame=frame), mock.patch.object(probe, "_save_html") as save:
                result = self.run_probe([delta("synthetic partial body"), frame], html_dir=Path("unused-fixture-dir"))
                self.assertEqual(result["status"], "error")
                self.assertEqual(result["error_type"], "RuntimeError")
                self.assertEqual(self.ws.recv_calls, 2)
                self.assertTrue(self.ws.closed)
                save.assert_not_called()

    def test_non_json_and_non_object_frames_are_skipped(self):
        result = self.run_probe(["not-json", [], 123, completed()])
        self.assertEqual(result["status"], "completed")
        self.assertTrue(self.ws.closed)


if __name__ == "__main__":
    unittest.main()
