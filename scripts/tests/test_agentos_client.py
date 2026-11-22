"""Real loopback HTTP/SSE transport tests; no cluster, model or real credentials."""

from contextlib import contextmanager
import http.server
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("agentos_client", ROOT / "sdk/python/agentos_client.py")
sdk = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sdk)

TOKEN = "synthetic-test-token-" + "x" * 32
RUN = "a" * 32
OTHER = "b" * 32
AT = "2026-11-21T00:00:00Z"
EPOCH = "c" * 32
NEW_EPOCH = "d" * 32


def record(state="succeeded", run_id=RUN, count=2):
    return {"id": run_id, "state": state, "tenant_id": "synthetic-test-tenant",
            "events": [{"at": AT, "state": state} for _ in range(count)]}


def event(sequence, state="running", run_id=RUN):
    return {"sequence": sequence, "run_id": run_id, "at": AT, "state": state, "store_epoch": EPOCH}


def frame(value, event_type="run", event_id=None):
    prefix = "event: " + event_type + "\n"
    if event_type == "run" or event_id is not None:
        prefix += "id: " + ((value.get("store_epoch", EPOCH) + ":" + str(value.get("sequence"))) if event_id is None else str(event_id)) + "\n"
    return (prefix + "data: " + json.dumps(value, ensure_ascii=False) + "\n\n").encode()


def end(sequence, state="succeeded", run_id=RUN):
    return frame({"run_id": run_id, "state": state, "last_event_id": EPOCH + ":" + str(sequence), "store_epoch": EPOCH}, "end")


class Stub(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, router):
        super().__init__(("127.0.0.1", 0), Handler)
        self.router = router
        self.requests = []
        self.errors = []

    @property
    def url(self):
        return "http://127.0.0.1:" + str(self.server_port)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        self.dispatch()

    def do_POST(self):
        self.dispatch()

    def dispatch(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.server.requests.append({"method": self.command, "path": self.path,
                                     "headers": dict(self.headers), "body": body})
        try:
            self.server.router(self)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True  # Expected when a deadline closes a fixture stream.
        except Exception as error:
            self.server.errors.append(type(error).__name__)
            self.close_connection = True

    def raw(self, body=b"", status=200, content_type="application/json", headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        defaults = {"AgentOS-Store-Epoch": EPOCH}
        if "/events?" in self.path and status in (200, 204):
            defaults["AgentOS-Events-Version"] = "3"
        defaults.update(headers or {})
        for key, value in defaults.items():
            if value is not None:
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def json(self, value, status=200, headers=None):
        self.raw(json.dumps(value).encode(), status, headers=headers)

    def sse(self, body, headers=None):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Connection", "close")
        defaults = {"AgentOS-Events-Version": "3", "AgentOS-Store-Epoch": EPOCH}
        defaults.update(headers or {})
        for key, value in defaults.items():
            if value is not None:
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True


@contextmanager
def serving(router):
    server = Stub(router)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)
        if server.errors:
            raise AssertionError("local stub errors: " + str(server.errors))


class ClientTests(unittest.TestCase):
    def client(self, server, **kwargs):
        return sdk.AgentOSClient(server.url, token=TOKEN, reconnect_delay=0, **kwargs)

    def test_create_get_cancel_logs_and_wait_over_http(self):
        def router(h):
            if h.path == "/v1/runs" and h.command == "POST":
                h.json(record("queued", count=1), 202)
            elif "/events?" in h.path:
                h.sse(b": heartbeat\r\n\r\n" + frame(event(1, "queued")) + frame(event(2, "succeeded")) + end(2))
            elif h.path.endswith("/cancel"):
                h.json(record("cancelling"), 202)
            elif h.path.endswith("/logs"):
                h.json({"run_id": RUN, "logs": "tool output\n", "limit_bytes": 65536})
            else:
                h.json(record())

        with serving(router) as server:
            client = self.client(server)
            self.assertEqual(client.create_run("python", ["python", "-c", "print(1)"], 30, idempotency_key="persisted-key")["id"], RUN)
            self.assertEqual(client.wait(RUN, timeout=2)["state"], "succeeded")
            self.assertEqual(client.get_run(RUN)["id"], RUN)
            self.assertEqual(client.cancel(RUN)["state"], "cancelling")
            self.assertEqual(client.logs(RUN)["logs"], "tool output\n")
            request = server.requests[0]
            self.assertEqual(request["headers"]["Authorization"], "Bearer " + TOKEN)
            self.assertEqual(request["headers"]["Idempotency-Key"], "persisted-key")
            self.assertEqual(json.loads(request["body"]), {"template": "python", "command": ["python", "-c", "print(1)"], "timeout_seconds": 30})
            streams = [r for r in server.requests if "/events?" in r["path"]]
            self.assertEqual({k.lower(): v for k, v in streams[0]["headers"].items()}["agentos-events-version"], "3")

    def test_coded_json_records_and_legacy_omissions_remain_compatible(self):
        cases = [(state, None) for state in sorted(sdk.STATES)]
        cases += [("succeeded", "completed"), ("cancelled", "cancelled"), ("cancelling", "cancelled")]
        cases += [(state, code) for state in ("failed", "cancelling") for code in sorted(sdk.FAILURE_REASON_CODES)]
        for state, code in cases:
            value = record(state, count=0)
            value["reason"] = "Diagnostic text says budget_exceeded; this is not a classifier."
            if code is not None:
                value["reason_code"] = code
            with self.subTest(state=state, reason_code=code), serving(lambda h: h.json(value)) as server:
                self.assertEqual(self.client(server).get_run(RUN), value)

        def router(h):
            state, code = ("cancelling", "cancelled") if h.path.endswith("/cancel") else ("succeeded", "completed")
            h.json(dict(record(state), reason_code=code), 200)

        with serving(router) as server:
            client = self.client(server)
            self.assertEqual(client.create_run("python", idempotency_key="existing-coded-run")["reason_code"], "completed")
            self.assertEqual(client.cancel(RUN)["reason_code"], "cancelled")

    def test_invalid_record_reason_codes_fail_sanitized(self):
        invalid = [("failed", code) for code in ("", None, True, 42, [], {}, "made_up", TOKEN, "completed", "cancelled")]
        invalid += [("succeeded", "task_error"), ("cancelled", "completed"), ("cancelling", "completed")]
        invalid += [(state, "unknown") for state in ("queued", "starting", "activating", "running")]
        for state, code in invalid:
            value = dict(record(state), reason_code=code)
            with self.subTest(state=state, code_type=type(code).__name__), serving(lambda h: h.json(value)) as server:
                with self.assertRaises(sdk.AgentOSProtocolError) as caught:
                    self.client(server).get_run(RUN)
                self.assertNotIn(TOKEN, str(caught.exception))

        # The same validator applies to creation and cancellation responses.
        with serving(lambda h: h.json(dict(record("succeeded"), reason_code="cancelled"))) as server:
            client = self.client(server)
            with self.assertRaises(sdk.AgentOSProtocolError):
                client.create_run("python", idempotency_key="invalid-coded-response")
            with self.assertRaises(sdk.AgentOSProtocolError):
                client.cancel(RUN)

    def test_record_history_reason_code_is_validated_and_preserved(self):
        value = dict(record("succeeded", count=1), reason_code="completed")
        value["events"][0]["reason_code"] = "task_error"
        with serving(lambda h: h.json(value)) as server:
            with self.assertRaises(sdk.AgentOSProtocolError):
                self.client(server).get_run(RUN)
            value["events"][0]["reason_code"] = "completed"
            self.assertEqual(self.client(server).get_run(RUN), value)

    def test_coded_sse_reconnect_preserves_code_reason_and_auth(self):
        coded = dict(event(2, "failed"), reason_code="infra_failure", reason="untrusted text says completed")

        def router(h):
            after = parse_qs(urlsplit(h.path).query)["after"][0]
            h.sse(frame(event(1, "queued")) if after == "0" else frame(coded) + end(2, "failed"))

        with serving(router) as server:
            values = list(self.client(server).iter_events(RUN, timeout=2))
            self.assertEqual(values, [event(1, "queued"), coded])
            self.assertEqual([r["path"].split("after=")[1] for r in server.requests], ["0", EPOCH + ":1"])
            self.assertTrue(all(r["headers"]["Authorization"] == "Bearer " + TOKEN for r in server.requests))

    def test_invalid_reason_code_never_advances_resumable_cursor(self):
        invalid = [("failed", code) for code in ("made_up", "", None, True, [], {}, "completed", "cancelled", TOKEN)]
        invalid += [("running", "task_error"), ("cancelling", "completed"), ("succeeded", "unknown")]
        for state, code in invalid:
            bad = dict(event(2, state), reason_code=code)
            fixed = dict(event(2, "failed"), reason_code="infra_failure")

            def router(h):
                after = parse_qs(urlsplit(h.path).query)["after"][0]
                h.sse(frame(event(1, "queued")) + frame(bad) if after == "0" else frame(fixed) + end(2, "failed"))

            with self.subTest(state=state, code_type=type(code).__name__), serving(router) as server:
                client = self.client(server)
                events = client.iter_events(RUN, timeout=2)
                last_validated = next(events)["sequence"]
                with self.assertRaises(sdk.AgentOSProtocolError) as caught:
                    next(events)
                self.assertNotIn(TOKEN, str(caught.exception))
                self.assertEqual(last_validated, 1)
                self.assertEqual(len(server.requests), 1)  # Malformed data is not retried automatically.
                # The caller resumes using the last event it actually received;
                # sequence 2 must still be delivered rather than silently skipped.
                self.assertEqual(list(client.iter_events(RUN, after=EPOCH + ":" + str(last_validated), timeout=2)), [fixed])
                self.assertEqual([r["path"].split("after=")[1] for r in server.requests], ["0", EPOCH + ":1"])
                self.assertTrue(all(r["headers"]["Authorization"] == "Bearer " + TOKEN for r in server.requests))

    def test_required_idempotency_and_policy_input_validation_precede_network(self):
        with serving(lambda h: h.json(record())) as server:
            client = self.client(server)
            with self.assertRaises(TypeError):
                client.create_run("python")
            for key in ("", "bad\nkey", "x" * 129):
                with self.subTest(key_length=len(key)), self.assertRaises(ValueError):
                    client.create_run("python", idempotency_key=key)
            with self.assertRaises(TypeError):
                client.create_run("python", idempotency_key="stable", image="arbitrary")
            with self.assertRaises(ValueError):
                client.create_run("python", timeout_seconds=True, idempotency_key="stable")
            with self.assertRaises(ValueError):
                client.get_run("../other-tenant")
            with self.assertRaises(ValueError):
                list(client.iter_events(RUN, after=True))
            self.assertEqual(server.requests, [])

    def test_create_never_auto_retries_and_same_key_is_caller_owned(self):
        def router(h):
            if len(h.server.requests) == 1:
                h.json({"error": TOKEN}, 503)
            else:
                h.json(record("queued"))

        with serving(router) as server:
            client = self.client(server)
            with self.assertRaises(sdk.AgentOSError) as caught:
                client.create_run("python", idempotency_key="recoverable-key")
            self.assertEqual(len(server.requests), 1)
            self.assertEqual(caught.exception.status_code, 503)
            self.assertNotIn(TOKEN, str(caught.exception))
            self.assertEqual(client.create_run("python", idempotency_key="recoverable-key")["id"], RUN)
            self.assertEqual(server.requests[0]["body"], server.requests[1]["body"])
            self.assertEqual([r["headers"]["Idempotency-Key"] for r in server.requests], ["recoverable-key"] * 2)

    def test_reconnect_only_commits_complete_validated_frames(self):
        def router(h):
            after = parse_qs(urlsplit(h.path).query)["after"][0]
            if after == "0":
                h.sse(frame(event(1, "queued")) + b'event: run\nid: 2\ndata: {"interrupted":')
            elif after == EPOCH + ":1":
                value = event(2, "succeeded")
                value["reason"] = "résumé"
                # Legal multiline SSE data and a comment within the frame.
                body = ("event: run\nid: " + EPOCH + ":2\n: heartbeat\ndata: {\n").encode()
                body += ("data: " + json.dumps(value, ensure_ascii=False)[1:] + "\n\n").encode()
                h.sse(body + end(2))
            else:
                h.json({}, 500)

        with serving(router) as server:
            result = list(self.client(server).iter_events(RUN, timeout=2))
            self.assertEqual([e["sequence"] for e in result], [1, 2])
            self.assertEqual(result[1]["reason"], "résumé")
            self.assertEqual([r["path"].split("after=")[1] for r in server.requests], ["0", EPOCH + ":1"])

    def test_terminal_204_requires_matching_terminal_record(self):
        def router(h):
            h.raw(status=204) if "/events?" in h.path else h.json(record())

        with serving(router) as server:
            client = self.client(server)
            self.assertEqual(list(client.iter_events(RUN, after=EPOCH + ":2", timeout=2)), [])
            self.assertEqual(client.wait(RUN, after=EPOCH + ":2", timeout=2)["state"], "succeeded")
        for state, count in (("running", 2), ("succeeded", 1)):
            with self.subTest(state=state, count=count):
                def invalid(h):
                    h.raw(status=204) if "/events?" in h.path else h.json(record(state, count=count))

                with serving(invalid) as server, self.assertRaises(sdk.AgentOSProtocolError):
                    list(self.client(server).iter_events(RUN, after=EPOCH + ":2", timeout=2))

    def test_malformed_or_cross_run_events_fail_without_reconnect(self):
        cases = [
            frame(event(1, run_id=OTHER)), frame(event(2)), frame(event(True)),
            frame(dict(event(1), state=["running"])), frame(dict(event(1), at="no timezone")),
            frame(event(1), event_id=2), b"event: run\nid: 1\ndata: []\n\n",
            b"event: run\nid: 1\nid: 1\ndata: {}\n\n",
            b"event: unknown\ndata: {}\n\n", b"event: run\nid: 1\ndata: {\n\n",
            b'event: run\nid: 1\ndata: {"sequence":1,"sequence":2}\n\n',
            frame(event(1, "running")) + end(1), frame(event(1, "succeeded")) + end(2),
            frame(event(1, "succeeded")) + end(1, "failed"),
            frame(event(1, "succeeded")) + frame(event(2, "running")),
        ]
        for body in cases:
            with self.subTest(body_prefix=body[:30]):
                with serving(lambda h: h.sse(body)) as server:
                    with self.assertRaises(sdk.AgentOSProtocolError):
                        list(self.client(server).iter_events(RUN, timeout=1))
                    self.assertEqual(len(server.requests), 1)

    def test_cursors_are_scoped_to_each_run_and_token_snapshot(self):
        def router(h):
            run_id = h.path.split("/")[3]
            h.sse(frame(event(1, "succeeded", run_id)) + end(1, run_id=run_id))

        with serving(router) as server:
            with patch.dict(os.environ, {"SYNTHETIC_AGENTOS_TOKEN": TOKEN}):
                client = sdk.AgentOSClient(server.url, token_env="SYNTHETIC_AGENTOS_TOKEN")
            with patch.dict(os.environ, {"SYNTHETIC_AGENTOS_TOKEN": "different-synthetic-token"}):
                self.assertEqual(list(client.iter_events(RUN, timeout=1))[0]["run_id"], RUN)
                self.assertEqual(list(client.iter_events(OTHER, timeout=1))[0]["run_id"], OTHER)
            self.assertEqual([r["path"].split("after=")[1] for r in server.requests], ["0", "0"])
            self.assertTrue(all(r["headers"]["Authorization"] == "Bearer " + TOKEN for r in server.requests))
            self.assertNotIn(TOKEN, repr(client))

    def test_restore_safe_cursor_validation_precedes_network(self):
        invalid = [True, False, 1, 2, sdk.MAX_CURSOR, -1, 0.0, "0", "1", "",
                   EPOCH + ":01", EPOCH + ":-1", EPOCH.upper() + ":1", EPOCH + ":" + str(sdk.MAX_CURSOR + 1),
                   EPOCH + ":1\n", "x" * 1024]
        with serving(lambda h: h.sse(frame(event(1, "succeeded")) + end(1))) as server:
            client = self.client(server)
            for cursor in invalid:
                with self.subTest(cursor_type=type(cursor).__name__), self.assertRaises(ValueError):
                    list(client.iter_events(RUN, after=cursor))
            self.assertEqual(server.requests, [])
            self.assertEqual(list(client.iter_events(RUN, after=EPOCH + ":0", timeout=1))[0]["sequence"], 1)
            self.assertEqual(parse_qs(urlsplit(server.requests[0]["path"]).query)["after"], [EPOCH + ":0"])

    def test_format_three_requires_explicit_version_and_valid_epoch(self):
        invalid = [{"AgentOS-Events-Version": version} for version in (None, "1", "2", "3, 3")]
        invalid += [{"AgentOS-Store-Epoch": epoch} for epoch in (None, "", "A" * 32, "g" * 32, TOKEN)]
        for headers in invalid:
            with self.subTest(header=next(iter(headers))), serving(lambda h: h.sse(frame(event(1, "succeeded")) + end(1), headers=headers)) as server:
                with self.assertRaises(sdk.AgentOSProtocolError) as caught:
                    list(self.client(server).iter_events(RUN, timeout=1))
                self.assertNotIn(TOKEN, str(caught.exception))
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(server.requests[0]["headers"]["AgentOS-Events-Version"], "3")

    def test_epoch_conflict_is_not_retried_or_downgraded_even_before_first_event(self):
        for delivered in (0, 1):
            def router(h):
                if len(h.server.requests) == 1:
                    h.sse(frame(event(1, "queued")) if delivered else b": heartbeat\n\n")
                else:
                    h.json({"error": {"code": "store_epoch_mismatch", "message": TOKEN}}, 409)

            with self.subTest(delivered=delivered), serving(router) as server:
                values = self.client(server).iter_events(RUN, timeout=2)
                if delivered:
                    self.assertEqual(next(values)["sequence"], 1)
                with self.assertRaises(sdk.AgentOSError) as caught:
                    list(values)
                self.assertEqual(caught.exception.status_code, 409)
                self.assertFalse(caught.exception.retryable)
                self.assertNotIn(TOKEN, str(caught.exception))
                self.assertEqual([r["path"].split("after=")[1] for r in server.requests], ["0", EPOCH + ":" + str(delivered)])
                self.assertTrue(all(r["headers"]["AgentOS-Events-Version"] == "3" for r in server.requests))
                self.assertTrue(all(r["headers"]["Authorization"] == "Bearer " + TOKEN for r in server.requests))

    def test_changed_epoch_on_successful_reconnect_fails_before_new_history(self):
        def router(h):
            if len(h.server.requests) == 1:
                h.sse(frame(event(1, "queued")))
            else:
                h.sse(frame(dict(event(2, "succeeded"), store_epoch=NEW_EPOCH)), headers={"AgentOS-Store-Epoch": NEW_EPOCH})

        with serving(router) as server:
            values = self.client(server).iter_events(RUN, timeout=2)
            self.assertEqual(next(values)["sequence"], 1)
            with self.assertRaises(sdk.AgentOSProtocolError) as caught:
                next(values)
            self.assertEqual(caught.exception.code, "store_epoch_mismatch")
            self.assertEqual(len(server.requests), 2)
            self.assertTrue(server.requests[1]["path"].endswith("after=" + EPOCH + ":1"))

    def test_frame_epoch_and_id_must_match_negotiated_history(self):
        missing = event(1, "succeeded")
        del missing["store_epoch"]
        invalid = [frame(missing), frame(dict(event(1, "succeeded"), store_epoch=NEW_EPOCH)),
                   frame(event(1, "succeeded"), event_id=NEW_EPOCH + ":1"), frame(event(1, "succeeded"), event_id=1)]
        for payload in ({"run_id": RUN, "state": "succeeded", "last_event_id": EPOCH + ":1"},
                        {"run_id": RUN, "state": "succeeded", "last_event_id": EPOCH + ":1", "store_epoch": NEW_EPOCH},
                        {"run_id": RUN, "state": "succeeded", "last_event_id": NEW_EPOCH + ":1", "store_epoch": EPOCH},
                        {"run_id": RUN, "state": "succeeded", "last_event_id": 1, "store_epoch": EPOCH}):
            invalid.append(frame(event(1, "succeeded")) + frame(payload, "end"))
        for body in invalid:
            with serving(lambda h: h.sse(body)) as server:
                with self.assertRaises(sdk.AgentOSProtocolError):
                    list(self.client(server).iter_events(RUN, timeout=1))
                self.assertEqual(len(server.requests), 1)

    def test_terminal_confirmation_rejects_restore_or_missing_epoch(self):
        for catchup in (False, True):
            for observed in (NEW_EPOCH, None):
                def router(h):
                    if "/events?" in h.path:
                        h.raw(status=204) if catchup else h.sse(frame(event(1, "running")) + frame(event(2, "succeeded")) + end(2))
                    else:
                        h.json(record(), headers={"AgentOS-Store-Epoch": observed})

                with self.subTest(catchup=catchup, observed=observed), serving(router) as server:
                    with self.assertRaises(sdk.AgentOSProtocolError) as caught:
                        self.client(server).wait(RUN, after=EPOCH + ":2" if catchup else 0, timeout=1)
                    self.assertEqual(caught.exception.code, "invalid_store_epoch" if observed is None else "store_epoch_mismatch")
                    self.assertEqual(len(server.requests), 2)

    def test_event_get_retry_is_bounded_and_status_aware(self):
        def router(h):
            if len(h.server.requests) == 1:
                h.json({"error": TOKEN}, 503)
            else:
                h.sse(frame(event(1, "succeeded")) + end(1))

        with serving(router) as server:
            self.assertEqual(len(list(self.client(server).iter_events(RUN, timeout=2))), 1)
            self.assertEqual(len(server.requests), 2)
        with serving(lambda h: h.json({"error": TOKEN}, 401)) as server:
            with self.assertRaises(sdk.AgentOSError) as caught:
                list(self.client(server).iter_events(RUN, timeout=1))
            self.assertEqual(caught.exception.status_code, 401)
            self.assertEqual(len(server.requests), 1)
        with serving(lambda h: h.sse(b": heartbeat\n\n")) as server:
            with self.assertRaises(sdk.AgentOSError) as caught:
                list(self.client(server, max_reconnects=2).iter_events(RUN, timeout=2))
            self.assertEqual(caught.exception.code, "event_reconnect_limit")
            self.assertEqual(len(server.requests), 3)

    def test_redirect_and_environment_proxies_never_receive_token(self):
        with serving(lambda h: h.json({})) as destination:
            def redirect(h):
                h.raw(TOKEN.encode(), 302, headers={"Location": destination.url + "/credential-trap"})

            with serving(redirect) as server:
                with patch.dict(os.environ, {"HTTP_PROXY": destination.url, "HTTPS_PROXY": destination.url, "ALL_PROXY": destination.url,
                                             "http_proxy": destination.url, "https_proxy": destination.url, "all_proxy": destination.url, "NO_PROXY": "", "no_proxy": ""}):
                    with self.assertRaises(sdk.AgentOSError) as caught:
                        self.client(server).get_run(RUN)
                self.assertEqual(caught.exception.code, "redirect_refused")
                self.assertNotIn(TOKEN, str(caught.exception))
                self.assertNotIn(server.url, str(caught.exception))
                self.assertEqual(destination.requests, [])

    def test_plain_http_origins_and_header_injection_are_rejected(self):
        for url in ("http://example.com", "http://169.254.169.254", "https://u:p@example.com", "https://example.com/private",
                    "https://example.com?token=secret", "http://127.0.0.1:0", "http://127.0.0.1\n"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                sdk.AgentOSClient(url, token=TOKEN)
        for token in (TOKEN + "\n", "Bearer " + TOKEN):
            with self.assertRaises(ValueError):
                sdk.AgentOSClient("https://example.com", token=token)

    def test_hard_deadline_interrupts_drip_fed_partial_sse_line(self):
        def router(h):
            h.send_response(200)
            h.send_header("Content-Type", "text/event-stream")
            h.send_header("AgentOS-Events-Version", "3")
            h.send_header("AgentOS-Store-Epoch", EPOCH)
            h.send_header("Connection", "close")
            h.end_headers()
            try:
                for _ in range(100):
                    h.wfile.write(b"x")
                    h.wfile.flush()
                    time.sleep(0.01)
            finally:
                h.close_connection = True

        with serving(router) as server:
            started = time.monotonic()
            with self.assertRaises(sdk.AgentOSTimeout):
                list(self.client(server, stream_timeout=1).iter_events(RUN, timeout=0.12))
            self.assertLess(time.monotonic() - started, 0.8)

    def test_consumer_pause_expires_buffered_run_and_end_frames(self):
        bodies = [
            frame(event(1, "running")) + frame(event(2, "succeeded")) + end(2),
            frame(event(1, "succeeded")) + end(1),
        ]
        for body in bodies:
            with self.subTest(buffered_terminal_only=body == bodies[1]):
                original_read = sdk._Exchange.read
                reads = []

                def coalesced_read(exchange, limit):
                    # Coalesce actual HTTP response bytes into one SDK read so
                    # TCP chunk boundaries cannot hide the buffered-frame bug.
                    chunk = bytearray(original_read(exchange, limit))
                    if not reads:
                        while len(chunk) < len(body):
                            part = original_read(exchange, limit)
                            if not part:
                                break
                            chunk.extend(part)
                    reads.append(bytes(chunk))
                    return bytes(chunk)

                with serving(lambda h: h.sse(body)) as server, patch.object(sdk._Exchange, "read", coalesced_read):
                    events = self.client(server).iter_events(RUN, timeout=0.2)
                    try:
                        self.assertEqual(next(events)["sequence"], 1)
                        self.assertEqual(reads, [body])
                        time.sleep(0.25)  # Consumer work counts against the iterator timeout.
                        with self.assertRaises(sdk.AgentOSTimeout):
                            next(events)
                        self.assertEqual(len(reads), 1)  # The deadline was checked without another socket read.
                        self.assertEqual(len(server.requests), 1)
                    finally:
                        events.close()

    def test_exhausted_connection_budget_cannot_send_a_late_post(self):
        original_connect = sdk.http.client.HTTPConnection.connect

        def delayed_connect(connection):
            original_connect(connection)  # A real loopback socket, not a fake transport.
            time.sleep(0.08)

        with serving(lambda h: h.json(record("queued"), 202)) as server:
            client = self.client(server, request_timeout=0.04)
            # Simulate a watchdog thread which has not been scheduled yet. The
            # explicit post-connect deadline check must still prevent the POST.
            with patch.object(sdk.http.client.HTTPConnection, "connect", delayed_connect), patch.object(sdk._Exchange, "arm"):
                with self.assertRaises(sdk.AgentOSTimeout):
                    client.create_run("python", idempotency_key="must-not-be-admitted")
            self.assertEqual(server.requests, [])

    def test_oversized_lines_and_response_bodies_fail_closed(self):
        with serving(lambda h: h.sse(b"x" * (sdk.MAX_LINE_BYTES + 1))) as server:
            with self.assertRaises(sdk.AgentOSProtocolError):
                list(self.client(server).iter_events(RUN, timeout=1))
        with serving(lambda h: h.raw(b"x" * (sdk.MAX_RESPONSE_BYTES + 1))) as server:
            with self.assertRaises(sdk.AgentOSProtocolError):
                self.client(server).get_run(RUN)

    def test_wait_confirms_terminal_state_and_logs_identity(self):
        def router(h):
            if "/events?" in h.path:
                h.sse(frame(event(1, "succeeded")) + end(1))
            elif h.path.endswith("/logs"):
                h.json({"run_id": OTHER, "logs": "wrong run", "limit_bytes": 65536})
            else:
                h.json(record("running"))

        with serving(router) as server:
            client = self.client(server)
            with self.assertRaises(sdk.AgentOSProtocolError):
                client.wait(RUN, timeout=1)
            with self.assertRaises(sdk.AgentOSProtocolError):
                client.logs(RUN)

    def test_launch_example_is_runnable_against_stub(self):
        def router(h):
            if h.command == "POST":
                h.json(record("queued"), 202)
            elif "/events?" in h.path:
                h.sse(frame(event(1, "succeeded")) + end(1))
            elif h.path.endswith("/logs"):
                h.json({"run_id": RUN, "logs": '{"task":"sum-of-squares","result":332833500}\n', "limit_bytes": 65536})
            else:
                h.json(record(count=1))

        with serving(router) as server:
            environment = dict(os.environ, SYNTHETIC_AGENTOS_TOKEN=TOKEN)
            result = subprocess.run([sys.executable, str(ROOT / "examples/agent-harness/run_tool.py"), "--api-url", server.url,
                                     "--token-env", "SYNTHETIC_AGENTOS_TOKEN", "--idempotency-key", "synthetic-tool-call", "--wait-seconds", "2"],
                                    env=environment, capture_output=True, timeout=5, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(RUN, result.stdout.decode())
            self.assertIn("State: succeeded", result.stdout.decode())
            self.assertIn("332833500", result.stdout.decode())
            self.assertNotIn(TOKEN.encode(), result.stdout + result.stderr)
            payload = json.loads(server.requests[0]["body"])
            self.assertEqual(payload["template"], "python")
            self.assertEqual(payload["timeout_seconds"], 30)


if __name__ == "__main__":
    unittest.main()
