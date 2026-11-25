"""Persistent caller channel checks against an authenticated loopback fixture."""

import http.server
import importlib.util
import json
import os
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("evaluation_api_fixture", Path(__file__).parents[1] / "evaluate.py")
ev = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ev)


class Trickle:
    def __init__(self, mode):
        self.mode = mode
        self.raw = b'{"status":"ready"}'


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def get_request(self):
        request = super().get_request()
        self.accepts += 1
        return request

    def handle_error(self, *_):
        pass


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = json.loads(raw) if raw else None
        self.server.requests.append((self.command, self.path, body, dict(self.headers)))
        status, value = self.server.answer(self.command, self.path, body)
        raw = value.raw if isinstance(value, Trickle) else value if isinstance(value, bytes) else json.dumps(value).encode()
        if isinstance(value, Trickle) and value.mode == "headers":
            for byte in b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n":
                self.wfile.write(bytes([byte]))
                self.wfile.flush()
                time.sleep(.02)
            self.wfile.write(raw)
            return
        self.send_response(status)
        self.send_header("Content-Length", str(len(raw)))
        if status == 302:
            self.send_header("Location", self.server.other_url)
        self.end_headers()
        if isinstance(value, Trickle):
            for byte in raw:
                self.wfile.write(bytes([byte]))
                self.wfile.flush()
                time.sleep(.02)
        else:
            self.wfile.write(raw)

    do_POST = do_GET


class PersistentAPITests(unittest.TestCase):
    def setUp(self):
        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.accepts, self.server.requests = 0, []
        self.server.answer = lambda method, path, body: (200, {"status": "ready"} if path == "/readyz" else {"id": "fixture"})
        self.base = "http://127.0.0.1:" + str(self.server.server_address[1])
        self.server.other_url = self.base + "/must-not-follow"
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        self.thread.start()
        self.environment = patch.dict(os.environ, {"FIXTURE_BEARER": "OFFLINE_" + "x" * 40})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def api(self):
        api = ev.PersistentAgentOS(self.base, "FIXTURE_BEARER")
        self.addCleanup(api.close)
        return api

    def test_requests_reuse_channel_ignore_proxy_environment_and_preserve_idempotency_key(self):
        with patch.dict(os.environ, {"HTTP_PROXY": "http://must-not-connect.invalid", "HTTPS_PROXY": "http://must-not-connect.invalid",
                                     "ALL_PROXY": "http://must-not-connect.invalid", "NO_PROXY": ""}):
            api = self.api()
            api.call("/readyz")
            api.call("/v1/runs", {"template": "python"}, key="same-registered-key")
            api.call("/v1/runs/fixture")
        self.assertEqual(self.server.accepts, 1)
        self.assertEqual([request[0] for request in self.server.requests], ["GET", "POST", "GET"])
        self.assertEqual(self.server.requests[1][2], {"template": "python"})
        self.assertEqual(self.server.requests[1][3]["Idempotency-Key"], "same-registered-key")
        self.assertTrue(all(request[3]["Authorization"] == "Bearer OFFLINE_" + "x" * 40 for request in self.server.requests))

    def test_fork_warms_ready_channel_and_is_independent_of_control_logs(self):
        api = self.api()
        api.call("/readyz")
        child = api.fork()
        self.addCleanup(child.close)
        self.assertEqual(self.server.requests[-1][1], "/readyz")
        entered, release = threading.Event(), threading.Event()

        def answer(_, path, body):
            if path.endswith("/logs"):
                entered.set()
                release.wait(timeout=2)
            return 200, {"status": "ready"}

        self.server.answer = answer
        thread = threading.Thread(target=lambda: api.call("/v1/runs/fixture/logs"), daemon=True)
        thread.start()
        try:
            self.assertTrue(entered.wait(timeout=1))
            self.assertEqual(child.call("/readyz"), {"status": "ready"})
            self.assertTrue(thread.is_alive())
            self.assertEqual(self.server.accepts, 2)
        finally:
            release.set()
            thread.join(timeout=2)
        child.close()
        api.call("/readyz")
        self.assertEqual(self.server.accepts, 2)

    def test_http_errors_redirects_invalid_json_and_oversized_bodies_are_redacted(self):
        api = self.api()
        for status, raw, error in ((403, b"PRIVATE_FIXTURE_DETAIL", "api_http_403"),
                                   (302, b"PRIVATE_FIXTURE_DETAIL", "api_http_302"),
                                   (200, b"not-json PRIVATE_FIXTURE_DETAIL", "invalid_api_json"),
                                   (200, b"[]", "invalid_api_json"),
                                   (200, b"x" * (ev.MAX_JSON + 1), "api_response_too_large")):
            self.server.answer = lambda *_, status=status, raw=raw: (status, raw)
            with self.subTest(status=status, error=error), self.assertRaisesRegex(ev.Failure, "^" + error + "$"):
                api.call("/readyz")
        self.assertEqual(len(self.server.requests), 5)
        self.assertNotIn("/must-not-follow", [request[1] for request in self.server.requests])

    def test_transport_outcome_unknown_is_redacted_and_unsafe_paths_are_rejected(self):
        api = self.api()
        with patch.object(api.connection, "request", side_effect=OSError("PRIVATE_FIXTURE_DETAIL")):
            with self.assertRaisesRegex(ev.Failure, "^api_transport_outcome_unknown$"):
                api.call("/readyz")
        for path in ("//foreign.invalid", "/readyz\r\nHeader: private", "relative"):
            with self.subTest(path=path), self.assertRaisesRegex(ev.Failure, "^invalid_api_path$"):
                api.call(path)
        self.assertEqual(self.server.requests, [])

    def test_https_selects_tls_client_and_failed_warmup_closes_fork(self):
        api = ev.PersistentAgentOS("https://api.fixture.invalid", "FIXTURE_BEARER")
        self.addCleanup(api.close)
        self.assertIsInstance(api.connection, ev.http.client.HTTPSConnection)
        self.assertTrue(api.connection._context.check_hostname)
        self.assertEqual(api.connection._context.verify_mode.name, "CERT_REQUIRED")
        api = self.api()
        self.server.answer = lambda *_: (200, {"status": "not-ready"})
        with self.assertRaisesRegex(ev.Failure, "^api_not_execution_ready$"):
            api.fork()

    def test_total_deadline_includes_dripped_headers_and_body(self):
        for mode in ("headers", "body"):
            api = self.api()
            api.call("/readyz")
            api.connection._request_timeout = .05
            self.server.answer = lambda *_, mode=mode: (200, Trickle(mode))
            started = time.monotonic()
            with self.subTest(mode=mode), self.assertRaisesRegex(ev.Failure, "^api_transport_outcome_unknown$"):
                api.call("/v1/runs/fixture")
            self.assertLess(time.monotonic() - started, .20)
            self.server.answer = lambda *_: (200, {"status": "ready"})
            # A failed read closes its connection; the next request explicitly
            # begins a fresh budget and can reconnect without another POST.
            self.assertEqual(api.call("/readyz"), {"status": "ready"})


if __name__ == "__main__":
    unittest.main()
