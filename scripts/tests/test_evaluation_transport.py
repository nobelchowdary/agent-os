"""Private local Unix HTTP fixtures; no Kubernetes or agent code is executed."""

import copy
import http.server
import importlib.util
import json
import os
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import threading
import time
import unittest
from unittest.mock import patch
from unittest.mock import Mock


SPEC = importlib.util.spec_from_file_location("evaluation_transport", Path(__file__).parents[1] / "evaluation_transport.py")
transport = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transport)


class SlowBody:
    raw = b'{"items":[]}'


class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    block_on_close = False

    def get_request(self):
        request = super().get_request()
        self.accepts += 1
        return request

    def handle_error(self, *_):
        pass  # Deliberately suppressed broken-pipe/timeout fixture diagnostics.


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):
        self.respond()

    do_POST = do_PATCH = do_DELETE = do_GET

    def respond(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = json.loads(raw) if raw else None
        self.server.requests.append((self.command, self.path, body, dict(self.headers)))
        value = self.server.answer(self.command, self.path, body)
        if value is None:
            # Model a create committed by the server with its response lost.
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        status, value = value
        raw = value.raw if isinstance(value, SlowBody) else value if isinstance(value, bytes) else json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        if isinstance(value, SlowBody):
            for byte in raw:
                self.wfile.write(bytes([byte]))
                self.wfile.flush()
                time.sleep(.02)
        else:
            self.wfile.write(raw)


class Process:
    def __init__(self, arguments, kwargs, answer, serve=True):
        self.arguments, self.kwargs = arguments, kwargs
        self.returncode = None
        self.terminated = False
        self.path = arguments[arguments.index("proxy") + 1].split("=", 1)[1]
        self.server = None
        if serve:
            self.server = UnixServer(self.path, Handler)
            self.server.accepts, self.server.requests = 0, []
            self.server.answer = answer
            self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
            self.thread.start()

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = 0
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=1)

    def kill(self):
        self.terminate()

    def wait(self, timeout=None):
        return self.returncode


class ProxyTests(unittest.TestCase):
    def setUp(self):
        self.registration = {"namespace": "agentos-workloads", "context": "explicit-context"}
        self.processes = []
        self.answer = lambda method, path, body: (200, {"gitVersion": "fixture"} if path == "/version" else {"items": []})

        def popen(arguments, **kwargs):
            process = Process(arguments, kwargs, lambda *args: self.answer(*args))
            self.processes.append(process)
            return process

        self.popen = patch.object(transport.subprocess, "Popen", side_effect=popen).start()
        self.addCleanup(patch.stopall)

    def proxy(self, **kwargs):
        value = transport.KubernetesProxy(self.registration, **kwargs)
        self.addCleanup(value.close)
        return value

    def manifest(self):
        return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
            "name": "job-fixture", "namespace": self.registration["namespace"]}, "spec": {}}

    def test_one_private_proxy_and_persistent_channel_for_reads_submit_patch_and_log(self):
        proxy = self.proxy(token_env="FIXTURE_API_TOKEN")
        manifest = self.manifest()
        created = copy.deepcopy(manifest)
        created["metadata"].update(uid="uid-fixture", resourceVersion="7")

        def answer(method, path, body):
            if path == "/version":
                return 200, {"gitVersion": "fixture"}
            if method == "POST":
                self.assertEqual(body, manifest)
                return 201, created
            if "/log?" in path:
                return 200, b"ready\ncomplete\n"
            return 200, created

        self.answer = answer
        with patch.dict(os.environ, {"FIXTURE_API_TOKEN": "SECRET_MUST_NOT_REACH_KUBECTL",
                                     "HTTP_PROXY": "http://proxy.invalid"}):
            proxy.start()
            proxy.get("jobs", "job-fixture")
            proxy.get("pods", selector="test=value/with spaces")
            self.assertEqual(proxy.create(manifest), created)
            patch_value = [{"op": "test", "path": "/metadata/uid", "value": "uid-fixture"}]
            proxy.call(["patch", "job", "job-fixture", "--type=json", "--patch", json.dumps(patch_value), "-o", "json"])
            self.assertEqual(proxy.logs("job-fixture-pod"), "ready\ncomplete\n")
        self.assertEqual(self.popen.call_count, 1)
        process = self.processes[0]
        self.assertEqual(process.server.accepts, 1)
        self.assertNotIn("FIXTURE_API_TOKEN", process.kwargs["env"])
        self.assertEqual(os.stat(Path(process.path).parent).st_mode & 0o777, 0o700)
        self.assertIn("--context=explicit-context", process.arguments)
        self.assertNotIn("--disable-filter", process.arguments)
        self.assertTrue(any(item.startswith("--unix-socket=") for item in process.arguments))
        self.assertFalse(any(item.startswith("--port=") for item in process.arguments))
        self.assertEqual(process.kwargs["stdout"], subprocess.DEVNULL)
        self.assertEqual(process.kwargs["stderr"], subprocess.DEVNULL)
        requests = process.server.requests
        self.assertEqual([method for method, *_ in requests], ["GET", "GET", "GET", "POST", "PATCH", "GET"])
        self.assertIn("labelSelector=test%3Dvalue%2Fwith+spaces", requests[2][1])
        self.assertEqual(requests[-2][3]["Content-Type"], "application/json-patch+json")
        self.assertTrue(all(headers["Host"] == "localhost" and "Authorization" not in headers
                            for _, _, _, headers in requests))

    def test_fork_has_independent_persistent_channel_and_does_not_own_proxy(self):
        proxy = self.proxy().start()
        child = proxy.fork()
        child.get("pods")
        child.get("pods")
        process = self.processes[0]
        self.assertEqual(process.server.accepts, 2)
        self.assertEqual(self.popen.call_count, 1)
        child.close()
        self.assertFalse(process.terminated)
        proxy.get("pods")
        self.assertEqual(process.server.accepts, 2)
        path = process.path
        proxy.close()
        self.assertTrue(process.terminated)
        self.assertFalse(Path(path).parent.exists())
        with self.assertRaisesRegex(transport.TransportFailure, "kubernetes_proxy_closed"):
            child.get("pods")

    def test_owner_closes_live_fork_and_cleans_socket_on_context_exception(self):
        proxy = self.proxy()
        with self.assertRaisesRegex(RuntimeError, "fixture"):
            with proxy:
                child = proxy.fork()
                raise RuntimeError("fixture")
        self.assertTrue(self.processes[0].terminated)
        self.assertFalse(Path(self.processes[0].path).parent.exists())
        with self.assertRaisesRegex(transport.TransportFailure, "closed"):
            child.get("pods")

    def test_sampling_fork_is_not_blocked_by_a_control_log_read(self):
        proxy = self.proxy().start()
        child = proxy.fork()
        self.addCleanup(child.close)
        reading, release = threading.Event(), threading.Event()

        def answer(_, path, body):
            if "/log?" in path:
                reading.set()
                release.wait(timeout=2)
                return 200, b"fixture\n"
            return 200, {"items": []}

        self.answer = answer
        logs = []
        thread = threading.Thread(target=lambda: logs.append(proxy.logs("pod-fixture")), daemon=True)
        thread.start()
        try:
            self.assertTrue(reading.wait(timeout=1))
            self.assertEqual(child.get("pods"), {"items": []})
            self.assertTrue(thread.is_alive(), "control read should still wait while sampling finishes")
            self.assertEqual(self.popen.call_count, 1)
            self.assertEqual(self.processes[0].server.accepts, 2)
        finally:
            release.set()
            thread.join(timeout=2)
        self.assertEqual(logs, ["fixture\n"])

    def test_ambiguous_create_inspects_exact_name_without_resubmitting(self):
        manifest = self.manifest()
        created = copy.deepcopy(manifest)
        created["metadata"]["uid"] = "committed-before-response-loss"

        def answer(method, path, _):
            if method == "POST":
                return None
            return (200, {"gitVersion": "fixture"}) if path == "/version" else (200, created)

        self.answer = answer
        proxy = self.proxy().start()
        self.assertEqual(proxy.create(manifest), created)
        requests = self.processes[0].server.requests
        self.assertEqual([method for method, *_ in requests], ["GET", "POST", "GET"])
        self.assertEqual(requests[-1][1], "/apis/batch/v1/namespaces/agentos-workloads/jobs/job-fixture")

    def test_unresolved_create_and_definite_denial_do_not_retry_submission(self):
        proxy = self.proxy().start()
        self.answer = lambda method, *_: (500 if method == "POST" else 404, {"private": "DO_NOT_PRINT"})
        with self.assertRaisesRegex(transport.TransportFailure, "^creation_outcome_unknown$"):
            proxy.create(self.manifest())
        self.assertEqual([method for method, *_ in self.processes[0].server.requests].count("POST"), 1)
        self.answer = lambda *_: (403, {"private": "DO_NOT_PRINT"})
        before = len(self.processes[0].server.requests)
        with self.assertRaisesRegex(transport.TransportFailure, "^kubectl_request_failed$"):
            proxy.create(self.manifest())
        self.assertEqual(len(self.processes[0].server.requests) - before, 1)

    def test_get_not_found_is_none_but_redirect_and_invalid_json_are_fixed_errors(self):
        proxy = self.proxy().start()
        for status, raw, code in ((404, b"PRIVATE", None), (302, b"PRIVATE", "kubectl_request_failed"),
                                  (200, b"PRIVATE_INVALID_JSON", "invalid_kubernetes_json"),
                                  (200, b"[]", "invalid_kubernetes_json")):
            self.answer = lambda *_, status=status, raw=raw: (status, raw)
            with self.subTest(status=status, raw=raw):
                if code:
                    with self.assertRaisesRegex(transport.TransportFailure, "^" + code + "$"):
                        proxy.get("jobs", "job-fixture")
                else:
                    self.assertIsNone(proxy.get("jobs", "job-fixture"))

    def test_output_size_and_transport_timeout_are_bounded(self):
        proxy = self.proxy(request_timeout=.05).start()
        self.answer = lambda *_: (200, b"x" * (transport.MAX_LOG + 2))
        with self.assertRaisesRegex(transport.TransportFailure, "^kubectl_output_too_large$"):
            proxy.logs("job-fixture-pod")
        self.answer = lambda *_: (time.sleep(.2) or 200, {})
        with self.assertRaisesRegex(transport.TransportFailure, "^kubernetes_transport_outcome_unknown$"):
            proxy.get("pods")

    def test_total_request_deadline_rejects_a_peer_dripping_body_bytes(self):
        proxy = self.proxy(request_timeout=.05).start()
        self.answer = lambda *_: (200, SlowBody())
        started = time.monotonic()
        with self.assertRaisesRegex(transport.TransportFailure, "^kubernetes_transport_outcome_unknown$"):
            proxy.get("pods")
        self.assertLess(time.monotonic() - started, .18)

    def test_delete_preserves_uid_resource_version_and_foreground_policy(self):
        proxy = self.proxy().start()
        path = "/apis/batch/v1/namespaces/agentos-workloads/jobs/job-fixture"
        options = {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
                   "preconditions": {"uid": "uid-fixture", "resourceVersion": "7"}}
        proxy.call(["delete", "--raw", path, "-f", "-"], options)
        request = self.processes[0].server.requests[-1]
        self.assertEqual(request[:3], ("DELETE", path, options))
        for candidate_path, candidate in ((path.replace("agentos-workloads", "other"), options),
                                           (path + "/../pods", options), (path, dict(options, gracePeriodSeconds=0)),
                                           (path, dict(options, preconditions={"uid": "uid-fixture"}))):
            with self.subTest(path=candidate_path, body=candidate), self.assertRaisesRegex(
                    transport.TransportFailure, "^invalid_kubernetes_delete$"):
                proxy.call(["delete", "--raw", candidate_path, "-f", "-"], candidate)

    def test_invalid_paths_manifests_and_commands_are_rejected_before_proxy_start(self):
        proxy = self.proxy()
        actions = [lambda: proxy.get("secrets"), lambda: proxy.get("pods", "../escape"),
                   lambda: proxy.get("pods", selector=[]), lambda: proxy.call(["exec", "pod", "env"]),
                   lambda: proxy.create(dict(self.manifest(), metadata={"name": "job", "namespace": "other"})),
                   lambda: proxy.create({"apiVersion": "v1", "kind": "Pod", "metadata": {}})]
        for action in actions:
            with self.assertRaises(transport.TransportFailure):
                action()
        self.popen.assert_not_called()

    def test_start_failure_discards_diagnostics_and_removes_private_directory(self):
        paths = []

        def unavailable(arguments, **_):
            paths.append(arguments[arguments.index("proxy") + 1].split("=", 1)[1])
            raise OSError("PRIVATE_CREDENTIAL_DETAIL")

        self.popen.side_effect = unavailable
        with self.assertRaisesRegex(transport.TransportFailure, "^kubectl_proxy_unavailable$"):
            self.proxy().start()
        self.assertTrue(paths)
        self.assertFalse(Path(paths[0]).parent.exists())

    def test_start_timeout_and_early_exit_stop_owned_process(self):
        for exit_code, expected in ((None, "kubectl_proxy_start_timeout"), (3, "kubectl_proxy_exited")):
            processes = []

            def unavailable(arguments, **kwargs):
                process = Process(arguments, kwargs, self.answer, serve=False)
                process.returncode = exit_code
                processes.append(process)
                return process

            self.popen.side_effect = unavailable
            with self.subTest(exit_code=exit_code), self.assertRaisesRegex(transport.TransportFailure, "^" + expected + "$"):
                self.proxy(request_timeout=.01, startup_timeout=.05).start()
            self.assertFalse(Path(processes[0].path).parent.exists())
            if exit_code is None:
                self.assertTrue(processes[0].terminated)

    def test_start_interrupt_closes_process_and_custom_failure_type_is_preserved(self):
        proxy = self.proxy()
        with patch.object(proxy, "_exchange", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            proxy.start()
        self.assertTrue(self.processes[0].terminated)
        self.assertFalse(Path(self.processes[0].path).parent.exists())

        class CallerFailure(Exception):
            pass

        proxy = self.proxy(failure_type=CallerFailure)
        with self.assertRaisesRegex(CallerFailure, "unsupported_kubernetes_resource"):
            proxy.get("secrets")
        child = proxy.start().fork()
        self.addCleanup(child.close)
        with self.assertRaises(CallerFailure):
            child.get("secrets")

    def test_dead_proxy_is_not_restarted_inside_a_timed_request(self):
        proxy = self.proxy().start()
        self.processes[0].terminate()
        with self.assertRaisesRegex(transport.TransportFailure, "^kubectl_proxy_exited$"):
            proxy.get("pods")
        self.assertEqual(self.popen.call_count, 1)


class DeadlineConnectionTests(unittest.TestCase):
    def test_tls_keeps_verification_and_limits_handshake_to_remaining_budget(self):
        with patch.object(transport.time, "monotonic", return_value=10):
            connection = transport.persistent_http_connection("https", "verified.fixture", timeout=10)
        self.addCleanup(connection.close)
        self.assertTrue(connection._context.check_hostname)
        self.assertEqual(connection._context.verify_mode, ssl.CERT_REQUIRED)
        raw, wrapped = Mock(), Mock()

        def tcp_connect(current):
            current.sock = raw

        with patch.object(transport.http.client.HTTPConnection, "connect", tcp_connect), \
             patch.object(connection._context, "wrap_socket", return_value=wrapped) as wrap, \
             patch.object(transport.time, "monotonic", return_value=14):
            connection.connect()
        raw.settimeout.assert_called_once_with(6)
        wrap.assert_called_once_with(raw, server_hostname="verified.fixture")
        self.assertEqual(wrapped.deadline, 20)

    def test_tls_receive_and_send_reject_expired_absolute_budget_before_io(self):
        connection = transport.persistent_http_connection("https", "verified.fixture")
        self.addCleanup(connection.close)
        raw, peer = socket.socketpair()
        self.addCleanup(peer.close)
        wrapped = connection._context.wrap_socket(raw, server_hostname="verified.fixture", do_handshake_on_connect=False)
        self.addCleanup(wrapped.close)
        wrapped.deadline = time.monotonic() - 1
        with self.assertRaises(TimeoutError):
            wrapped.recv_into(bytearray(1))
        with self.assertRaises(TimeoutError):
            wrapped.sendall(b"fixture")


if __name__ == "__main__":
    unittest.main()
