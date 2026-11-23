"""Stdlib client for the experimental Agent OS execution API (Python 3.10+).

Each request opens a direct connection, verifies HTTPS certificates, and refuses
redirects. Event cursors belong to one iterator/run and are never cached across
runs or credentials. Submitted code is executed by the configured server only.
"""

from datetime import datetime
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit


STATES = frozenset({"queued", "starting", "activating", "running", "cancelling",
                    "succeeded", "failed", "cancelled"})
TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
REASON_CODES = frozenset({"completed", "task_error", "deadline_exceeded", "cancelled",
                          "oom_killed", "evicted", "image_pull_failed", "policy_denied",
                          "budget_exceeded", "infra_failure", "unknown"})
FAILURE_REASON_CODES = REASON_CODES - {"completed", "cancelled"}
MAX_CURSOR = 2**63 - 1
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_EVENT_BYTES = 128 * 1024
MAX_LINE_BYTES = 64 * 1024
RETRY_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class AgentOSError(Exception):
    """Sanitized failure; response bodies, URLs and credentials are excluded."""

    def __init__(self, code, *, status_code=None, retryable=False):
        self.code = code
        self.status_code = status_code
        self.retryable = retryable
        message = "Agent OS: " + code
        if status_code is not None:
            message += " (HTTP " + str(status_code) + ")"
        super().__init__(message)


class AgentOSProtocolError(AgentOSError):
    pass


class AgentOSTimeout(AgentOSError, TimeoutError):
    pass


def _seconds(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or not 0 < value <= 86400:
        raise ValueError("timeout must be a finite positive number at most 86400 seconds")
    return float(value)


def _run_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{32}", value):
        raise ValueError("run_id must be 32 lowercase hexadecimal characters")
    return value


def _cursor(value):
    if type(value) is int and value == 0:
        return None, 0
    if isinstance(value, str):
        match = re.fullmatch(r"([a-f0-9]{32}):(0|[1-9][0-9]{0,18})", value)
        if match and int(match[2]) <= MAX_CURSOR:
            return match[1], int(match[2])
    raise ValueError("after must be 0 for a fresh stream or an epoch:sequence event cursor")


def _response_epoch(response, expected=None):
    epoch = response.getheader("AgentOS-Store-Epoch", "")
    if not isinstance(epoch, str) or not re.fullmatch(r"[a-f0-9]{32}", epoch):
        raise AgentOSProtocolError("invalid_store_epoch")
    if expected is not None and epoch != expected:
        raise AgentOSProtocolError("store_epoch_mismatch")
    return epoch


def _reason_code(value):
    """Validate the optional machine code, never infer one from diagnostic text."""
    if "reason_code" not in value:
        return  # Legacy records/events omit it; omission is not an unknown-code assertion.
    code, state = value["reason_code"], value.get("state")
    if not isinstance(code, str) or code not in REASON_CODES:
        raise AgentOSProtocolError("invalid_reason_code")
    consistent = ((state == "succeeded" and code == "completed")
                  or (state == "cancelled" and code == "cancelled")
                  or (state == "failed" and code in FAILURE_REASON_CODES)
                  or (state == "cancelling" and (code == "cancelled" or code in FAILURE_REASON_CODES)))
    if not consistent:
        raise AgentOSProtocolError("reason_code_state_mismatch")


def _json(encoded):
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError()
            value[key] = item
        return value

    try:
        value = json.loads(encoded, object_pairs_hook=unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeError, RecursionError):
        raise AgentOSProtocolError("invalid_json") from None
    if not isinstance(value, dict):
        raise AgentOSProtocolError("invalid_json_object")
    return value


def _execution_result(value):
    if "result" not in value:
        return
    result = value["result"]
    required = {"output_head", "output_tail", "output_truncated", "output_complete", "status", "captured_at", "expires_at"}
    if (not isinstance(result, dict) or not required <= set(result) or not set(result) <= required | {"exit_code"}
            or not isinstance(result.get("status"), str) or result["status"] not in {"captured", "unavailable", "expired"}
            or not isinstance(result.get("output_head"), str) or not isinstance(result.get("output_tail"), str)
            or type(result.get("output_truncated")) is not bool or type(result.get("output_complete")) is not bool
            or ("exit_code" in result and (type(result["exit_code"]) is not int or not -(2**31) <= result["exit_code"] < 2**31))):
        raise AgentOSProtocolError("invalid_execution_result")
    try:
        if len(result["output_head"].encode("utf-8")) + len(result["output_tail"].encode("utf-8")) > 65536:
            raise ValueError()
        captured = datetime.fromisoformat(result["captured_at"].replace("Z", "+00:00"))
        expires = datetime.fromisoformat(result["expires_at"].replace("Z", "+00:00"))
        if captured.tzinfo is None or expires.tzinfo is None or expires <= captured:
            raise ValueError()
    except (ValueError, TypeError, AttributeError, UnicodeError):
        raise AgentOSProtocolError("invalid_execution_result") from None
    if result["status"] != "captured" and (result["output_head"] or result["output_tail"]):
        raise AgentOSProtocolError("invalid_execution_result")
    if result["status"] == "unavailable" and (result["output_complete"] or result["output_truncated"]):
        raise AgentOSProtocolError("invalid_execution_result")


class _Exchange:
    """An established connection with a deadline for drip-fed partial lines.

    OS DNS resolution precedes this socket watchdog and is not interruptible by
    it. Elapsed resolution time still consumes the caller's timeout budget.
    """

    def __init__(self, connection, deadline):
        self.connection = connection
        self.deadline = deadline
        self.response = None
        self.timer = None
        self.expired = threading.Event()

    def arm(self):
        transport = self.connection.sock

        def expire():
            self.expired.set()
            try:
                transport.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        self.timer = threading.Timer(max(0, self.deadline - time.monotonic()), expire)
        self.timer.daemon = True
        self.timer.start()

    def check(self):
        if self.expired.is_set() or time.monotonic() >= self.deadline:
            raise AgentOSTimeout("deadline_exceeded")

    def read(self, limit):
        self.check()
        try:
            data = self.response.read1(limit)
        except (OSError, http.client.HTTPException, ValueError):
            self.check()
            raise AgentOSError("connection_interrupted", retryable=True) from None
        self.check()
        return data

    def close(self):
        if self.timer is not None:
            self.timer.cancel()
        try:
            if self.response is not None:
                self.response.close()
        except (OSError, http.client.HTTPException, ValueError):
            pass
        finally:
            try:
                self.connection.close()
            except OSError:
                pass


class AgentOSClient:
    def __init__(self, base_url, *, token=None, token_env=None, request_timeout=15,
                 stream_timeout=20, reconnect_delay=0.25, max_reconnects=20):
        if not isinstance(base_url, str) or any(ord(c) <= 32 for c in base_url):
            raise ValueError("base_url must be an HTTPS or loopback HTTP origin")
        try:
            parsed = urlsplit(base_url)
            host, port = parsed.hostname, parsed.port
        except ValueError:
            raise ValueError("invalid API origin") from None
        if parsed.scheme not in {"https", "http"} or not host or parsed.username is not None or parsed.password is not None or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("base_url must be an HTTPS or loopback HTTP origin without credentials or a path")
        if parsed.scheme == "http":
            # Pin localhost to IPv4 loopback instead of trusting DNS/hosts aliases.
            host = "127.0.0.1" if host == "localhost" else host
            try:
                loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                loopback = False
            if not loopback:
                raise ValueError("plain HTTP is permitted only on loopback")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("invalid API port")
        if token is not None and token_env is not None:
            raise ValueError("provide token or token_env, not both")
        if token is None:
            token = os.environ.get(token_env or "AGENTOS_TOKEN")
        if not isinstance(token, str) or not 1 <= len(token) <= 1017 or any(not 33 <= ord(c) <= 126 for c in token):
            raise ValueError("an ASCII bearer token must be supplied in the constructor or environment")
        self._host, self._port, self._https = host, port, parsed.scheme == "https"
        self._token = token  # Captured once; changing an environment variable cannot change a live stream's tenant.
        self.request_timeout = _seconds(request_timeout)
        self.stream_timeout = _seconds(stream_timeout)
        if isinstance(reconnect_delay, bool) or not isinstance(reconnect_delay, (float, int)) or not math.isfinite(reconnect_delay) or not 0 <= reconnect_delay <= 5:
            raise ValueError("reconnect_delay must be between 0 and 5 seconds")
        if type(max_reconnects) is not int or not 0 <= max_reconnects <= 1000:
            raise ValueError("max_reconnects must be an integer between 0 and 1000")
        self.reconnect_delay, self.max_reconnects = float(reconnect_delay), max_reconnects

    def __repr__(self):
        return "AgentOSClient(origin=<configured>, token=<redacted>)"

    def _open(self, method, path, *, deadline, stream=False, body=None, extra_headers=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AgentOSTimeout("deadline_exceeded")
        timeout = min(self.stream_timeout if stream else self.request_timeout, remaining)
        connection = (http.client.HTTPSConnection(self._host, self._port, timeout=timeout, context=ssl.create_default_context())
                      if self._https else http.client.HTTPConnection(self._host, self._port, timeout=timeout))
        exchange = _Exchange(connection, deadline)
        headers = {"Authorization": "Bearer " + self._token,
                   "Connection": "close",
                   "Accept": "text/event-stream" if stream else "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        headers.update(extra_headers or {})
        try:
            connection.connect()
            exchange.arm()
            # Establishment (including OS DNS) may have consumed the budget.
            # Check synchronously before a POST can send side effects; the
            # watchdog thread alone is not a reliable scheduling barrier.
            exchange.check()
            connection.request(method, path, body=body, headers=headers)
            exchange.response = connection.getresponse()
            exchange.check()
            return exchange
        except (OSError, http.client.HTTPException, ValueError):
            timed_out = exchange.expired.is_set() or time.monotonic() >= deadline
            exchange.close()
            if timed_out:
                raise AgentOSTimeout("deadline_exceeded") from None
            raise AgentOSError("connection_failed", retryable=True) from None
        except BaseException:
            exchange.close()
            raise

    @staticmethod
    def _status(exchange, allowed):
        status = exchange.response.status
        if status not in allowed:
            raise AgentOSError("redirect_refused" if 300 <= status < 400 else "request_failed",
                               status_code=status, retryable=status in RETRY_STATUSES)

    def _request(self, method, path, *, deadline=None, body=None, headers=None, allowed=(200,), expected_epoch=None):
        deadline = time.monotonic() + self.request_timeout if deadline is None else deadline
        exchange = self._open(method, path, deadline=deadline, body=body, extra_headers=headers)
        try:
            self._status(exchange, allowed)
            if expected_epoch is not None:
                _response_epoch(exchange.response, expected_epoch)
            if exchange.response.getheader("Content-Type", "").split(";")[0].strip().lower() != "application/json":
                raise AgentOSProtocolError("expected_json_content_type")
            encoded = bytearray()
            while True:
                block = exchange.read(min(16384, MAX_RESPONSE_BYTES + 1 - len(encoded)))
                if not block:
                    break
                encoded.extend(block)
                if len(encoded) > MAX_RESPONSE_BYTES:
                    raise AgentOSProtocolError("response_too_large")
            return _json(encoded)
        finally:
            exchange.close()

    @staticmethod
    def _record(value, run_id=None):
        if (not isinstance(value.get("id"), str) or not re.fullmatch(r"[a-f0-9]{32}", value["id"])
                or not isinstance(value.get("state"), str) or value["state"] not in STATES
                or (run_id is not None and value["id"] != run_id)):
            raise AgentOSProtocolError("invalid_or_mismatched_run_record")
        if "events" in value and not isinstance(value["events"], list):
            raise AgentOSProtocolError("invalid_run_events")
        _reason_code(value)
        _execution_result(value)
        for event in value.get("events", []):
            if isinstance(event, dict):
                _reason_code(event)
        return value

    def create_run(self, template, command=None, timeout_seconds=None, *, idempotency_key):
        """One POST attempt. On ambiguity, retry identical input with the SAME key."""
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 128 or any(not 33 <= ord(c) <= 126 for c in idempotency_key):
            raise ValueError("idempotency_key is required and must contain 1..128 visible ASCII characters")
        if not isinstance(template, str) or not template or len(template) > 128:
            raise ValueError("template must name an administrator-owned template")
        payload = {"template": template}
        if command is not None:
            if not isinstance(command, (list, tuple)) or not command or any(not isinstance(item, str) for item in command):
                raise ValueError("command must be a nonempty list of strings")
            payload["command"] = list(command)
        if timeout_seconds is not None:
            if type(timeout_seconds) is not int or timeout_seconds <= 0:
                raise ValueError("timeout_seconds must be a positive integer")
            payload["timeout_seconds"] = timeout_seconds
        try:
            encoded = json.dumps(payload, allow_nan=False).encode()
        except (ValueError, UnicodeError):
            raise ValueError("request cannot be encoded") from None
        if len(encoded) > 32 * 1024:
            raise ValueError("run request exceeds 32 KiB")
        return self._record(self._request("POST", "/v1/runs", body=encoded,
                                         headers={"Idempotency-Key": idempotency_key}, allowed=(200, 202)))

    def get_run(self, run_id):
        return self._get_run(_run_id(run_id))

    def _get_run(self, run_id, deadline=None, expected_epoch=None):
        return self._record(self._request("GET", "/v1/runs/" + run_id, deadline=deadline,
                                         expected_epoch=expected_epoch), run_id)

    def cancel(self, run_id):
        run_id = _run_id(run_id)
        return self._record(self._request("POST", "/v1/runs/" + run_id + "/cancel", allowed=(200, 202)), run_id)

    def logs(self, run_id):
        """Return the server's bounded {run_id, logs, limit_bytes} object."""
        run_id = _run_id(run_id)
        value = self._request("GET", "/v1/runs/" + run_id + "/logs")
        if (value.get("run_id") != run_id or not isinstance(value.get("logs"), str)
                or type(value.get("limit_bytes")) is not int or not 1 <= value["limit_bytes"] <= 65536):
            raise AgentOSProtocolError("invalid_or_mismatched_logs")
        try:
            size = len(value["logs"].encode("utf-8"))
        except UnicodeError:
            raise AgentOSProtocolError("invalid_logs_encoding") from None
        if size > value["limit_bytes"]:
            raise AgentOSProtocolError("logs_exceed_server_limit")
        return value

    @staticmethod
    def _frames(exchange):
        pending = bytearray()
        frame, size = {}, 0
        while True:
            block = exchange.read(8192)
            if not block:
                return  # A truncated frame is replayed from the last fully validated cursor.
            pending.extend(block)
            while b"\n" in pending:
                raw, _, rest = pending.partition(b"\n")
                pending = bytearray(rest)
                if len(raw) > MAX_LINE_BYTES:
                    raise AgentOSProtocolError("event_line_too_large")
                try:
                    line = raw.removesuffix(b"\r").decode("utf-8")
                except UnicodeError:
                    raise AgentOSProtocolError("invalid_event_encoding") from None
                if "\x00" in line or "\r" in line:
                    raise AgentOSProtocolError("invalid_event_line")
                if line == "":
                    if frame:
                        if "event" not in frame or "data" not in frame:
                            raise AgentOSProtocolError("incomplete_event_frame")
                        yield frame
                    frame, size = {}, 0
                elif not line.startswith(":"):
                    field, separator, value = line.partition(":")
                    if not separator or field not in {"event", "data", "id"}:
                        raise AgentOSProtocolError("unsupported_event_field")
                    value = value[1:] if value.startswith(" ") else value
                    size += len(raw)
                    if size > MAX_EVENT_BYTES or (field in frame and field != "data"):
                        raise AgentOSProtocolError("invalid_or_oversized_event_frame")
                    frame[field] = frame[field] + "\n" + value if field in frame else value
            if len(pending) > MAX_LINE_BYTES:
                raise AgentOSProtocolError("event_line_too_large")

    def iter_events(self, run_id, *, after=0, timeout=300):
        """Yield format-3 events; reconnect from the last validated epoch:ID.

        Consumes terminal `end` internally. Close the generator when stopping
        early. The timeout includes reconnects and time spent by the consumer;
        OS DNS resolution cannot be interrupted by the socket deadline. Persist
        store_epoch + ':' + str(sequence) to resume; a positive number alone
        cannot safely identify history across a restore. Never downgrades.
        """
        run_id = _run_id(run_id)
        epoch, cursor = _cursor(after)
        deadline = time.monotonic() + _seconds(timeout)
        reconnects, last_state = 0, None
        while True:
            exchange = None
            try:
                after_value = str(cursor) if epoch is None else epoch + ":" + str(cursor)
                exchange = self._open("GET", "/v1/runs/" + run_id + "/events?after=" + after_value, deadline=deadline, stream=True,
                                      extra_headers={"AgentOS-Events-Version": "3"})
                self._status(exchange, (200, 204))
                if exchange.response.getheader("AgentOS-Events-Version") != "3":
                    raise AgentOSProtocolError("event_version_not_negotiated")
                # Bind even an empty/rotated stream to its validated header.
                # Reconnecting after zero events must still detect a restore.
                epoch = _response_epoch(exchange.response, epoch)
                if exchange.response.status == 204:
                    record = self._get_run(run_id, deadline, expected_epoch=epoch)
                    if record["state"] not in TERMINAL_STATES or ("events" in record and len(record["events"]) != cursor):
                        raise AgentOSProtocolError("invalid_terminal_catchup")
                    exchange.check()
                    return epoch
                if exchange.response.getheader("Content-Type", "").split(";")[0].strip().lower() != "text/event-stream":
                    raise AgentOSProtocolError("expected_event_stream")
                for frame in self._frames(exchange):
                    # Multiple frames can already be buffered while the caller
                    # handles a yielded event. Socket reads are not a deadline
                    # boundary for those later frames.
                    exchange.check()
                    value = _json(frame["data"])
                    if value.get("run_id") != run_id:
                        raise AgentOSProtocolError("event_run_mismatch")
                    if frame["event"] == "end":
                        if (set(value) != {"run_id", "state", "last_event_id", "store_epoch"} or "id" in frame
                                or not isinstance(value.get("state"), str) or value["state"] not in TERMINAL_STATES
                                or value["store_epoch"] != epoch
                                or value.get("last_event_id") != epoch + ":" + str(cursor)
                                or (last_state is not None and value["state"] != last_state)):
                            raise AgentOSProtocolError("invalid_terminal_event")
                        exchange.check()
                        return epoch
                    if (frame["event"] != "run"
                            or not {"sequence", "run_id", "at", "state", "store_epoch"} <= set(value)
                            or not set(value) <= {"sequence", "run_id", "at", "state", "store_epoch", "reason", "reason_code"}
                            or type(value.get("sequence")) is not int or value["sequence"] != cursor + 1
                            or frame.get("id") != epoch + ":" + str(value["sequence"]) or value["sequence"] > MAX_CURSOR
                            or value["store_epoch"] != epoch
                            or not isinstance(value.get("state"), str) or value["state"] not in STATES
                            or not isinstance(value.get("at"), str)
                            or ("reason" in value and not isinstance(value["reason"], str))):
                        raise AgentOSProtocolError("invalid_run_event")
                    _reason_code(value)
                    try:
                        timestamp = datetime.fromisoformat(value["at"].replace("Z", "+00:00"))
                        if timestamp.tzinfo is None:
                            raise ValueError()
                    except ValueError:
                        raise AgentOSProtocolError("invalid_event_timestamp") from None
                    if last_state in TERMINAL_STATES:
                        raise AgentOSProtocolError("event_after_terminal_state")
                    exchange.check()
                    cursor, last_state = value["sequence"], value["state"]
                    yield value
            except AgentOSError as error:
                if not error.retryable:
                    raise
            finally:
                if exchange is not None:
                    exchange.close()
            if time.monotonic() >= deadline:
                raise AgentOSTimeout("deadline_exceeded")
            if reconnects >= self.max_reconnects:
                raise AgentOSError("event_reconnect_limit")
            reconnects += 1
            time.sleep(min(self.reconnect_delay * min(reconnects, 4), max(0, deadline - time.monotonic())))

    def wait(self, run_id, *, timeout=300, after=0):
        """Wait through SSE, then return an authenticated terminal run record."""
        run_id = _run_id(run_id)
        deadline = time.monotonic() + _seconds(timeout)
        events = self.iter_events(run_id, after=after, timeout=max(0.000001, deadline - time.monotonic()))
        try:
            while True:
                next(events)
        except StopIteration as completed:
            epoch = completed.value
        finally:
            events.close()
        record = self._get_run(run_id, deadline, expected_epoch=epoch)
        if record["state"] not in TERMINAL_STATES:
            raise AgentOSProtocolError("stream_ended_before_terminal_run")
        return record
