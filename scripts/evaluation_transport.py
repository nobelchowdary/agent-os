"""Collection-scoped Kubernetes proxy and persistent, bounded REST channels.

The private Unix socket never opens an unauthenticated TCP listener. kubectl
owns kubeconfig authentication/TLS and refresh; each observer channel reuses its
HTTP connection through that one proxy. No workload code runs in this module.
"""

import http.client
import json
import math
import os
from pathlib import Path
import re
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.parse
import weakref


MAX_JSON = 512 * 1024
MAX_LOG = 64 * 1024
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?\Z")
_NAMESPACE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
# Deliberately finite: this is not an arbitrary Kubernetes request API.
_RESOURCES = {
    "jobs": ("batch/v1", "Job"),
    "pods": ("v1", "Pod"),
    "serviceaccounts": ("v1", "ServiceAccount"),
    "networkpolicies": ("networking.k8s.io/v1", "NetworkPolicy"),
    "runtimeclasses": ("node.k8s.io/v1", "RuntimeClass"),
    "sandboxtemplates": ("extensions.agents.x-k8s.io/v1beta1", "SandboxTemplate"),
    "sandboxwarmpools": ("extensions.agents.x-k8s.io/v1beta1", "SandboxWarmPool"),
    "sandboxclaims": ("extensions.agents.x-k8s.io/v1beta1", "SandboxClaim"),
    "sandboxes": ("agents.x-k8s.io/v1beta1", "Sandbox"),
}
_ALIASES = {"job": "jobs", "serviceaccount": "serviceaccounts", "networkpolicy": "networkpolicies"}
for _plural, (_version, _kind) in _RESOURCES.items():
    _ALIASES[_plural] = _plural
    if "/" in _version:
        _ALIASES[_plural + "." + _version.split("/")[0]] = _plural


class TransportFailure(Exception):
    """Fixed codes only; HTTP bodies and subprocess diagnostics are discarded."""


class _RequestFailure(Exception):
    def __init__(self, code, status=None):
        super().__init__(code)
        self.code, self.status = code, status


def _json_bytes(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, UnicodeError):
        raise _RequestFailure("invalid_kubernetes_request") from None


def _object(raw):
    try:
        value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeError):
        raise _RequestFailure("invalid_kubernetes_json") from None
    if not isinstance(value, dict):
        raise _RequestFailure("invalid_kubernetes_json")
    return value


class _DeadlineIO:
    """Bound total request time, including peers that drip headers/body bytes."""
    deadline = None

    def _remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("request deadline exceeded")
        self.settimeout(remaining)

    def recv_into(self, *args, **kwargs):
        self._remaining()
        return super().recv_into(*args, **kwargs)

    def sendall(self, *args, **kwargs):
        self._remaining()
        return super().sendall(*args, **kwargs)

    def send(self, *args, **kwargs):
        # SSLSocket.sendall is a Python loop over send(), unlike the plain
        # socket implementation; reset only to the remaining absolute budget.
        self._remaining()
        return super().send(*args, **kwargs)


class _DeadlineSocket(_DeadlineIO, socket.socket):
    pass


class _DeadlineSSLSocket(_DeadlineIO, ssl.SSLSocket):
    pass


class _RequestDeadline:
    def begin_request(self, timeout=None):
        duration = self._request_timeout if timeout is None else timeout
        if type(duration) not in (float, int) or not math.isfinite(duration) or duration <= 0 or duration > 60:
            raise TransportFailure("invalid_http_deadline")
        self.timeout = duration
        self._request_deadline = time.monotonic() + duration
        if self.sock is not None:
            self.sock.deadline = self._request_deadline
            self.sock.settimeout(duration)
        return self


class _PersistentHTTPConnection(_RequestDeadline, http.client.HTTPConnection):
    def __init__(self, host, port, timeout):
        super().__init__(host, port, timeout=timeout)
        self._request_timeout = timeout
        self.begin_request()

    def connect(self):
        super().connect()
        self.sock = _DeadlineSocket(fileno=self.sock.detach())
        self.sock.deadline = self._request_deadline
        self.sock._remaining()


class _PersistentHTTPSConnection(_RequestDeadline, http.client.HTTPSConnection):
    def __init__(self, host, port, timeout):
        # Preserve certificate and hostname validation. No unverified context,
        # proxy environment, redirects, client credentials or protocol upgrade.
        context = ssl.create_default_context()
        context.set_alpn_protocols(["http/1.1"])
        context.sslsocket_class = _DeadlineSSLSocket
        super().__init__(host, port, timeout=timeout, context=context)
        self._request_timeout = timeout
        self.begin_request()

    def connect(self):
        # Bound TLS handshake by what remains after TCP connection setup.
        http.client.HTTPConnection.connect(self)
        remaining = self._request_deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("request deadline exceeded")
        self.sock.settimeout(remaining)
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self._tunnel_host or self.host)
        self.sock.deadline = self._request_deadline


def persistent_http_connection(scheme, host, port=None, timeout=10):
    """Direct persistent HTTP(S) with begin_request() total network deadlines.

    Call begin_request() immediately before each request. Socket send/receive and
    TLS use the remaining budget, so a trickling peer cannot reset idle timers.
    The system resolver itself is synchronous; pilot URLs should use the literal
    loopback address of their port-forward. No proxy variables are consulted.
    """
    if scheme not in {"http", "https"}:
        raise TransportFailure("invalid_http_scheme")
    factory = _PersistentHTTPSConnection if scheme == "https" else _PersistentHTTPConnection
    return factory(host, port, timeout)


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path, timeout):
        super().__init__("localhost", timeout=timeout)
        self.path = path
        self.deadline = None

    def connect(self):
        self.sock = _DeadlineSocket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.deadline = self.deadline
        self.sock.settimeout(self.timeout)
        try:
            self.sock.connect(self.path)
        except OSError:
            self.close()
            raise


class _Proxy:
    def __init__(self):
        self.process = self.directory = self.path = None
        self.channels = weakref.WeakSet()
        self.lock = threading.RLock()
        self.closed = False


class KubernetesProxy:
    """One kubectl process per collection; fork() makes an independent channel.

    Call start() before the timed window, or use a context manager. Channels are
    thread-safe, but a dedicated fork prevents log/control I/O from delaying an
    outcome-sampling thread. Closing a fork leaves the shared proxy alive.
    """

    def __init__(self, registration, token_env=None, failure_type=TransportFailure,
                 request_timeout=10.0, startup_timeout=30.0, _proxy=None):
        self.registration, self.token_env = registration, token_env
        self.failure_type = failure_type
        self.request_timeout, self.startup_timeout = request_timeout, startup_timeout
        self._proxy = _proxy or _Proxy()
        self._owner = _proxy is None
        self._connection = None
        self._lock = threading.RLock()
        self._started = self._closed = False
        self._proxy.channels.add(self)
        namespace, context = registration.get("namespace"), registration.get("context")
        if (not isinstance(namespace, str) or not _NAMESPACE.fullmatch(namespace)
                or not isinstance(context, str) or not context or "\0" in context or len(context) > 1024
                or any(type(x) not in (float, int) or not math.isfinite(x) or x <= 0 or x > 60
                       for x in (request_timeout, startup_timeout))):
            self._fail("invalid_proxy_configuration")

    def _fail(self, code):
        raise self.failure_type(code) from None

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()

    def _resource(self, resource):
        plural = _ALIASES.get(resource) if isinstance(resource, str) else None
        if plural is None:
            raise _RequestFailure("unsupported_kubernetes_resource")
        version, _ = _RESOURCES[plural]
        prefix = "/api/v1" if version == "v1" else "/apis/" + version
        if plural != "runtimeclasses":
            prefix += "/namespaces/" + self.registration["namespace"]
        return prefix + "/" + plural

    def _path(self, resource, name=None):
        path = self._resource(resource)
        if name is not None:
            if not isinstance(name, str) or not _NAME.fullmatch(name):
                raise _RequestFailure("invalid_kubernetes_resource_name")
            path += "/" + name
        return path

    def _accept_paths(self):
        paths = [re.escape(self._resource(resource)) + "(/[^/]+)?" for resource in _RESOURCES]
        paths += [re.escape(self._resource("pods")) + "/[^/]+/log", "/version"]
        return "^(" + "|".join(paths) + ")$"

    def start(self):
        with self._proxy.lock:
            if self._closed or self._proxy.closed:
                self._fail("kubernetes_proxy_closed")
            if self._started:
                return self
            if self._proxy.process is None:
                if not self._owner:
                    self._fail("kubernetes_proxy_not_started")
                try:
                    # Short paths are required by macOS's Unix socket limit.
                    self._proxy.directory = tempfile.TemporaryDirectory(prefix="ae-kp-", dir="/tmp")
                    os.chmod(self._proxy.directory.name, 0o700)
                    self._proxy.path = str(Path(self._proxy.directory.name) / "proxy.sock")
                    environment = dict(os.environ)
                    if self.token_env:
                        environment.pop(self.token_env, None)
                    self._proxy.process = subprocess.Popen([
                        "kubectl", "--context=" + self.registration["context"], "--request-timeout=10s",
                        "proxy", "--unix-socket=" + self._proxy.path, "--api-prefix=/",
                        "--accept-hosts=^localhost$", "--accept-paths=" + self._accept_paths(), "--keepalive=30s"],
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        env=environment)
                except (OSError, _RequestFailure):
                    self.close()
                    self._fail("kubectl_proxy_unavailable")
            deadline = time.monotonic() + self.startup_timeout
            try:
                while time.monotonic() < deadline:
                    if self._proxy.process.poll() is not None:
                        self._fail("kubectl_proxy_exited")
                    try:
                        # Warm the channel and upstream authentication/TLS before timing.
                        _object(self._exchange("GET", "/version", timeout=min(
                            self.request_timeout, max(.01, deadline - time.monotonic()))))
                        self._started = True
                        return self
                    except _RequestFailure:
                        time.sleep(min(.05, max(0, deadline - time.monotonic())))
                self._fail("kubectl_proxy_start_timeout")
            except BaseException:
                self.close()
                raise

    def fork(self):
        self.start()
        child = KubernetesProxy(self.registration, self.token_env, failure_type=self.failure_type,
                                request_timeout=self.request_timeout, startup_timeout=self.startup_timeout,
                                _proxy=self._proxy)
        try:
            return child.start()
        except BaseException:
            child.close()
            raise

    def _disconnect(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def close(self):
        with self._lock:
            self._closed = True
            self._disconnect()
        if self._owner:
            with self._proxy.lock:
                self._proxy.closed = True
                for child in list(self._proxy.channels):
                    if child is not self:
                        child.close()
                process = self._proxy.process
                if process is not None:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=2)
                    self._proxy.process = None
                if self._proxy.directory is not None:
                    self._proxy.directory.cleanup()
                    self._proxy.directory = None

    def _exchange(self, method, path, body=None, limit=MAX_JSON,
                  content_type="application/json", allow_missing=False, timeout=None):
        with self._lock:
            if type(limit) is not int or limit <= 0 or limit > MAX_JSON:
                raise _RequestFailure("invalid_kubernetes_response_limit")
            if self._proxy.closed or self._closed:
                raise _RequestFailure("kubernetes_proxy_closed")
            if self._proxy.process is None or self._proxy.process.poll() is not None:
                raise _RequestFailure("kubectl_proxy_exited")
            try:
                if self._connection is None:
                    self._connection = _UnixHTTPConnection(self._proxy.path, timeout or self.request_timeout)
                self._connection.timeout = timeout or self.request_timeout
                self._connection.deadline = time.monotonic() + self._connection.timeout
                if self._connection.sock is not None:
                    self._connection.sock.deadline = self._connection.deadline
                    self._connection.sock.settimeout(timeout or self.request_timeout)
                headers = {"Accept": "application/json", "Content-Type": content_type}
                data = None if body is None else _json_bytes(body)
                if data is not None and len(data) > MAX_JSON:
                    raise _RequestFailure("kubernetes_request_too_large")
                self._connection.request(method, path, body=data, headers=headers)
                response = self._connection.getresponse()
                raw = response.read(limit + 1)
                if len(raw) > limit:
                    raise _RequestFailure("kubectl_output_too_large")
                if response.status == 404 and allow_missing:
                    return None
                if not 200 <= response.status < 300:
                    raise _RequestFailure("kubectl_request_failed", response.status)
                return raw
            except _RequestFailure:
                self._disconnect()
                raise
            except (OSError, http.client.HTTPException):
                self._disconnect()
                raise _RequestFailure("kubernetes_transport_outcome_unknown") from None

    def _request(self, *args, **kwargs):
        self.start()
        return self._exchange(*args, **kwargs)

    def _get(self, resource, name=None, selector=None):
        path = self._path(resource, name)
        if selector is not None:
            if not isinstance(selector, str) or len(selector) > 1024 or "\0" in selector:
                raise _RequestFailure("invalid_kubernetes_selector")
            path += "?" + urllib.parse.urlencode({"labelSelector": selector})
        raw = self._request("GET", path, allow_missing=name is not None)
        return None if raw is None else _object(raw)

    def get(self, resource, name=None, selector=None):
        try:
            return self._get(resource, name, selector)
        except _RequestFailure as error:
            self._fail(error.code)

    def create(self, manifest):
        try:
            if not isinstance(manifest, dict) or not isinstance(manifest.get("metadata"), dict):
                raise _RequestFailure("invalid_kubernetes_manifest")
            matches = [resource for resource, (version, kind) in _RESOURCES.items()
                       if kind == manifest.get("kind") and version == manifest.get("apiVersion")]
            if len(matches) != 1 or matches[0] not in {"jobs", "sandboxclaims", "sandboxtemplates", "sandboxwarmpools"}:
                raise _RequestFailure("unsupported_kubernetes_manifest")
            resource = matches[0]
            meta = manifest["metadata"]
            self._path(resource, meta.get("name", ""))
            if meta.get("namespace") != self.registration["namespace"]:
                raise _RequestFailure("kubernetes_manifest_namespace_mismatch")
            try:
                # A successful submit is one request, with no mandatory extra GET.
                return _object(self._request("POST", self._resource(resource), manifest))
            except _RequestFailure as error:
                if error.status is not None and error.status not in {408, 409, 429} and error.status < 500:
                    raise
                # Lost/invalid responses may follow a committed create. Never
                # resubmit or choose a new name; inspect this exact identity.
                obj = self._get(resource, meta["name"])
                if obj is None:
                    raise _RequestFailure("creation_outcome_unknown") from None
                return obj
        except _RequestFailure as error:
            self._fail(error.code)

    def logs(self, pod):
        try:
            path = self._path("pods", pod) + "/log?" + urllib.parse.urlencode({
                "container": "agent", "limitBytes": MAX_LOG + 1})
            raw = self._request("GET", path, limit=MAX_LOG + 1)
            try:
                return raw.decode("utf-8")
            except UnicodeError:
                raise _RequestFailure("invalid_utf8_output") from None
        except _RequestFailure as error:
            self._fail(error.code)

    def call(self, arguments, body=None, limit=MAX_JSON):
        """Narrow compatibility for the collector's fenced patch/delete calls."""
        try:
            if (isinstance(arguments, list) and len(arguments) == 8 and arguments[:2] == ["patch", "job"]
                    and arguments[3:5] == ["--type=json", "--patch"] and arguments[6:] == ["-o", "json"]
                    and body is None):
                try:
                    patch = json.loads(arguments[5])
                except (ValueError, TypeError):
                    raise _RequestFailure("invalid_kubernetes_patch") from None
                if not isinstance(patch, list):
                    raise _RequestFailure("invalid_kubernetes_patch")
                raw = self._request("PATCH", self._path("jobs", arguments[2]), patch,
                                    limit=limit, content_type="application/json-patch+json")
            elif (isinstance(arguments, list) and len(arguments) == 5 and arguments[:2] == ["delete", "--raw"]
                    and arguments[3:] == ["-f", "-"]):
                path = arguments[2]
                allowed = any(isinstance(path, str) and path.startswith(self._resource(resource) + "/")
                              and _NAME.fullmatch(path[len(self._resource(resource)) + 1:])
                              for resource in _RESOURCES if resource != "runtimeclasses")
                if (not allowed or not isinstance(body, dict) or set(body) != {
                        "apiVersion", "kind", "propagationPolicy", "preconditions"}
                        or body["apiVersion"] != "v1" or body["kind"] != "DeleteOptions"
                        or body["propagationPolicy"] != "Foreground" or not isinstance(body["preconditions"], dict)
                        or set(body["preconditions"]) != {"uid", "resourceVersion"}
                        or any(not isinstance(value, str) or not value for value in body["preconditions"].values())):
                    raise _RequestFailure("invalid_kubernetes_delete")
                raw = self._request("DELETE", path, body, limit=limit)
            else:
                raise _RequestFailure("unsupported_kubernetes_command")
            try:
                return raw.decode("utf-8")
            except UnicodeError:
                raise _RequestFailure("invalid_utf8_output") from None
        except _RequestFailure as error:
            self._fail(error.code)
