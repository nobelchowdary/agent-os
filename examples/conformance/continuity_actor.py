#!/usr/bin/env python3
"""Dedicated Substrate smoke fixture; never mount a customer workspace here.

RAM nonce is deliberately never written to disk. File nonce/counter live only
under STATE_DIR. Restoring files alone cannot preserve the RAM nonce/counter.
This fixture has no outbound calls, model requests, credentials or tool access.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import secrets
import threading


class State:
    def __init__(self, directory, probe_id):
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = self.directory / "continuity.json"
        self.probe_id = probe_id
        self.memory_nonce = secrets.token_hex(32)
        self.memory_count = 0
        self.lock = threading.Lock()
        if self.path.exists():
            value = json.loads(self.path.read_text())
            self.file_nonce, self.file_count = value["file_nonce"], value["file_count"]
        else:
            self.file_nonce, self.file_count = secrets.token_hex(32), 0
            self.persist()

    def persist(self):
        temporary = self.directory / "continuity.tmp"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump({"file_nonce": self.file_nonce, "file_count": self.file_count}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def read(self, increment=False):
        with self.lock:
            if increment:
                self.memory_count += 1
                self.file_count += 1
                self.persist()
            return {"probe_id": self.probe_id, "memory_nonce": self.memory_nonce,
                    "memory_count": self.memory_count, "file_nonce": self.file_nonce,
                    "file_count": self.file_count}


def handler(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def respond(self, value, status=200):
            encoded = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            self.respond(state.read()) if self.path == "/state" else self.respond({}, 404)

        def do_POST(self):
            if self.path != "/increment":
                self.respond({}, 404)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > 256:
                    raise ValueError()
                request = json.loads(self.rfile.read(length))
                if request != {"probe_id": state.probe_id}:
                    raise ValueError()
            except (ValueError, json.JSONDecodeError):
                self.respond({}, 400)
                return
            self.respond(state.read(increment=True))

    return Handler


def main():
    probe_id = os.environ.get("AGENTOS_PROBE_ID", "")
    if len(probe_id) < 16 or len(probe_id) > 128:
        raise SystemExit("Set a dedicated AGENTOS_PROBE_ID of 16..128 characters.")
    state = State(os.environ.get("STATE_DIR", "/tmp/agentos-continuity"), probe_id)
    server = HTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), handler(state))
    server.timeout = 5
    server.serve_forever()


if __name__ == "__main__":
    main()
