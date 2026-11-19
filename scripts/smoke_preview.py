#!/usr/bin/env python3
"""Exercise the compiled preview server without a cluster or agent execution."""

import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def smoke(configuration, runtime_class):
    root = Path(__file__).resolve().parents[1]
    token = secrets.token_urlsafe(32)
    env = dict(os.environ, AGENTOS_LOCAL_TOKEN=token)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(path, *, body=None, authenticated=True):
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(
            base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
        )
        try:
            with opener.open(req, timeout=2) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    with tempfile.TemporaryDirectory(prefix="agentos-preview-") as state:
        process = subprocess.Popen(
            [
                str(root / "bin/agentos"),
                "--preview",
                f"--listen=127.0.0.1:{port}",
                f"--config={root / configuration}",
                f"--db={Path(state) / 'state/runs.db'}",
            ],
            cwd=root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError("preview server exited during startup")
                try:
                    if call("/healthz", authenticated=False)[0] == 200:
                        break
                except urllib.error.URLError:
                    time.sleep(0.05)
            else:
                raise RuntimeError("preview server did not start")

            assert call("/v1/templates", authenticated=False)[0] == 401
            assert set(call("/v1/templates")[1]["templates"]) == {"python", "node"}
            status, plan = call("/v1/plan", body={"template": "python"})
            assert status == 200 and plan["execution"] is False
            job = plan["job"]
            assert job["kind"] == "Job" and job["apiVersion"] == "batch/v1"
            assert job["spec"]["suspend"] is True
            pod = job["spec"]["template"]["spec"]
            assert pod["runtimeClassName"] == runtime_class
            assert call("/v1/plan", body={"template": "python", "runtime_class": "runc"})[0] == 400
            if runtime_class == "isolated-sandbox":
                assert job["spec"]["template"]["metadata"]["annotations"]["sandbox.amazonaws.com/network-isolated"] == "true"
                assert pod["nodeSelector"]["eks.amazonaws.com/nitro-sandbox"] == "true"
                assert all(volume["emptyDir"]["medium"] == "Memory" for volume in pod["volumes"])
            assert pod["automountServiceAccountToken"] is False
            assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
            assert call("/v1/runs", body={"template": "python"})[0] == 503
            assert call("/v1/runs")[1]["runs"] == []
            assert call("/readyz", authenticated=False)[1]["execution"] == "disabled"
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        assert process.returncode == 0, "preview server did not shut down cleanly"

def main():
    for configuration, runtime_class in (("config/example.json", "gvisor"),
                                         ("config/gvisor.example.json", "gvisor"),
                                         ("config/nitro-isolated.example.json", "isolated-sandbox")):
        smoke(configuration, runtime_class)
    print("Compiled preview HTTP smoke passed for legacy, gVisor and Nitro profiles; no agent code was executed.")


if __name__ == "__main__":
    main()
