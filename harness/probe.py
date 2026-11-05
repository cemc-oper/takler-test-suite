"""Bounded JSON-lines access to the two public QueryDocument clients."""

import json
import os
import select
import subprocess
import sys
from pathlib import Path

from .runtime import EnvironmentBlocked, require


class Probe:
    def __init__(self, evidence, service):
        self.evidence = evidence
        self.service = service
        if service.client_kind == "go":
            binary = Path(os.environ.get("SCENARIO_GO_PROBE", ""))
            if not binary.is_absolute() or not os.access(binary, os.X_OK):
                raise EnvironmentBlocked(
                    "Go query cases require SCENARIO_GO_PROBE; see SCN-07 verify-wsl.sh"
                )
            argv = [str(binary)]
        else:
            argv = [
                sys.executable,
                "-B",
                str(Path(__file__).resolve().parents[1] / "assets/query_probe.py"),
            ]
        self.stderr = (evidence.directory / "probe-private.log").open("w")
        self.process = subprocess.Popen(
            [
                *argv,
                "--host",
                "127.0.0.1",
                "--port",
                str(service.port),
                "--transport",
                service.transport,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
            bufsize=1,
            env=service.env,
            cwd=evidence.directory,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            if self.process.poll() is None:
                self.call("close")
                self.process.wait(timeout=5)
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=5)
            self.stderr.close()

    def call(self, operation, **arguments):
        self.process.stdin.write(
            json.dumps({"operation": operation, **arguments}) + "\n"
        )
        self.process.stdin.flush()
        ready, _, _ = select.select([self.process.stdout], [], [], 20)
        require(bool(ready), f"Query probe timed out: {operation}")
        line = self.process.stdout.readline()
        require(
            bool(line), f"Query probe exited during {operation}; see probe-private.log"
        )
        response = json.loads(line)
        self.evidence.record(
            "query_probe",
            operation=operation,
            ok=response["ok"],
            client=self.service.client_kind,
        )
        return response

    def ok(self, operation, **arguments):
        response = self.call(operation, **arguments)
        require(response["ok"], f"Query {operation} failed: {response.get('error')}")
        return response["value"]
