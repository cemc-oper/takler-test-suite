import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


class RunError(RuntimeError):
    """A safe, user-facing run diagnostic."""


class EnvironmentBlocked(RunError):
    pass


def require(condition, message):
    if not condition:
        raise RunError(message)


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class Evidence:
    def __init__(self, root, case, profile):
        root.mkdir(parents=True, exist_ok=True)
        self.directory = Path(tempfile.mkdtemp(prefix=f"{case.id.lower()}-", dir=root))
        self.report = {
            "run_id": self.directory.name,
            "case_id": case.id,
            "profile": profile,
            "started_at": timestamp(),
            "platform": platform.release(),
            "python": platform.python_version(),
            "result": "running",
        }
        self.write_json("report.json", self.report)

    def write_json(self, name, value):
        target = self.directory / name
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
        temporary.replace(target)

    def record(self, kind, **fields):
        event = {
            "at": timestamp(),
            "run_id": self.directory.name,
            "kind": kind,
            **fields,
        }
        with (self.directory / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(event, ensure_ascii=False) + "\n")

    def record_checks(self, **checks):
        self.report["checks"] = checks
        self.record("case_checks", **checks)

    def finish(self, result, reason=None):
        self.report.update(result=result, reason=reason, ended_at=timestamp())
        self.write_json("report.json", self.report)


def wait_for(description, check, *, timeout, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    raise RunError(f"Timed out waiting for {description}")


class LocalService:
    """One owned WSL service, selected CLI, and isolated config."""

    def __init__(self, evidence, *, name="server"):
        self.evidence = evidence
        self.name = name
        self.directory = (
            evidence.directory if name == "server" else evidence.directory / name
        )
        self.directory.mkdir(exist_ok=True)
        self.transport = evidence.report.get("transport", "grpc")
        self.checkpoint = self.directory / "takler.check"
        self.checkpoint_interval = 600
        self.stop_timeout = 15
        self.config = self.directory / "connect.yaml"
        self.process = None
        self.generation = 0
        self.expected_returncode = 0
        self.port = None
        self.grpc_port = None
        self.ports = []
        self.bin = Path(sys.executable).absolute().parent
        self.client_kind = evidence.report.get("client", "python")
        self.client_bin = (
            Path(os.environ.get("SCENARIO_GO_CLIENT", "takler_client"))
            if self.client_kind == "go"
            else self.bin / "takler-client-py"
        )
        self.security = {}
        home = self.directory / "home"
        home.mkdir()
        self.env = {
            "HOME": str(home),
            "PATH": f"{self.bin}:/usr/bin:/bin",
            "TAKLER_TRANSPORT": self.transport,
            "TAKLER_TIMEOUT": "0",
            "TAKLER_CONNECT_FILE": str(self.config),
            "TZ": "Asia/Shanghai",
        }

    @property
    def address(self):
        return ("--host", "127.0.0.1", "--port", str(self.port))

    def _listeners(self):
        selection = " or ".join(
            f"sport = :{port}" for port in (self.ports or [self.port])
        )
        try:
            return subprocess.check_output(
                ["ss", "-H", "-ltnp", selection],
                text=True,
                timeout=3,
                env=self.env,
            ).splitlines()
        except (OSError, subprocess.SubprocessError) as exc:
            raise EnvironmentBlocked(
                "Cannot inspect the local listener with ss"
            ) from exc

    def _owns_listener(self, lines):
        return bool(lines) and all(
            re.findall(r"pid=(\d+),", line) == [str(self.process.pid)] for line in lines
        )

    def start(self, *, expect_rejection=False):
        require(self.process is None, "Stop the owned service before restarting it")
        for name in ("takler-server", "takler-client-py"):
            if not os.access(self.bin / name, os.X_OK):
                raise EnvironmentBlocked(
                    f"Missing {name} beside the selected Python interpreter"
                )
        if self.client_kind == "go" and (
            not self.client_bin.is_absolute() or not os.access(self.client_bin, os.X_OK)
        ):
            raise EnvironmentBlocked(
                "Go scenarios require an absolute executable SCENARIO_GO_CLIENT"
            )
        if not self.ports:
            sockets = []
            try:
                for _ in range(2 if self.transport == "http" else 1):
                    sock = socket.socket(socket.AF_INET6)
                    sockets.append(sock)
                    sock.bind(("::", 0))
                    self.ports.append(sock.getsockname()[1])
            finally:
                for sock in sockets:
                    sock.close()
        self.grpc_port = self.ports[0]
        self.port = self.ports[-1]
        if self._listeners():
            raise RunError(f"Port {self.port} is already in use")
        server_config = {
            "address": {
                "hostname": "127.0.0.1",
                "ip": "127.0.0.1",
                "port": str(self.grpc_port),
            },
            "transport": self.transport,
        }
        if self.transport == "http":
            server_config["http"] = {"host": "127.0.0.1", "port": str(self.port)}
        self.config.write_text(
            json.dumps(
                {
                    "server": server_config,
                    "checkpoint": {
                        "file": str(self.checkpoint),
                        "interval": self.checkpoint_interval,
                    },
                    "security": self.security,
                },
                indent=2,
            )
            + "\n"
        )
        argv = [
            str(self.bin / "takler-server"),
            "--config",
            str(self.config),
        ]
        self.generation += 1
        self.expected_returncode = 1 if expect_rejection else 0
        log_name = (
            "server.log" if self.generation == 1 else f"server-{self.generation}.log"
        )
        with (self.directory / log_name).open("w") as log:
            self.process = subprocess.Popen(
                argv,
                cwd=self.directory,
                env=self.env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        details = {
            "pid": self.process.pid,
            "port": self.port,
            "ports": self.ports,
            "transport": self.transport,
            "config": str(self.config.relative_to(self.evidence.directory)),
            "generation": self.generation,
            "log": str(
                (self.directory / log_name).relative_to(self.evidence.directory)
            ),
            "checkpoint": str(self.checkpoint.relative_to(self.evidence.directory)),
            "checkpoint_interval": self.checkpoint_interval,
        }
        self.evidence.record("server_start", name=self.name, **details)
        if self.name == "server":
            self.evidence.report["server"] = details
            self.evidence.report.setdefault("server_history", []).append(details)
        else:
            self.evidence.report.setdefault("peers", {})[self.name] = details

        if expect_rejection:
            samples = 0

            def rejected():
                nonlocal samples
                samples += 1
                require(not self._listeners(), "Rejected startup opened a listener")
                return self.process.poll() is not None

            wait_for("checkpoint startup refusal", rejected, timeout=20, interval=0.02)
            require(
                self.process.returncode == 1,
                "Startup did not reject invalid checkpoints",
            )
            self.evidence.record(
                "server_rejected",
                name=self.name,
                pid=self.process.pid,
                listener_samples=samples,
                returncode=1,
            )
            return

        def ready():
            if self.process.poll() is not None:
                raise RunError(
                    f"Service exited during startup ({self.process.returncode})"
                )
            lines = self._listeners()
            if lines and not self._owns_listener(lines):
                raise RunError(f"Listener ownership mismatch on port {self.port}")
            if lines:
                result = self.client("ping", allow_failure=True)
                if result.returncode:
                    (self.directory / "readiness-error.log").write_text(
                        result.stdout + result.stderr
                    )
                return result.returncode == 0
            return False

        wait_for("the owned Takler listener and ping", ready, timeout=20)
        self.evidence.record(
            "server_ready", name=self.name, pid=self.process.pid, port=self.port
        )

    def crash(self):
        require(
            self.process is not None and self.process.poll() is None,
            "Crash requires the live owned service",
        )
        self.expected_returncode = -signal.SIGKILL
        self.evidence.record(
            "fault_signal", name=self.name, pid=self.process.pid, signal="SIGKILL"
        )
        self.process.kill()
        self.process.wait(timeout=10)
        require(
            self.process.returncode == -signal.SIGKILL,
            "Service did not exit from SIGKILL",
        )
        self.stop()

    def client(self, name, *args, allow_failure=False, address=None, env=None):
        # Log operation names and outcomes. Keep raw diagnostics in the private run directory.
        started = time.monotonic()
        try:
            result = subprocess.run(
                [
                    str(self.client_bin),
                    name,
                    *(self.address if address is None else address),
                    *args,
                ],
                cwd=self.directory,
                env=self.env if env is None else env,
                text=True,
                capture_output=True,
                timeout=8,
            )
        except subprocess.TimeoutExpired as exc:
            self.evidence.record("client", command=name, result="timeout")
            raise RunError(f"Client {name} timed out") from exc
        self.evidence.record(
            "client",
            command=name,
            returncode=result.returncode,
            duration_s=round(time.monotonic() - started, 3),
        )
        if result.returncode and not allow_failure:
            (self.directory / "client-error.log").write_text(
                result.stdout + result.stderr
            )
            raise RunError(f"Client {name} exited {result.returncode}")
        return result

    def stop(self):
        process = self.process
        if process is None:
            return
        forced = False
        # Finish bounded cleanup even if another Ctrl-C arrives during shutdown.
        previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=self.stop_timeout)
                except subprocess.TimeoutExpired:
                    forced = True
                    process.kill()
                    process.wait(timeout=5)
            listeners = self._listeners()
            own_listener = any(f"pid={process.pid}," in line for line in listeners)
            outcome = {
                "pid": process.pid,
                "returncode": process.returncode,
                "forced": forced,
                "port_free": not listeners,
                "owned_listener_closed": not own_listener,
                "expected_returncode": self.expected_returncode,
                "exit_expected": process.returncode == self.expected_returncode,
            }
            if self.name == "server":
                self.evidence.report["cleanup"] = outcome
                self.evidence.report.setdefault("cleanup_history", []).append(outcome)
            else:
                self.evidence.report.setdefault("peer_cleanup", {})[self.name] = outcome
            self.evidence.record("server_stop", name=self.name, **outcome)
            if forced or own_listener or not outcome["exit_expected"]:
                raise RunError("Service did not shut down cleanly")
        finally:
            self.process = None
            signal.signal(signal.SIGINT, previous)
