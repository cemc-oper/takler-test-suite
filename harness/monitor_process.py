"""Dedicated client interpreters for CAP-004 with a shared parent-side IPC protocol."""

from multiprocessing.connection import Connection
import os
from pathlib import Path
import socket
import subprocess
import sys

from takler.exceptions import QueryResponseError

from .monitor_windows import HeadlessWindow, RpcTrace
from .runtime import RunError, require


class ProcessWindow:
    """Launch a fresh interpreter and keep its IPC and shutdown outcome per reader."""

    def __init__(
        self, port, secret_file, scope, marker, timeout=15, *, evidence=None, index=None
    ):
        self.timeout = timeout
        self.scope, self.marker = scope, marker
        self.evidence = evidence
        self.index = index
        self._exit_reported = False
        self._ipc_timed_out = False
        self.cleanup_outcome = None
        self._trace = {}
        env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
        env.update(
            TAKLER_SECRET_FILE=str(Path(secret_file).resolve()),
            TAKLER_TIMEOUT="0",
            no_proxy="localhost,127.0.0.1",
            NO_PROXY="localhost,127.0.0.1",
            no_grpc_proxy="localhost,127.0.0.1",
        )
        parent, child = socket.socketpair()
        with parent, child:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    self.module,
                    str(child.fileno()),
                    str(port),
                    scope,
                    marker,
                    str(timeout),
                ],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                pass_fds=(child.fileno(),),
                stdin=subprocess.DEVNULL,
            )
            self.conn = Connection(parent.detach())
        try:
            require(
                self._result(self.timeout + 2) == "ready", "Missing reader handshake"
            )
        except BaseException:
            self.close()
            raise

    def _result(self, timeout):
        if not self.conn.poll(timeout):
            # The stream has no request IDs. Discard it and reap the unresponsive
            # process so its late reply can never be consumed by a later wave.
            self._ipc_timed_out = True
            self.close()
            raise TimeoutError(f"{self.label} IPC timed out")
        ok, result, self._trace = self.conn.recv()
        if not ok:
            code, message = result
            if code == "TimeoutError":
                raise TimeoutError(message)
            if code:
                raise QueryResponseError(code, message)
            raise RunError(message)
        return result

    def step(self, action, expected_suspended):
        self._trace = {}
        try:
            self.conn.send((action, expected_suspended))
            return self._result(self.step_timeout)
        except TimeoutError:
            raise
        except (EOFError, OSError) as exc:
            # The wave ledger owns the first failure. close() still records this exit.
            self._exit_reported = True
            raise RunError(f"{self.label} exited before replying") from exc

    def rpc_evidence(self):
        if self._trace:
            return self._trace
        # A process can disappear after sending an RPC but before sending its
        # evidence. The empty list counts only the records received by the parent.
        return {**RpcTrace().evidence(), "rpc_trace_missing": True}

    def close(self):
        if self.cleanup_outcome is not None:
            return self.cleanup_outcome
        alive = self.process.poll() is None
        sent = False
        escalation = None
        try:
            try:
                if alive and self._ipc_timed_out:
                    escalation = "kill"
                    self.process.kill()
                elif alive and not self.conn.closed:
                    self.conn.send(("quit", None))
                    sent = True
            except OSError:
                pass
            finally:
                self.conn.close()
            try:
                self.process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                escalation = "terminate"
                self.process.terminate()
                try:
                    self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    escalation = "kill"
                    self.process.kill()
                    self.process.wait(timeout=2)
        finally:
            returncode = self.process.poll()
            outcome = {
                "pid": self.process.pid,
                "quit_sent": sent,
                "forced": escalation,
                "returncode": returncode,
                "reaped": returncode is not None,
                "ipc_closed": self.conn.closed,
                "ipc_timed_out": self._ipc_timed_out,
            }
            self.cleanup_outcome = outcome
            if self.evidence is not None:
                self.record_cleanup(outcome)
        require(
            returncode is not None
            and (
                self._ipc_timed_out
                or (
                    escalation is None
                    and (self._exit_reported or (sent and returncode == 0))
                )
            ),
            f"{self.label} did not shut down cleanly: {outcome}",
        )
        return outcome


class HeadlessProcessWindow(ProcessWindow):
    module = "harness.monitor_process"
    label = "Headless monitor"

    @property
    def step_timeout(self):
        # Individual RPCs keep the thread-mode deadline. Give a complete
        # multi-page operation a separate watchdog in case the interpreter hangs.
        return max(300, self.timeout + 2)

    def record_cleanup(self, outcome):
        outcome["window"] = self.index
        self.evidence.report.setdefault("monitor_headless_cleanup", []).append(outcome)
        self.evidence.record("monitor_headless_cleanup", **outcome)


def _session(conn, port, scope, marker, timeout):
    window = HeadlessWindow(
        port, os.environ["TAKLER_SECRET_FILE"], scope, marker, timeout
    )
    try:
        conn.send((True, "ready", {}))
        while True:
            try:
                action, expected = conn.recv()
            except EOFError:
                return
            if action == "quit":
                return
            try:
                result = window.step(action, expected)
            except QueryResponseError as exc:
                reply = (False, (exc.code, exc.message))
            except TimeoutError:
                reply = (False, ("TimeoutError", f"Headless {action} timed out"))
            except Exception as exc:
                message = str(exc) if isinstance(exc, RunError) else type(exc).__name__
                reply = (False, (None, message))
            else:
                reply = (True, result)
            conn.send((*reply, window.rpc_evidence()))
    finally:
        window.close()


if __name__ == "__main__":
    fd, port, scope, marker, timeout = sys.argv[1:]
    with Connection(int(fd)) as conn:
        _session(conn, int(port), scope, marker, float(timeout))
