"""One real Textual monitor with its own process, credentials and gRPC channel."""

import asyncio
from contextlib import AsyncExitStack
from multiprocessing.connection import Connection
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from xml.etree import ElementTree

import grpc
from takler.exceptions import QueryResponseError

from .runtime import RunError, require


class TuiWindow:
    def __init__(self, port, secret_file, scope, marker, timeout=15, *, evidence=None):
        self.timeout = timeout
        self.scope, self.marker = scope, marker
        self.evidence = evidence
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
                    "harness.monitor_tui",
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
            self._result()  # Import and IPC are ready; the first RPC waits for open.
        except BaseException:
            self.close()
            raise

    def _result(self):
        if not self.conn.poll(self.timeout + 2):
            raise TimeoutError("Textual monitor timed out")
        try:
            ok, result, self._trace = self.conn.recv()
        except EOFError as exc:
            raise RunError("Textual monitor exited before replying") from exc
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
        self.conn.send((action, expected_suspended))
        return self._result()

    def rpc_evidence(self):
        return self._trace

    def close(self):
        if self.cleanup_outcome is not None:
            return self.cleanup_outcome
        alive = self.process.poll() is None
        sent = False
        escalation = None
        try:
            try:
                if alive and not self.conn.closed:
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
            }
            self.cleanup_outcome = outcome
            if self.evidence is not None:
                self.evidence.report["monitor_tui_cleanup"] = outcome
                self.evidence.record("monitor_tui_cleanup", **outcome)
        require(
            sent and escalation is None and returncode == 0,
            f"Textual monitor did not shut down cleanly: {outcome}",
        )
        return outcome


def _screen_observation(svg, marker, state, suspended):
    """Read the Info tab's path and precise state from the composed screen image."""
    rows = {}
    for node in ElementTree.fromstring(svg).iter("{http://www.w3.org/2000/svg}text"):
        row = rows.setdefault(node.get("y"), [])
        row.append("".join(node.itertext()).replace("\xa0", " "))
    lines = [" ".join(" ".join(row).split()) for row in rows.values()]
    label = f"suspend ({state})" if suspended else state
    path_visible = any(
        "Path:" in line and marker in line.split("Path:", 1)[1].split()
        for line in lines
    )
    state_visible = any(
        "State:" in line and line.split("State:", 1)[1].strip(" ■") == label
        for line in lines
    )
    return path_visible and state_visible


async def _session(conn, port, scope, marker, timeout):
    # Imported inside the child's interpreter so preparing a suite has no UI side effects.
    from takler.tui.app import TaklerTuiApp
    from takler.tui.service import TaklerTuiService
    from takler.tui.widgets import NodeTree
    from textual.widgets import Switch
    from harness.monitor_windows import RpcTrace, snapshot_inventory

    class ObservedService(TaklerTuiService):
        """Timestamp the real service's public return values before the app draws."""

        view = detail = root = None
        views = snapshots = details = 0
        view_ns = root_ns = 0
        failure = None

        def observed_call(self, read, *args, **options):
            try:
                return read(*args, **options)
            except QueryResponseError as exc:
                if exc.code != "cancelled":
                    self.failure = exc
                raise

        def read_snapshot(self, **options):
            result = self.observed_call(super().read_snapshot, **options)
            self.view, self.view_ns = result, time.monotonic_ns()
            self.snapshots += 1
            self.views += 1
            return result

        def read_since(self, snapshot, **options):
            result = self.observed_call(super().read_since, snapshot, **options)
            self.view, self.view_ns = result, time.monotonic_ns()
            self.views += 1
            return result

        def read_root_detail(self, **options):
            result = self.observed_call(super().read_root_detail, **options)
            self.root, self.root_ns = result, time.monotonic_ns()
            return result

        def read_detail(self, snapshot, path, groups, **options):
            result = self.observed_call(
                super().read_detail, snapshot, path, groups, **options
            )
            if result is not None:
                if path == "/":
                    self.root, self.root_ns = result, time.monotonic_ns()
                if path == marker:
                    self.detail = result
                    self.details += 1
            return result

    service = ObservedService("127.0.0.1", port, transport_name="grpc")
    app = TaklerTuiApp(service, scope_path=scope)
    pilot = None

    async def until(test, explanation):
        deadline = time.monotonic() + timeout
        while not test():
            if service.failure is not None:
                raise service.failure
            if time.monotonic() >= deadline:
                raise TimeoutError(explanation)
            await pilot.pause(0.01)

    async def act(action, expected):
        nonlocal pilot
        before = service.view
        if action == "inventory":
            return snapshot_inventory(before)
        if action == "disconnect":
            require(before is not None, "Disconnected TUI has no committed view")
            service.close()  # Preserve the app and its last committed revision.
            return {"query_id": before.query_id, "revision": before.revision}
        count, captures, old_details = service.views, service.snapshots, service.details
        if action == "open":
            require(pilot is None, "Textual monitor already opened")
            pilot = await stack.enter_async_context(app.run_test(size=(120, 40)))
            # The shared scenario clock drives every window at its planned times.
            app.query_one("#toolbar-auto-switch", Switch).value = False
        elif action in {"refresh", "reconnect"}:
            require(pilot is not None, "Textual monitor has not opened")
            app.action_refresh()
        else:
            raise RunError(f"Unknown Textual monitor action {action}")
        await until(
            lambda: service.views > count and service.root is not None,
            "TUI did not receive its view and root detail",
        )
        if service.failure is not None:
            raise service.failure
        require(
            service.snapshots == captures + (action == "open"),
            "TUI unexpectedly rebuilt its initial view",
        )
        view = service.view
        require(view.scope_path == scope and not view.stale_scopes, "Stale TUI scope")
        summary = view.get(marker)
        require(
            summary is not None and summary.suspended == expected,
            "TUI summary has the wrong marker state",
        )
        changed = before is not None and before.get(marker).suspended != expected
        if before is not None:
            require(
                before.query_id == view.query_id
                and before.snapshot_id == view.snapshot_id
                and view.revision >= before.revision,
                "TUI incremental read replaced its identity or revision went backwards",
            )
        if changed:
            require(
                view.revision > before.revision and marker in view.changed_paths,
                "TUI marker changed without advancing its old revision",
            )
        await until(
            lambda: not any(w.group == "refresh" and w.is_running for w in app.workers),
            "TUI refresh did not commit to the application",
        )
        # Select the real tree row through its public widget interface, like a
        # user opening task details. Enter dispatches its actual selection event.
        if action == "open":
            tree = app.query_one(NodeTree)
            tree.focus()
            tree.reveal_query_path(marker)
            await pilot.pause()
            tree.select_path(marker)
            await pilot.press("enter")
        await until(
            lambda: _screen_observation(
                app.export_screenshot(), marker, summary.status, expected
            ),
            "TUI composed screen did not show the selected marker path and state",
        )
        screen_ns = time.monotonic_ns()
        if service.details == old_details:
            await pilot.press("enter")
        await until(
            lambda: service.details > old_details,
            "TUI did not receive selected task details",
        )
        detail = service.detail
        require(
            any(
                p.name == "LABEL" and p.value == marker.rsplit("/", 1)[-1]
                for p in detail.groups.parameters.user
            ),
            "TUI marker detail lost its expected LABEL",
        )
        require(detail.path == marker, "TUI detail was for another node")
        require(
            service.root.groups.service.status == "running", "Unexpected service status"
        )
        return dict(
            scope=scope,
            marker=marker,
            query_id=view.query_id,
            root_query_id=service.root.query_id,
            revision=view.revision,
            previous_revision=before.revision if before else None,
            snapshot_id=view.snapshot_id,
            suspended=summary.suspended,
            detail_path=detail.path,
            marker_changed=changed,
            service_status=service.root.groups.service.status,
            nodes=len(view.ordered_paths),
            summary_ready_ns=service.view_ns,
            view_ready_ns=max(service.view_ns, service.root_ns),
            screen_ready_ns=screen_ns,
            screen_driver="textual-run-test",
        )

    # Intercept the plaintext gRPC channel only in this dedicated process.
    # All product routing, request validation and credential handling stay intact.
    channel = grpc.insecure_channel
    trace = RpcTrace()

    def traced_channel(*args, **kwargs):
        return grpc.intercept_channel(channel(*args, **kwargs), trace)

    grpc.insecure_channel = traced_channel
    try:
        async with AsyncExitStack() as stack:
            conn.send((True, "ready", {}))
            while True:
                try:
                    action, expected = await asyncio.to_thread(conn.recv)
                except EOFError:
                    break
                if action == "quit":
                    break
                trace.calls = []
                service.failure = None
                try:
                    result = await asyncio.wait_for(act(action, expected), timeout)
                    conn.send((True, result, trace.evidence()))
                except Exception as exc:
                    if isinstance(exc, TimeoutError):
                        failure = ("TimeoutError", f"Textual {action} timed out")
                    elif isinstance(exc, QueryResponseError):
                        failure = (exc.code, exc.message)
                    else:
                        message = (
                            str(exc)
                            if isinstance(exc, RunError)
                            else type(exc).__name__
                        )
                        failure = (None, f"Textual {action}: {message}")
                    conn.send((False, failure, trace.evidence()))
    finally:
        grpc.insecure_channel = channel
        service.close()


if __name__ == "__main__":
    fd, port, scope, marker, timeout = sys.argv[1:]
    with Connection(int(fd)) as conn:
        asyncio.run(_session(conn, int(port), scope, marker, float(timeout)))
