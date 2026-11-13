"""Independent authenticated readers and per-window, first-attempt evidence."""

import hashlib
import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Barrier

import grpc

from takler.client.grpc_transport import GrpcTransport
from takler.client.query_client import QueryClient
from takler.exceptions import QueryResponseError
from takler.protocol.query_v1 import DetailRequest, FailureReply

from ..runtime import RunError, require


def snapshot_inventory(view):
    return {
        "scope": view.scope_path,
        "query_id": view.query_id,
        "snapshot_id": view.snapshot_id,
        "node_kinds": dict(Counter(node.node_kind for node in view.nodes.values())),
        "paths_sha256": hashlib.sha256(
            "\n".join(sorted(view.ordered_paths)).encode()
        ).hexdigest(),
    }


class RpcTrace(grpc.UnaryUnaryClientInterceptor):
    """Record the wire boundary without persisting credentials or response bodies."""

    def __init__(self):
        self.calls = []
        self.before_page = None

    def intercept_unary_unary(self, continuation, details, request):
        if not isinstance(getattr(request, "json", None), bytes):
            return continuation(details, request)
        method = details.method.rsplit("/", 1)[-1]
        if self.before_page is not None and method == "ReadPage":
            self.before_page(json.loads(request.json))
        row = {
            "method": method,
            "request_hash": hashlib.sha256(request.json).hexdigest(),
            "start_ns": time.monotonic_ns(),
            "reply_bytes": 0,
        }
        try:
            response = continuation(details, request)
            row["code"] = response.code().name
            if response.code() == grpc.StatusCode.OK:
                payload = response.result().json
                row["reply_bytes"] = len(payload)
                # A query denial travels inside an OK gRPC response. The compact
                # QueryDocument encoder puts the discriminating kind first.
                if payload.startswith(b'{"kind":"error",'):
                    error = json.loads(payload)
                    row["query_error"] = error["code"]
                    if error["code"] == "reset_required":
                        row["reset_reason"] = error["message"]
                elif payload.startswith(b'{"kind":"reset",'):
                    reset = json.loads(payload)
                    row["reset_reason"] = reset["reason"]
                    row["reset_scope"] = reset["reset_scope"]
            return response
        finally:
            row["end_ns"] = time.monotonic_ns()
            self.calls.append(row)

    def evidence(self):
        calls = self.calls
        failed = set()
        retries = 0
        for row in calls:
            key = (row["method"], row["request_hash"])
            if key in failed:
                retries += 1
                failed.remove(key)
            if row.get("query_error") or row.get("code") != "OK":
                failed.add(key)
        return {
            "transport": "grpc",
            "retries": retries,
            "rpc_calls": len(calls),
            "reply_bytes": sum(row["reply_bytes"] for row in calls),
            "rpc_ns": sum(row["end_ns"] - row["start_ns"] for row in calls),
            "rpcs": calls,
        }


class _WindowTransport(GrpcTransport):
    """Each in-process reader gets its own loopback connection, as a CLI would."""

    def __init__(self, port, secret_file, timeout):
        super().__init__(
            host="127.0.0.1",
            port=port,
            secret_file=str(secret_file),
            single_timeout=timeout,
            retry_window=0,
        )
        self.trace = RpcTrace()

    def create_channel(self):
        channel = grpc.insecure_channel(
            self.listen_address,
            options=[
                ("grpc.enable_retries", 0),
                ("grpc.enable_http_proxy", 0),
                ("grpc.use_local_subchannel_pool", 1),
            ],
        )
        self.channel = grpc.intercept_channel(channel, self.trace)


class HeadlessWindow:
    def __init__(self, port, secret_file, scope, marker, timeout=15):
        self.scope, self.marker = scope, marker
        self.wire = _WindowTransport(port, secret_file, timeout)
        self.primary = QueryClient(self.wire)
        self.root = QueryClient(self.wire)

    def close(self):
        self.wire.close()

    def rpc_evidence(self):
        return self.wire.trace.evidence()

    def step(self, action, expected_suspended):
        self.wire.trace.calls = []
        before = self.primary.current
        if action == "inventory":
            return snapshot_inventory(before)
        if action == "disconnect":
            require(before is not None, "Disconnected reader has no committed view")
            self.close()
            return {"query_id": before.query_id, "revision": before.revision}
        if action == "fault":
            # Send the otherwise valid subtree identity to the real detail RPC.
            # A high-level read_detail call would reject the path locally.
            require(before is not None, "Fault needs an open window")
            restricted = before
            if self.scope == "/":
                restricted = self.root.read_snapshot(scope_path="/monitor", depth=0)
            rejected = self.wire.query(
                DetailRequest(
                    kind="detail_request",
                    schema_version=1,
                    session_id=restricted.session_id,
                    query_id=restricted.query_id,
                    path="/",
                    flow_generation=None,
                    attempt_id=None,
                    field_groups=["service"],
                )
            )
            if isinstance(rejected, FailureReply):
                raise QueryResponseError(rejected.code, rejected.message)
            raise RunError("Subtree credentials unexpectedly read the root detail")
        if action in {"open", "reconnect"}:
            require(self.wire.channel is None, "Window already connected")
            self.wire.open()
        if action == "open":
            require(before is None, "Cold open unexpectedly reused an existing window")
            view = self.primary.read_snapshot(scope_path=self.scope)
        else:
            require(
                action in {"refresh", "reconnect"}, f"Unknown monitor action {action}"
            )
            require(before is not None, "Incremental read needs an existing window")
            view = self.primary.read_since(before)
            require(
                view.query_id == before.query_id
                and view.snapshot_id == before.snapshot_id,
                "Incremental read replaced the original identity",
            )
            require(view.revision >= before.revision, "Reader went backwards")
        ready = time.monotonic_ns()
        marker = view.get(self.marker)
        require(
            view.scope_path == self.scope and not view.stale_scopes,
            "Unexpected stale scope",
        )
        require(
            marker is not None and marker.suspended is expected_suspended,
            "Reader did not see expected marker state",
        )
        changed = (
            before is not None and marker.suspended != before.get(self.marker).suspended
        )
        if changed:
            require(
                view.revision > before.revision and self.marker in view.changed_paths,
                "Changed marker did not advance from the reader's old waterline",
            )
        if self.scope == "/":
            root = view
            service = self.primary.read_detail(root, "/", ["service", "parameters"])
        else:
            root = self.root.read_snapshot(depth=0)
            require(root.ordered_paths == ("/",), "Service query escaped depth zero")
            service = self.root.read_detail(root, "/", ["service", "parameters"])
        root_ready = time.monotonic_ns()
        detail = self.primary.read_detail(view, self.marker, ["parameters"])
        labels = {item.name: item.value for item in detail.groups.parameters.user}
        require(
            labels.get("LABEL") == self.marker.rsplit("/", 1)[-1],
            "Wrong marker label in live detail",
        )
        require(service.groups.service.status == "running", "Unexpected service status")
        return {
            "scope": self.scope,
            "marker": self.marker,
            "nodes": len(view.ordered_paths),
            "revision": view.revision,
            "previous_revision": before.revision if before else None,
            "query_id": view.query_id,
            "snapshot_id": view.snapshot_id,
            "root_query_id": root.query_id,
            "root_snapshot_id": root.snapshot_id,
            "suspended": marker.suspended,
            "marker_changed": changed,
            "detail_path": detail.path,
            "service_status": service.groups.service.status,
            "summary_ready_ns": ready,
            "view_ready_ns": root_ready,
            "screen_ready_ns": None,
        }


class MonitorGroup:
    """Run one operation per selected window; preserve all first attempts on failure."""

    def __init__(self, evidence, windows, check):
        self.evidence, self.windows, self.check = evidence, list(windows), check
        require(bool(self.windows), "At least one monitor window is required")
        self.rows = []
        self.waves = []
        self.cleanup = ExitStack()
        for window in self.windows:
            self.cleanup.callback(window.close)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.cleanup.close()

    def wave(
        self,
        phase,
        action,
        expected_suspended,
        *,
        stagger_seconds=0,
        expected_failure=None,
        receipt_ns=None,
        indices=None,
    ):
        indices = list(range(len(self.windows)) if indices is None else indices)
        require(
            indices
            and len(set(indices)) == len(indices)
            and all(0 <= i < len(self.windows) for i in indices),
            "Invalid window selection",
        )
        require(stagger_seconds >= 0, "Stagger must be non-negative")
        require(
            expected_failure is None or expected_failure[0] in indices,
            "Expected failure needs a planned window",
        )
        wave = len(self.waves) + 1
        base_ns = time.monotonic_ns()
        barrier = Barrier(len(indices))
        plan = [
            {
                "wave": wave,
                "phase": phase,
                "action": action,
                "window": index,
                "scope": self.windows[index].scope,
                "marker": self.windows[index].marker,
                "planned_ns": base_ns + round(rank * stagger_seconds * 1e9),
                "receipt_ns": receipt_ns,
                "start_ns": None,
                "first_attempt": False,
                "retries": 0,
                "result": "not_sent",
                "error_code": None,
                "reset_reason": None,
                "expected_failure": expected_failure is not None
                and index == expected_failure[0],
            }
            for rank, index in enumerate(indices)
        ]
        # Leave the full intended denominator in evidence even on external interruption.
        self.evidence.record(
            "monitor_wave_planned",
            wave=wave,
            phase=phase,
            action=action,
            windows=indices,
            planned_ns=base_ns,
            stagger_seconds=stagger_seconds,
        )

        def run(row):
            actor = self.windows[row["window"]]
            try:
                barrier.wait(timeout=15)
                delay = (row["planned_ns"] - time.monotonic_ns()) / 1e9
                if delay > 0:
                    time.sleep(delay)
                self.check()
                row.update(
                    start_ns=time.monotonic_ns(), first_attempt=True, result="failed"
                )
                row.update(actor.step(action, expected_suspended))
                row["result"] = "ok"
            except Exception as exc:
                row["error_code"] = getattr(exc, "code", type(exc).__name__)
                row["error"] = str(exc)
                if row["error_code"] == "reset_required":
                    row["reset_reason"] = exc.message
            finally:
                row["end_ns"] = time.monotonic_ns()
                if row["first_attempt"] and hasattr(actor, "rpc_evidence"):
                    row.update(actor.rpc_evidence())
                    first_error = next(
                        (
                            rpc
                            for rpc in row.get("rpcs", [])
                            if rpc.get("query_error") or rpc.get("code") != "OK"
                        ),
                        None,
                    )
                    if first_error and row["result"] == "ok":
                        row.update(
                            result="failed",
                            error_code=first_error.get("query_error")
                            or first_error.get("code"),
                            reset_reason=first_error.get("reset_reason"),
                        )
                if receipt_ns is not None and row.get("view_ready_ns") is not None:
                    row["receipt_to_view_ns"] = row["view_ready_ns"] - receipt_ns
            return row

        with ThreadPoolExecutor(max_workers=len(indices)) as executor:
            rows = list(executor.map(run, plan))
        if action == "open":
            identities = set()
            for row in rows:
                if row["result"] != "ok":
                    continue
                pair = {row.get("query_id"), row.get("root_query_id")}
                expected = 1 if row["scope"] == "/" else 2
                if (
                    len(pair) != expected
                    or None in pair
                    or identities.intersection(pair)
                ):
                    row.update(
                        result="failed",
                        error_code="monitor_identity",
                        error="Unexpected or shared monitor query identity",
                    )
                identities.update(pair)
        self.rows.extend(rows)
        with (self.evidence.directory / "windows.jsonl").open("a") as out:
            for row in rows:
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
        errors = [r for r in rows if r["result"] != "ok"]
        attempted = [r for r in rows if r["first_attempt"]]
        rejected = {"permission_denied", "resource_exhausted", "snapshot_expired"}
        slowest = max(
            attempted, key=lambda r: r["end_ns"] - r["planned_ns"], default=None
        )
        summary = {
            "wave": wave,
            "phase": phase,
            "action": action,
            "planned": len(rows),
            "sent": len(attempted),
            "ok": len(rows) - len(errors),
            "not_sent": len(rows) - len(attempted),
            "failed": len(errors),
            "rejected": sum(r["error_code"] in rejected for r in errors),
            "timeouts": sum(
                r["error_code"] == "TimeoutError"
                or any(
                    rpc.get("code") == "DEADLINE_EXCEEDED" for rpc in r.get("rpcs", [])
                )
                for r in errors
            ),
            "resets": sum(r["reset_reason"] is not None for r in rows),
            "retries": sum(r["retries"] for r in rows),
            "slowest_attempted_window": slowest["window"] if slowest else None,
            "max_scheduled_to_end_ns": slowest["end_ns"] - slowest["planned_ns"]
            if slowest
            else None,
        }
        self.waves.append(summary)
        self.evidence.write_json("monitor-summary.json", self.waves)
        actual = [(r["window"], r["error_code"]) for r in errors]
        require(
            actual == ([] if expected_failure is None else [expected_failure])
            and not summary["not_sent"],
            f"{phase}: unexpected first-attempt failures {actual}, see windows.jsonl",
        )
        return rows
