"""Exercise mixed business scopes and real lease expiry on the live CAP-004 service."""

import copy
import hashlib
import json
import os
import subprocess
import time

from takler.exceptions import QueryResponseError

from .monitor_windows import HeadlessWindow, MonitorGroup
from .runtime import RunError, require


def definition_paths(node, parent=""):
    path = parent + "/" + node["name"]
    yield path
    for child in node["children"]:
        yield from definition_paths(child, path)


class ScopeReader(HeadlessWindow):
    def __init__(self, service, secret, scope, marker, paths):
        super().__init__(
            service.port, secret, scope, marker, service.capacity["rpc_timeout_seconds"]
        )
        self.paths = paths
        self.path_hash = hashlib.sha256("\n".join(sorted(paths)).encode()).hexdigest()

    def step(self, action, expected_suspended):
        self.wire.trace.calls = []
        old = self.primary.current
        if action == "disconnect":
            return super().step(action, expected_suspended)
        if action in {"scope-open", "expired-since"}:
            self.wire.open()
        if action == "expired-since":
            try:
                self.primary.read_since(old)
            except QueryResponseError as error:
                require(
                    (error.code, error.message, error.reset_scope)
                    == ("reset_required", "history_expired", self.scope),
                    f"Unexpected reset for {self.scope}: {error}",
                )
                require(
                    self.primary.current.is_stale(self.scope),
                    "Reset did not mark scope stale",
                )
                return dict(
                    expected_reset=error.code,
                    reset_reason=error.message,
                    reset_scope=error.reset_scope,
                    session_id=old.session_id,
                    query_id=old.query_id,
                    snapshot_id=old.snapshot_id,
                    revision=old.revision,
                )
            raise RunError("An expired reader waterline was accepted")
        if action in {"scope-open", "rebuild"}:
            view = self.primary.read_snapshot(scope_path=self.scope)
            if action == "rebuild":
                require(
                    old.is_stale(self.scope)
                    and view.session_id == old.session_id
                    and view.snapshot_id != old.snapshot_id,
                    "Expected a fresh snapshot of the stale scope in the same session",
                )
        else:
            require(
                action in {"changed", "after-rebuild"}, "Unknown mixed-scope action"
            )
            view = self.primary.read_since(old)
            require(
                (view.query_id, view.snapshot_id) == (old.query_id, old.snapshot_id),
                "Unexpected rebuild during incremental refresh",
            )
        ready = time.monotonic_ns()
        require(
            view.scope_path == self.scope
            and not view.stale_scopes
            and view.nodes.keys() == self.paths,
            "Snapshot differs from the input definition",
        )
        marker = view.get(self.marker)
        if self.marker in self.paths:
            require(
                marker is not None and marker.suspended is expected_suspended,
                "Marker state differs from acknowledged write",
            )
            if action == "changed":
                require(
                    self.marker in view.changed_paths and view.revision > old.revision,
                    "Changed marker did not advance its reader",
                )
        elif action == "changed":
            require(
                not view.changed_paths and not view.removed_paths,
                "An unrelated scope reported a change",
            )
        return dict(
            session_id=view.session_id,
            query_id=view.query_id,
            snapshot_id=view.snapshot_id,
            revision=view.revision,
            previous_revision=old.revision if old else None,
            nodes=len(view.nodes),
            paths_sha256=self.path_hash,
            changed_paths=view.changed_paths,
            marker_suspended=marker.suspended if marker else None,
            view_ready_ns=ready,
        )


def record_peers(evidence, service, phase, count):
    sockets = subprocess.check_output(
        ["ss", "-Hntp", "state", "established", f"dport = :{service.port}"],
        text=True,
        timeout=5,
    )
    endpoints = {
        line.split()[2]
        for line in sockets.splitlines()
        if f"pid={os.getpid()}," in line
    }
    evidence.record("scope_connections", phase=phase, endpoints=sorted(endpoints))
    require(len(endpoints) == count, f"{phase}: expected {count} independent TCP peers")
    return endpoints


def verify_scope_rebuild(workload):
    """Run after the original group closes, with all real jobs completed and the guards active."""
    service = workload.service
    evidence = copy.copy(workload.evidence)
    evidence.directory = evidence.directory / "scope-rebuild"
    evidence.directory.mkdir()
    definition = json.loads((workload.evidence.directory / "monitor.json").read_text())
    paths = {"/", *definition_paths(definition["root"])}
    cycles = [
        f"/monitor/cycle{hour}" for hour in service.capacity["structure"]["cycles"]
    ]
    marker = cycles[0] + "/data/h000/prepare"
    require(marker in paths, "Missing leaf for the mixed-scope test")
    scopes = ["/", *cycles]
    by_scope = {
        scope: frozenset(
            p for p in paths if p == scope or p.startswith(scope.rstrip("/") + "/")
        )
        for scope in scopes
    }
    readers = []
    for index, secret in enumerate(service.monitor_secrets):
        scope = scopes[index % len(scopes)]
        readers.append(ScopeReader(service, secret, scope, marker, by_scope[scope]))
    count = len(readers)
    with MonitorGroup(evidence, readers, workload.check) as group:
        opened = group.wave("mixed-open", "scope-open", False)
        require(
            len({row["query_id"] for row in opened}) == count,
            "Mixed scopes did not retain independent query identities",
        )
        opened_endpoints = record_peers(evidence, service, "mixed-open", count)
        service.client("suspend", marker)
        ack = time.monotonic_ns()
        evidence.record("online_marker_suspended", ack_ns=ack, path=marker)
        group.wave("mixed-change", "changed", True, receipt_ns=ack)
        group.wave("mixed-disconnect", "disconnect", True)
        record_peers(evidence, service, "offline", 0)
        offline = time.monotonic_ns()
        service.client("resume", marker)
        evidence.record(
            "offline_marker_resumed", ack_ns=time.monotonic_ns(), path=marker
        )
        # Each disconnected reader must outlive the product's 120-second lease.
        until = offline + 125_000_000_000
        while (remaining := until - time.monotonic_ns()) > 0:
            workload.check()
            time.sleep(min(remaining / 1e9, service.capacity["sample_seconds"]))
        offline_end = time.monotonic_ns()
        evidence.record("lease_expiry_wait", offline_ns=offline, end_ns=offline_end)
        resets = group.wave("expired-reconnect", "expired-since", False)
        require(
            all(
                [
                    (rpc.get("reset_reason"), rpc.get("reset_scope"))
                    for rpc in r["rpcs"]
                    if rpc["method"] == "ReadSince"
                ]
                == [("history_expired", r["scope"])]
                for r in resets
            ),
            "Expected one history_expired ReadSince reply naming each reader's scope",
        )
        new_endpoints = record_peers(evidence, service, "expired-reconnect", count)
        rebuilt = group.wave("mixed-rebuild", "rebuild", False)
        require(
            len({row["query_id"] for row in rebuilt}) == count,
            "Rebuilt scopes lost independent query identities",
        )
        group.wave("after-rebuild", "after-rebuild", False)
        require(
            not any(row["retries"] for row in group.rows),
            "Unexpected mixed-scope RPC retry",
        )
    return dict(
        windows=count,
        scope_sizes={k: len(v) for k, v in by_scope.items()},
        expected_history_expired=len(resets),
        rebuilt=len(rebuilt),
        offline_ns=offline_end - offline,
        reused_endpoint_count=len(opened_endpoints & new_endpoints),
        marker=marker,
    )
