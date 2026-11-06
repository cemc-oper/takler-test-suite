import json
from types import SimpleNamespace

import grpc
import pytest

from harness.monitor_slow import slow_reader_refresh
from harness.monitor_windows import MonitorGroup, RpcTrace
from harness.runtime import Evidence, RunError


def exercise(tmp_path, *, single_page=False, peer_fails=False):
    trace = RpcTrace()
    state = {"suspended": True, "revision": 11}
    view = SimpleNamespace(
        revision=11,
        snapshot_id="pinned-snapshot",
        query_id="slow-reader",
        ordered_paths=("/monitor", "/monitor/marker"),
        get=lambda path: SimpleNamespace(suspended=True),
    )

    def read_snapshot(*, scope_path):
        for cursor in [None] if single_page else [None, "second-page"]:
            details = SimpleNamespace(method="/Query/ReadPage")
            req = SimpleNamespace(
                json=json.dumps({"scope_path": scope_path, "cursor": cursor}).encode()
            )
            response = SimpleNamespace(
                code=lambda: grpc.StatusCode.OK,
                result=lambda: SimpleNamespace(json=b"{}"),
            )
            trace.intercept_unary_unary(lambda *_: response, details, req)
        return view

    def reader(index):
        def step(action, expected):
            assert action == "refresh" and expected == state["suspended"]
            if index == 1 and peer_fails:
                raise RunError("peer failure")
            return {
                "query_id": view.query_id if index == 2 else f"peer-{index}",
                "snapshot_id": view.snapshot_id,
                "previous_revision": view.revision,
                "revision": state["revision"],
                "marker_changed": True,
                "suspended": expected,
            }

        return SimpleNamespace(
            step=step, close=lambda: None, scope="/monitor", marker="/monitor/marker"
        )

    def set_marker(suspended):
        import time

        state.update(suspended=suspended, revision=12)
        return time.monotonic_ns()

    readers = [reader(index) for index in range(3)]
    readers[-1].wire = SimpleNamespace(trace=trace)
    readers[-1].primary = SimpleNamespace(current=view, read_snapshot=read_snapshot)
    evidence = Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")
    workload = SimpleNamespace(
        cfg={"rpc_timeout_seconds": 1, "stop_timeout_seconds": 5},
        evidence=evidence,
        check=lambda: None,
        report_meter=lambda value: None,
        set_marker=set_marker,
    )
    return workload, MonitorGroup(evidence, readers, workload.check)


def result(workload):
    return json.loads((workload.evidence.directory / "slow-reader.json").read_text())


def test_waits_between_real_rpc_boundaries_until_peers_finish(tmp_path):
    workload, group = exercise(tmp_path)
    with group:
        assert slow_reader_refresh(workload, group) == {
            "peers": 2,
            "pages": 2,
            "caught_up_revision": 12,
        }
    evidence = result(workload)
    first, second = evidence["snapshot_rpc"]["rpcs"]
    assert first["end_ns"] <= evidence["held_ns"] <= evidence["receipt_ns"]
    assert all(
        evidence["receipt_ns"] <= r["start_ns"] <= r["end_ns"] <= evidence["release_ns"]
        for r in group.rows[:2]
    )
    assert evidence["release_ns"] <= second["start_ns"]
    assert evidence["result"] == "passed"


def test_peer_failure_releases_continuation_and_keeps_original_failure(tmp_path):
    workload, group = exercise(tmp_path, peer_fails=True)
    with group, pytest.raises(RunError, match="slow-peers"):
        slow_reader_refresh(workload, group)
    evidence = result(workload)
    assert evidence["result"] == "failed"
    assert evidence["release_ns"] <= evidence["snapshot_rpc"]["rpcs"][1]["start_ns"]
    assert group.windows[-1].wire.trace.before_page is None


def test_one_page_cannot_count_as_a_slow_reader(tmp_path):
    workload, group = exercise(tmp_path, single_page=True)
    with group, pytest.raises(RunError, match="without requesting a second page"):
        slow_reader_refresh(workload, group)
    evidence = result(workload)
    assert "held_ns" not in evidence and evidence["snapshot_rpc"]["rpc_calls"] == 1
    assert not group.rows
