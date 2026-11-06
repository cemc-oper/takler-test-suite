import json
import time
from types import SimpleNamespace

import grpc
import pytest

from harness.monitor_windows import MonitorGroup, RpcTrace
from harness.runtime import Evidence, RunError
from takler.exceptions import QueryResponseError


class Reader:
    scope = "/monitor"
    marker = "/monitor/marker"

    def __init__(self, index, fault=None):
        self.index, self.fault = index, fault
        self.closed = False
        self.calls = 0

    def step(self, action, expected):
        self.calls += 1
        if self.fault:
            raise self.fault
        return {
            "query_id": f"scope-{self.index}",
            "root_query_id": f"root-{self.index}",
            "suspended": expected,
            "view_ready_ns": time.monotonic_ns(),
        }

    def close(self):
        self.closed = True


def run_evidence(tmp_path):
    return Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")


def ledger(evidence):
    return [
        json.loads(line)
        for line in (evidence.directory / "windows.jsonl").read_text().splitlines()
    ]


def test_monitor_wave_keeps_first_failure_and_valid_siblings(tmp_path):
    evidence = run_evidence(tmp_path)
    readers = [
        Reader(0),
        Reader(1, QueryResponseError("permission_denied", "outside scope")),
        Reader(2),
    ]
    receipt = time.monotonic_ns()
    with MonitorGroup(evidence, readers, lambda: None) as group:
        rows = group.wave(
            "fault",
            "refresh",
            True,
            receipt_ns=receipt,
            expected_failure=(1, "permission_denied"),
        )
        assert [r["window"] for r in rows] == [0, 1, 2]
        assert [r["result"] for r in rows] == ["ok", "failed", "ok"]
        assert rows[1]["expected_failure"] and rows[1]["retries"] == 0
        assert rows[0]["receipt_to_view_ns"] >= 0
        assert group.waves[-1]["rejected"] == 1
        assert group.waves[-1]["sent"] == 3
        assert ledger(evidence) == rows
        with pytest.raises(RunError, match="missing-fault"):
            group.wave(
                "missing-fault",
                "refresh",
                True,
                indices=[2],
                expected_failure=(2, "permission_denied"),
            )
    assert all(r.closed for r in readers)


def test_monitor_wave_records_unsent_and_reset_without_retry(tmp_path):
    evidence = run_evidence(tmp_path)

    def blocked():
        raise RunError("private resource limit")

    reader = Reader(0, QueryResponseError("reset_required", "history_expired"))
    with MonitorGroup(evidence, [reader], blocked) as group:
        with pytest.raises(RunError, match="stop"):
            group.wave("stop", "refresh", False)
        assert reader.calls == 0
        assert ledger(evidence)[0]["result"] == "not_sent"
        assert group.waves[-1]["sent"] == 0
        group.check = lambda: None
        with pytest.raises(RunError, match="reset"):
            group.wave("reset", "refresh", False)
        assert reader.calls == 1
        assert ledger(evidence)[1]["reset_reason"] == "history_expired"
        assert group.waves[-1]["resets"] == 1


def test_duplicate_open_identity_fails_in_the_window_ledger(tmp_path):
    evidence = run_evidence(tmp_path)
    with MonitorGroup(evidence, [Reader(0), Reader(0)], lambda: None) as group:
        with pytest.raises(RunError, match="monitor_identity"):
            group.wave("cold-open", "open", False)
    assert [r["error_code"] for r in ledger(evidence)] == [None, "monitor_identity"]


def test_transparent_retry_keeps_first_denial_in_ledger(tmp_path):
    trace = RpcTrace()
    request = SimpleNamespace(json=b'{"kind":"page_request","schema_version":1}')
    method = SimpleNamespace(method="/Query/ReadPage")
    for reply in (b'{"kind":"error","code":"resource_exhausted"}', b'{"kind":"page"}'):
        response = SimpleNamespace(
            code=lambda: grpc.StatusCode.OK, result=lambda: SimpleNamespace(json=reply)
        )
        trace.intercept_unary_unary(lambda details, req: response, method, request)
    reader = Reader(0)
    reader.rpc_evidence = trace.evidence
    evidence = run_evidence(tmp_path)
    with MonitorGroup(evidence, [reader], lambda: None) as group:
        with pytest.raises(RunError, match="resource_exhausted"):
            group.wave("refresh", "refresh", False)
        assert group.waves[0]["retries"] == 1
        assert group.waves[0]["rejected"] == 1
        assert ledger(evidence)[0]["rpcs"][0]["query_error"] == "resource_exhausted"
