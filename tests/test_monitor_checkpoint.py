import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from harness.monitor_checkpoint import checkpoint_refreshes
from harness.runtime import Evidence, RunError


def simulation(tmp_path, misses):
    evidence = Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")
    resources = SimpleNamespace(latest_checkpoint_start=None, windows=[])
    workload = SimpleNamespace(
        evidence=evidence,
        cfg={"checkpoint_seconds": 20},
        resources=resources,
        check=Mock(),
        report_meter=Mock(),
    )
    group = SimpleNamespace(windows=[0, 1], rows=[])

    def set_marker(suspended):
        index = len(resources.windows)
        receipt = (index + 1) * 100
        resources.latest_checkpoint_start = {
            "temp_name": str(index),
            "start_lower_ns": receipt + 1,
            "start_upper_ns": receipt + 2,
        }
        return receipt

    def wave(phase, action, suspended, receipt_ns):
        index = len(resources.windows)
        checkpoint = {
            **resources.latest_checkpoint_start,
            "end_lower_ns": receipt_ns + 20,
            "end_upper_ns": receipt_ns + 30,
            "bytes": 3,
            "matched_temp_rename": True,
        }
        resources.windows.append(checkpoint)
        for window in group.windows:
            rpc = {
                "method": "ReadSince",
                "start_ns": receipt_ns + 3,
                "end_ns": receipt_ns + 10,
            }
            later_page = {
                **rpc,
                "start_ns": receipt_ns + 10,
                "end_ns": receipt_ns + (21 if window == 1 and index in misses else 20),
            }
            group.rows.append(
                {
                    "window": window,
                    "result": "ok",
                    "start_ns": receipt_ns + 2,
                    "end_ns": receipt_ns + 25,
                    "rpcs": [rpc, later_page],
                    "marker_changed": True,
                    "suspended": suspended,
                    "revision": index + 2,
                    "previous_revision": index + 1,
                }
            )

    workload.set_marker = set_marker
    group.wave = wave
    return workload, group


def summary(workload):
    return json.loads(
        (workload.evidence.directory / "checkpoint-sync.json").read_text()
    )


def test_fully_contained_all_pages_on_distinct_saves_or_keep_missed_attempt(tmp_path):
    workload, group = simulation(tmp_path, {0})
    result = checkpoint_refreshes(workload, group, 3)
    assert result == {"requested": 3, "qualified": 3, "attempts": 4}
    attempts = summary(workload)["attempts"]
    assert [a["qualified"] for a in attempts] == [False, True, True, True]
    assert [a["observed_create"]["temp_name"] for a in attempts] == ["0", "1", "2", "3"]
    assert attempts[0]["all_reads_inside"] == 1 and attempts[0]["reason"]
    assert all(a["logical_inside"] == 0 and a["new_markers"] == 2 for a in attempts)
    assert workload.report_meter.call_count == 4


def test_bound_even_if_every_wave_misses_one_rpc(tmp_path):
    workload, group = simulation(tmp_path, range(99))
    with pytest.raises(RunError, match="Only 0/3"):
        checkpoint_refreshes(workload, group, 3)
    attempts = summary(workload)["attempts"]
    assert len(attempts) == 5
    assert all(a["planned"] == 2 and a["sent"] == 2 and a["reason"] for a in attempts)


def test_create_timeout_preserves_unsent_denominator(tmp_path, monkeypatch):
    workload, group = simulation(tmp_path, {})

    def timeout(description, check, **kwargs):
        raise RunError(f"Timed out waiting for {description}")

    monkeypatch.setattr("harness.monitor_checkpoint.wait_for", timeout)
    with pytest.raises(RunError, match="temp CREATE"):
        checkpoint_refreshes(workload, group, 3)
    (attempt,) = summary(workload)["attempts"]
    assert attempt["reason"].startswith("Timed out waiting")
    assert attempt["planned"] == 2 and attempt["sent"] == 0
    assert all(w["result"] == "not_sent" for w in attempt["windows"])
