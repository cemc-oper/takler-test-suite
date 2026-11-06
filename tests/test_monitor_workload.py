import hashlib
import json
import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from harness.monitor_workload import (
    ASSETS,
    MonitorWorkload,
    check_host,
    overlap_counts,
    prepare,
    source_version,
)
from harness.runtime import Evidence, LocalService, RunError


def test_freeze_smoke_config_before_launch(tmp_path, monkeypatch):
    source = tmp_path / "overrides.toml"
    raw = (
        (ASSETS / "monitoring.toml")
        .read_bytes()
        .replace(b"windows = 4", b"windows = 10")
    )
    source.write_bytes(raw)
    monkeypatch.setenv("SCENARIO_MONITOR_CONFIG", str(source))
    evidence = Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")
    service = LocalService(evidence)
    monkeypatch.setattr(
        "harness.monitor_workload.host_sample",
        lambda directory: {
            "available_bytes": 10 << 30,
            "swap_used_bytes": 1024,
            "disk_free_bytes": 100 << 30,
        },
    )
    prepare(evidence, service)
    saved = json.loads((evidence.directory / "monitor-config.json").read_text())
    assert saved["source_sha256"] == hashlib.sha256(raw).hexdigest()
    assert saved["tree"]["nodes"] == 34 and saved["tree"]["tasks"] == 32
    assert all(len(v["source_sha256"]) == 64 for v in saved["versions"].values())
    assert saved["config"]["windows"] == 10 and saved["config"]["job_concurrency"] == 1
    assert (
        service.process is None
        and not service.ports
        and service.checkpoint_interval == 10
    )


def test_source_fingerprint_tracks_real_files_and_ignores_bytecode(tmp_path):
    package = tmp_path / "package"
    package.mkdir()
    source = package / "client.py"
    source.write_text("one")
    initial = source_version(tmp_path, ["package"])
    assert initial["git_head"] is None
    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "client.pyc").write_bytes(b"cache")
    assert initial == source_version(tmp_path, ["package"])
    source.write_text("two")
    assert (
        initial["source_sha256"]
        != source_version(tmp_path, ["package"])["source_sha256"]
    )


def test_repeat_public_meter_and_check_each_server_value():
    service = SimpleNamespace(monitor_config={"job_concurrency": 1})
    workload = MonitorWorkload(None, service)
    workload.host = workload.resources = workload.jobs = Mock()
    path = workload.paths[0]

    def snapshot(value):
        return {
            path: {
                "groups": {
                    "runtime": {"attempt_id": "a", "meter_values": {"step": value}}
                }
            }
        }

    workload.active = snapshot(0)
    workload.jobs.phase.side_effect = [snapshot(25), snapshot(100), snapshot(100)]
    workload.report_meter(25)
    workload.report_meter()
    with pytest.raises(RunError, match="meter or attempt"):
        workload.report_meter(99)
    assert workload.meter_values == [25, 100]
    assert [call.args[-1] for call in workload.jobs.action.call_args_list] == [
        "meter:25",
        "meter:100",
        "meter:99",
    ]


@pytest.mark.parametrize(
    ("key", "value", "reason"),
    [
        ("available_bytes", 3 << 30, "memory"),
        ("disk_free_bytes", 19 << 30, "free-space"),
        ("swap_used_bytes", (1 << 30) + 8193, "swap growth"),
    ],
)
def test_host_guard_checks_deltas_and_free_space(key, value, reason):
    cfg = {
        "min_available_bytes": 4 << 30,
        "max_swap_growth_bytes": 1 << 30,
        "min_disk_free_bytes": 20 << 30,
    }
    sample = {
        "available_bytes": 5 << 30,
        "disk_free_bytes": 20 << 30,
        "swap_used_bytes": 8192,
    }
    check_host(sample, 8192, cfg)
    sample[key] = value
    with pytest.raises(RunError, match=reason):
        check_host(sample, 8192, cfg)


def test_possible_overlap_counts_each_timed_request_once():
    ranges = [
        {"start_lower_ns": 20, "end_upper_ns": 25},
        {"start_lower_ns": 25, "end_upper_ns": 35},
    ]
    rows = [
        {"start_ns": 10, "end_ns": 19},
        {"start_ns": 20, "end_ns": 35},
        {"start_ns": None, "end_ns": 30},
        {"start_ns": 35, "end_ns": 42},
    ]
    assert overlap_counts(rows, ranges) == {
        "timed": 3,
        "possible_checkpoint_io_overlap": 2,
    }
    assert overlap_counts(rows, [])["possible_checkpoint_io_overlap"] == 0


def test_failed_real_child_is_timed_without_credentials(tmp_path):
    output = tmp_path / "receipts.jsonl"
    result = subprocess.run(
        [
            sys.executable,
            str(ASSETS / "monitor_child.py"),
            str(output),
            "/bin/false",
            "meter",
        ],
        env={
            **os.environ,
            "TAKLER_NAME": "/monitor/worker0",
            "TAKLER_ATTEMPT_ID": "attempt-one",
            "TAKLER_PASS": "private-child-pass",
        },
    )
    assert result.returncode == 1
    row = json.loads(output.read_text())
    assert row["command"] == "meter" and row["attempt"] == "attempt-one"
    assert row["returncode"] == 1 and row["end_ns"] >= row["start_ns"]
    assert "private-child-pass" not in output.read_text()
