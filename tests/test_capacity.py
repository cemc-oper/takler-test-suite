import hashlib
import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from harness.capacity import Resources, descendants, prepare
from harness.catalog import load_catalog
from harness.jobs import process_identity
from harness.runtime import Evidence, LocalService, RunError, wait_for

ROOT = Path(__file__).resolve().parents[1]


def test_sampler_identifies_checkpoint_child_without_persisting_command_line():
    child = multiprocessing.get_context("spawn").Process(target=time.sleep, args=(30,))
    child.start()
    try:
        rows = descendants(os.getpid(), server_pid=os.getpid())
        worker = next(row for row in rows if row["pid"] == child.pid)
        assert worker["ppid"] == os.getpid()
        assert worker["helper_role"] == "checkpoint_worker"
        assert "cmdline" not in worker
    finally:
        child.terminate()
        child.join(5)
        child.close()


def evidence(tmp_path):
    return Evidence(tmp_path, load_catalog(ROOT / "catalog.toml")["CAP-002"], "local")


def test_capacity_configuration_is_frozen_before_service_start(tmp_path, monkeypatch):
    item = evidence(tmp_path)
    service = LocalService(item)
    source = tmp_path / "capacity.toml"
    original = (ROOT / "assets/capacity.toml").read_bytes()
    source.write_bytes(original)
    monkeypatch.setenv("SCENARIO_CAPACITY_CONFIG", str(source))
    prepare(item, service)
    source.write_text("invalid after prepare")
    frozen = json.loads((item.directory / "capacity-config.json").read_text())
    assert frozen["sha256"] == hashlib.sha256(original).hexdigest()
    assert service.capacity["endurance"]["duration_seconds"] == 7200
    assert service.checkpoint_interval == 20 and service.stop_timeout == 180
    assert service.process is None and not service.ports


def test_external_monitor_observes_checkpoint_rename(tmp_path, monkeypatch):
    item = evidence(tmp_path)
    service = LocalService(item)
    monkeypatch.delenv("SCENARIO_CAPACITY_CONFIG", raising=False)
    prepare(item, service)
    with Resources(item, service) as resources:
        wait_for("resource observer started", lambda: resources.samples, timeout=3)
        temporary = item.directory / "takler.check.tmp.test"
        temporary.write_bytes(b"independent filesystem observation")
        start = wait_for(
            "checkpoint temp create",
            lambda: resources.latest_checkpoint_start,
            timeout=3,
        )
        temporary.replace(service.checkpoint)
        wait_for("checkpoint primary rename", lambda: resources.windows, timeout=3)
    summary = json.loads((item.directory / "resource-summary.json").read_text())
    assert summary["error"] is None
    event = summary["checkpoint_windows"][0]
    assert event["temp_name"] == start["temp_name"] == temporary.name
    assert event["start_upper_ns"] == start["start_upper_ns"]
    assert event["matched_temp_rename"]
    assert event["start_lower_ns"] <= event["start_upper_ns"] <= event["end_upper_ns"]
    assert event["bytes"] == len(b"independent filesystem observation")


def test_external_monitor_distinguishes_unrelated_temp_from_publication(tmp_path):
    item = evidence(tmp_path)
    service = LocalService(item)
    prepare(item, service)
    with Resources(item, service) as resources:
        wait_for("resource observer started", lambda: resources.samples, timeout=3)
        unrelated = item.directory / "unrelated"
        unrelated.write_bytes(b"new")
        (item.directory / "takler.check.tmp.abandoned").touch()
        wait_for("temp CREATE", lambda: resources.latest_checkpoint_start, timeout=3)
        unrelated.replace(service.checkpoint)
        wait_for("primary rename", lambda: resources.windows, timeout=3)
    assert not resources.windows[0]["matched_temp_rename"]


def test_resource_limit_is_reported_without_signalling_other_processes(
    tmp_path, monkeypatch
):
    item = evidence(tmp_path)
    service = LocalService(item)
    monkeypatch.delenv("SCENARIO_CAPACITY_CONFIG", raising=False)
    prepare(item, service)
    service.capacity["max_owned_processes"] = 0
    resources = Resources(item, service)
    with pytest.raises(RunError, match="Owned process count exceeded"):
        with resources:
            wait_for(
                "frozen resource stop condition", lambda: resources.error, timeout=3
            )
            resources.check()
    saved = json.loads((item.directory / "resource-summary.json").read_text())
    assert "Owned process count exceeded" in saved["error"]


def test_proc_fd_permission_race_ignores_only_exited_process(tmp_path, monkeypatch):
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    original = Path.iterdir

    def fd_race(path):
        if path == Path(f"/proc/{process.pid}/fd"):
            process.terminate()
            process.wait(timeout=3)
            raise PermissionError("exited during fd lookup")
        return original(path)

    try:
        monkeypatch.setattr(Path, "iterdir", fd_race)
        assert descendants(process.pid) == []
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)


def test_proc_persistent_permission_error_remains_a_measurement_failure(monkeypatch):
    original = Path.iterdir

    def denied(path):
        if path == Path(f"/proc/{os.getpid()}/fd"):
            raise PermissionError("live process remains unreadable")
        return original(path)

    monkeypatch.setattr(Path, "iterdir", denied)
    with pytest.raises(PermissionError, match="live process remains unreadable"):
        descendants(os.getpid())


def test_detached_job_tree_is_counted_once_and_start_time_must_match():
    with subprocess.Popen(
        ["sh", "-c", 'sleep 30 & echo "$!"; wait'],
        start_new_session=True,
        stdout=subprocess.PIPE,
        text=True,
    ) as job:
        try:
            child = int(job.stdout.readline())
            root = (job.pid, process_identity(job.pid))
            # The sampler root cannot reach its parent; a registered job adds
            # the full subtree. Overlapping roots must not double count RSS.
            rows = descendants(child, extra_roots=[root, root])
            assert sorted(row["pid"] for row in rows) == sorted([child, job.pid])
            stale = (job.pid, root[1] - 1)
            assert [r["pid"] for r in descendants(child, extra_roots=[stale])] == [
                child
            ]
        finally:
            os.killpg(job.pid, signal.SIGTERM)
