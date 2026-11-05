import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from harness.catalog import load_catalog
from harness.jobs import Jobs, peak
from harness.runtime import Evidence, RunError, wait_for


def test_process_peak_counts_overlap_and_shared_endpoints():
    intervals = {
        "a": {"start": 1, "end": 5},
        "b": {"start": 2, "end": 4},
        "c": {"start": 5, "end": 8},
    }
    assert peak(intervals, "start", "end") == 2
    with pytest.raises(RunError, match="Empty process observation interval"):
        peak({"bad": {"start": 5, "end": 5}}, "start", "end")


def test_case_error_releases_owned_job_and_records_exit(tmp_path):
    case = load_catalog(Path(__file__).resolve().parents[1] / "catalog.toml")["RES-001"]
    evidence = Evidence(tmp_path, case, "local")
    process = None
    jobs = Jobs(evidence, None)
    try:
        with pytest.raises(RunError, match="controlled scenario failure"):
            with jobs:
                # A real process waits on the same cleanup gate as scenario Shell jobs.
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-B",
                        "-c",
                        "import os,time; from pathlib import Path; "
                        "Path('sample.pid').write_text(str(os.getpid())); "
                        "exec('while not Path(\"stop\").exists(): time.sleep(0.01)')",
                    ],
                    cwd=jobs.data,
                )
                wait_for(
                    "sample process observation",
                    lambda: "sample" in jobs.intervals,
                    timeout=3,
                )
                raise RunError("controlled scenario failure")
        assert process.wait(timeout=3) == 0
        saved = json.loads((evidence.directory / "process-intervals.json").read_text())
        assert saved["observer_error"] is None
        interval = saved["intervals"]["sample"]
        assert interval["pid"] == process.pid
        assert (
            interval["start_floor_ns"]
            <= interval["first_seen_ns"]
            < interval["exit_upper_ns"]
        )
        assert not jobs.thread.is_alive()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=3)


def test_controlled_template_renders_quoted_path_without_running(tmp_path):
    case = load_catalog(Path(__file__).resolve().parents[1] / "catalog.toml")["DEP-001"]
    evidence = Evidence(tmp_path / "path with spaces and ' quote", case, "local")
    jobs = Jobs(evidence, None)
    flow = jobs.flow("preview")
    task = jobs.task(flow, "work")
    task.create_job_script()
    script = Path(task.find_parameter("TAKLER_JOB").value)
    assert f"cd {shlex.quote(str(jobs.data))}\n" in script.read_text()
    assert not list(jobs.data.glob("*.pid"))
    assert task.attempt_id is None and not flow.begun
    subprocess.run(["bash", "-n", script], check=True, timeout=3)
