import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import run as runner
from harness.catalog import load_catalog
from harness.jobs import Jobs
from harness.runtime import Evidence, LocalService

ROOT = Path(runner.__file__).parent
VARIABLES = (
    "SCENARIO_CHILD_TIMEOUT_SECONDS",
    "SCENARIO_JOB_TIMEOUT_SECONDS",
    "SCENARIO_CLIENT_TIMEOUT_SECONDS",
)


@pytest.fixture(autouse=True)
def clear_control_timeouts(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, {"child": 6, "job": 10, "client": 8}),
        ({VARIABLES[0]: "17"}, {"child": 17, "job": 10, "client": 8}),
        ({VARIABLES[1]: "19"}, {"child": 6, "job": 19, "client": 8}),
        ({VARIABLES[2]: "23"}, {"child": 6, "job": 10, "client": 23}),
        (dict.fromkeys(VARIABLES, "30"), {"child": 30, "job": 30, "client": 30}),
    ],
)
def test_timeouts_reach_template_client_and_report(
    tmp_path, monkeypatch, overrides, expected
):
    for name, value in overrides.items():
        monkeypatch.setenv(name, value)
    evidence = Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")
    service = LocalService(evidence)
    service.port = 12345
    # Later environment changes must not alter this run's frozen controls.
    for name in VARIABLES:
        monkeypatch.setenv(name, "99")
    jobs = Jobs(evidence, service)
    flow = jobs.flow("preview")
    task = jobs.task(flow, "work")
    task.create_job_script()
    script = Path(task.find_parameter("TAKLER_JOB").value).read_text()
    assert f"timeout --foreground {expected['child']}s " in script
    assert "sleep 30" not in script
    assert task.attempt_id is None and not flow.begun
    execute = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr("harness.runtime.subprocess.run", execute)
    env = {"CUSTOM": "client override"}
    service.client("ping", env=env)
    assert execute.call_args.kwargs["timeout"] == expected["client"]
    assert execute.call_args.kwargs["env"] == env
    assert jobs.control_timeouts == expected
    evidence.finish("passed")
    report = json.loads((evidence.directory / "report.json").read_text())
    assert report["control_timeouts_seconds"] == expected
    assert service.process is None


@pytest.mark.parametrize("seconds", [10, 30])
def test_job_timeout_covers_ack_exit_cleanup_and_finish(tmp_path, monkeypatch, seconds):
    monkeypatch.setenv(VARIABLES[1], str(seconds))
    evidence = Evidence(tmp_path, SimpleNamespace(id="RES-001"), "local")
    jobs = Jobs(evidence, None)
    path, attempt = "/monitor/work", "attempt-one"
    key = jobs.key(path, attempt)
    jobs.intervals[key] = {}
    nodes = {path: {"groups": {"runtime": {"attempt_id": attempt}}}}
    waits = []

    def wait(description, check, *, timeout, **kwargs):
        waits.append((description, timeout))

    monkeypatch.setattr("harness.jobs.wait_for", wait)
    jobs.action(nodes, path, "meter:1")
    jobs.action(nodes, path, "success")
    jobs.thread = Mock()
    jobs.thread.is_alive.return_value = False
    jobs.__exit__(None, None, None)
    jobs.intervals.clear()
    assert jobs.finish({})["jobs"] == 0
    assert waits == [
        ("child attribute acknowledgment", seconds),
        ("job process exit", seconds),
        ("owned controlled jobs to exit", seconds),
        ("all observed jobs to exit", seconds),
    ]


@pytest.mark.parametrize("name", VARIABLES)
@pytest.mark.parametrize("value", ["", "0", "-1", "1.5", "nan", "inf", "30; exit 0"])
def test_invalid_timeout_is_reported_before_service_start(
    tmp_path, monkeypatch, name, value
):
    monkeypatch.setenv(name, value)
    entry = tmp_path / "unused.py"
    entry.write_text(
        "def run(evidence, service):\n    raise AssertionError('not run')\n"
    )
    case = replace(load_catalog(ROOT / "catalog.toml")["CAP-004"], entry=entry)
    start = Mock(side_effect=AssertionError("must not start"))
    monkeypatch.setattr(LocalService, "start", start)
    assert runner.execute(case, "local", tmp_path / "runs") == 1
    start.assert_not_called()
    (report_path,) = (tmp_path / "runs").glob("*/report.json")
    report = json.loads(report_path.read_text())
    assert report["result"] == "environment_blocked"
    assert name in report["reason"] and "positive integer" in report["reason"]
    assert "server" not in report
