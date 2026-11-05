import json
import os
import signal
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import pytest

import run as runner
from harness.catalog import CatalogError, load_catalog
from harness.runtime import Evidence, LocalService, RunError, wait_for

ROOT = Path(runner.__file__).parent


@pytest.mark.parametrize(
    ("edit", "error"),
    [
        (
            lambda text: text + text[text.index("[[cases]]") :],
            "duplicate ID",
        ),
        (
            lambda text: text.replace(
                'category = "submission"', 'category = "restoring"'
            ),
            "invalid category",
        ),
        (
            lambda text: text.replace('profiles = ["local"]', 'profiles = ["offline"]'),
            "requires profile local",
        ),
        (lambda text: text.replace("case.py", "../case.py"), "invalid entry path"),
        (lambda text: text.split("\nentry =", 1)[0], "requires an entry"),
    ],
)
def test_catalog_rejects_bad_rows(tmp_path, edit, error):
    source = "\n[[cases]]\n".join(
        (ROOT / "catalog.toml").read_text().split("\n[[cases]]\n", 2)[:2]
    )
    entry = tmp_path / "cases/execution/submission/exe_001_local_shell/case.py"
    entry.parent.mkdir(parents=True)
    entry.touch()
    catalog = tmp_path / "catalog.toml"
    catalog.write_text(edit(source))
    with pytest.raises(CatalogError, match=error):
        load_catalog(catalog)


def test_unimplemented_case_is_rejected(tmp_path, monkeypatch, capsys):
    case = load_catalog(ROOT / "catalog.toml")["DEF-001"]
    planned = replace(case, status="planned", entry=None)
    monkeypatch.setattr(runner, "load_catalog", lambda _: {planned.id: planned})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--runs-dir",
            str(tmp_path),
            "run",
            planned.id,
            "--profile",
            "offline",
        ],
    )
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 2
    assert "not an executable case" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("transport", ("grpc", "http"))
def test_offline_rejects_transport_selection(tmp_path, transport):
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            ROOT / "run.py",
            "--runs-dir",
            tmp_path,
            "run",
            "DEF-001",
            "--profile",
            "offline",
            "--transport",
            transport,
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 2
    assert "offline cases do not accept client or transport" in result.stderr
    assert not list(tmp_path.iterdir())


def test_offline_dispatch_never_creates_service(tmp_path, monkeypatch):
    def unexpected_service(*args):
        pytest.fail("offline execution created a service")

    monkeypatch.setattr(runner, "LocalService", unexpected_service)
    sample = tmp_path / "offline.py"
    sample.write_text(
        "def run(evidence):\n"
        '    (evidence.directory / "offline.txt").write_text("offline only")\n'
    )
    case = replace(load_catalog(ROOT / "catalog.toml")["DEF-001"], entry=sample)
    assert runner.execute(case, "offline", tmp_path / "runs") == 0
    report = read_report(tmp_path / "runs")
    assert report["result"] == "passed"
    assert "server" not in report


@pytest.mark.parametrize(
    "case_id", ("DEF-001", "DEF-002", "DEF-003", "PAR-001", "PAR-002")
)
def test_offline_case(tmp_path, monkeypatch, case_id):
    pytest.importorskip("takler")

    def unexpected_service(*args):
        pytest.fail("offline execution created a service")

    monkeypatch.setattr(runner, "LocalService", unexpected_service)
    case = load_catalog(ROOT / "catalog.toml")[case_id]
    assert case.kind == "offline" and case.status == "executable"
    assert runner.execute(case, "offline", tmp_path) == 0
    report = read_report(tmp_path)
    assert report["result"] == "passed" and report["checks"]
    assert "server" not in report and "cleanup" not in report


def test_wait_timeout():
    with pytest.raises(RunError, match="Timed out waiting for missing condition"):
        wait_for("missing condition", lambda: False, timeout=0.02)


def test_helper_cleanup_refuses_a_live_service(tmp_path, monkeypatch):
    pytest.importorskip("takler")
    from harness.recovery import cleanup_service_helpers

    case = load_catalog(ROOT / "catalog.toml")["RCV-003"]
    evidence = Evidence(tmp_path, case, "local")
    send = Mock()
    monkeypatch.setattr(subprocess, "run", send)
    with pytest.raises(RunError, match="server is still alive"):
        cleanup_service_helpers(evidence, os.getpid(), evidence.directory)
    send.assert_not_called()


def test_security_prepare_failure_does_not_start_service(tmp_path, monkeypatch):
    entry = tmp_path / "prepare_failure.py"
    entry.write_text(
        "from harness.runtime import RunError\n"
        "def prepare(evidence, service):\n"
        "    raise RunError('fixture preparation failed')\n"
        "def run(evidence, service):\n"
        "    raise AssertionError('case must not run')\n"
    )
    start = Mock(side_effect=AssertionError("must not start"))
    monkeypatch.setattr(LocalService, "start", start)
    case = replace(load_catalog(ROOT / "catalog.toml")["SEC-001"], entry=entry)
    assert runner.execute(case, "local", tmp_path / "runs") == 1
    start.assert_not_called()
    report = read_report(tmp_path / "runs")
    assert report["reason"] == "fixture preparation failed"
    assert "server" not in report


def test_missing_go_client_blocks_before_startup(tmp_path, monkeypatch):
    monkeypatch.setenv("SCENARIO_GO_CLIENT", str(tmp_path / "absent"))
    case = load_catalog(ROOT / "catalog.toml")["QRY-001"]
    assert runner.execute(case, "local", tmp_path / "runs", client="go") == 1
    report = read_report(tmp_path / "runs")
    assert report["result"] == "environment_blocked"
    assert "SCENARIO_GO_CLIENT" in report["reason"]
    assert "server" not in report


def test_offline_rejects_go_client(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            ROOT / "run.py",
            "--runs-dir",
            tmp_path,
            "run",
            "DEF-001",
            "--profile",
            "offline",
            "--client",
            "go",
        ],
        text=True,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 2
    assert "offline cases do not accept client or transport" in result.stderr
    assert not list(tmp_path.iterdir())


def read_report(runs):
    (path,) = runs.glob("*/report.json")
    return json.loads(path.read_text())


def test_failed_startup_is_recorded(tmp_path, monkeypatch):
    for name in ("takler-server", "takler-client-py"):
        path = tmp_path / name
        path.write_text("#!/bin/sh\nexit 17\n")
        path.chmod(0o700)

    def service(evidence):
        instance = LocalService(evidence)
        instance.bin = tmp_path
        return instance

    monkeypatch.setattr(runner, "LocalService", service)
    case = load_catalog(ROOT / "catalog.toml")["EXE-001"]
    runs = tmp_path / "runs"
    assert runner.execute(case, "local", runs) == 1
    report = read_report(runs)
    assert report["result"] == "failed"
    assert report["reason"] == "Service exited during startup (17)"
    assert report["cleanup"]["returncode"] == 17
    assert report["cleanup"]["port_free"]
    print(f"Startup failure: {report['reason']}, cleanup={report['cleanup']}")


def test_stop_escalates_only_the_owned_process(tmp_path, monkeypatch):
    case = load_catalog(ROOT / "catalog.toml")["EXE-001"]
    evidence = Evidence(tmp_path, case, "local")
    service = LocalService(evidence)
    process = Mock(pid=123, returncode=-9)
    process.poll.return_value = None
    process.wait.side_effect = [subprocess.TimeoutExpired("takler-server", 15), -9]
    service.process = process
    monkeypatch.setattr(service, "_listeners", lambda: [])
    with pytest.raises(RunError, match="did not shut down cleanly"):
        service.stop()
    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert evidence.report["cleanup"]["forced"]
    assert service.process is None


needs_takler = pytest.mark.skipif(
    not (Path(sys.executable).parent / "takler-server").is_file(),
    reason="requires the Takler Python environment",
)


@needs_takler
@pytest.mark.parametrize("transport", ("grpc", "http"))
def test_restart_keeps_addresses_and_recovers_halted(tmp_path, transport):
    case = load_catalog(ROOT / "catalog.toml")["RCV-001"]
    evidence = Evidence(tmp_path, case, "local")
    evidence.report.update(client="python", transport=transport)
    service = LocalService(evidence)
    try:
        service.start()
        first_ports = list(service.ports)
        first_pid = service.process.pid
        service.stop()
        service.start()
        assert service.ports == first_ports and service.process.pid != first_pid
        value = json.loads(service.client("server-status").stdout)
        assert value["status"] == "halted" and value["halt_causes"] == ["recovery"]
        assert value["restore_summary"]["source"] == "primary"
    finally:
        service.stop()
    assert len(evidence.report["server_history"]) == 2
    assert len(evidence.report["cleanup_history"]) == 2
    assert all(
        item["exit_expected"] and item["port_free"]
        for item in evidence.report["cleanup_history"]
    )


@needs_takler
def test_expected_checkpoint_refusal_is_not_a_ready_service(tmp_path):
    case = load_catalog(ROOT / "catalog.toml")["RCV-002"]
    evidence = Evidence(tmp_path, case, "local")
    service = LocalService(evidence)
    service.checkpoint.write_bytes(b"invalid checkpoint")
    try:
        service.start(expect_rejection=True)
        assert service.process.returncode == 1 and not service._listeners()
    finally:
        service.stop()
    events = [
        json.loads(line)
        for line in (evidence.directory / "events.jsonl").read_text().splitlines()
    ]
    assert not any(event["kind"] == "server_ready" for event in events)
    assert any(event["kind"] == "server_rejected" for event in events)
    assert evidence.report["cleanup"]["exit_expected"]
    assert evidence.report["cleanup"]["expected_returncode"] == 1


@needs_takler
def test_client_error_stops_real_server(tmp_path):
    entry = tmp_path / "client_error.py"
    entry.write_text(
        'def run(evidence, service):\n    service.client("load", "missing.json")\n'
    )
    case = replace(load_catalog(ROOT / "catalog.toml")["EXE-001"], entry=entry)
    runs = tmp_path / "runs"
    assert runner.execute(case, "local", runs) == 1
    report = read_report(runs)
    assert report["result"] == "failed"
    assert report["reason"].startswith("Client load exited")
    assert report["cleanup"]["returncode"] == 0 and report["cleanup"]["port_free"]
    print(f"Real client failure: {report['reason']}, cleanup={report['cleanup']}")


@needs_takler
def test_ctrl_c_stops_real_server(tmp_path):
    argv = [
        sys.executable,
        "-B",
        ROOT / "run.py",
        "--runs-dir",
        tmp_path,
        "run",
        "EXE-001",
        "--profile",
        "local",
    ]
    with subprocess.Popen(
        argv, start_new_session=True, stdout=subprocess.PIPE, text=True
    ) as process:
        try:

            def begun():
                files = list(tmp_path.glob("*/events.jsonl"))
                return (
                    files
                    and '"command": "begin", "returncode": 0' in files[0].read_text()
                )

            wait_for("real begin response", begun, timeout=15)
            process.send_signal(signal.SIGINT)
            output, _ = process.communicate(timeout=22)
            assert process.returncode == 130, output
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=22)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    raise

    report = read_report(tmp_path)
    assert report["result"] == "interrupted"
    assert report["cleanup"]["returncode"] == 0 and report["cleanup"]["port_free"]
    with pytest.raises(ProcessLookupError):
        os.kill(report["server"]["pid"], 0)
    print(f"Real Ctrl-C: runner exit=130, cleanup={report['cleanup']}")
