"""Use a fresh headless client interpreter without starting a server."""

import os
import signal
from types import SimpleNamespace

import pytest

from takler_test_suite.harness.monitoring.process import HeadlessProcessWindow
from takler_test_suite.harness.runtime import Evidence, RunError


@pytest.mark.parametrize("crash", [False, True])
def test_headless_worker_handshake_failure_and_cleanup(tmp_path, crash):
    evidence = Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")
    window = HeadlessProcessWindow(
        1,
        tmp_path / "unused",
        "/monitor",
        "/monitor/marker",
        evidence=evidence,
        index=2,
    )
    try:
        # Preparation has opened neither a gRPC channel nor a view.
        with pytest.raises(RunError, match="existing window"):
            window.step("refresh", False)
        assert window.rpc_evidence()["rpc_calls"] == 0
        assert not window.rpc_evidence().get("rpc_trace_missing")
        if crash:
            # A lost reply must never report a preceding wave's RPC evidence.
            window._trace = {"rpc_calls": 1, "rpcs": [{"method": "ReadPage"}]}
            window.process.terminate()
            window.process.wait(timeout=5)
            with pytest.raises(RunError, match="exited before replying"):
                window.step("refresh", False)
            assert window.rpc_evidence()["rpcs"] == []
            assert window.rpc_evidence()["rpc_trace_missing"]
    finally:
        outcome = window.close()

    assert evidence.report["monitor_headless_cleanup"] == [outcome]
    assert outcome["window"] == 2 and outcome["pid"] == window.process.pid
    assert outcome["ipc_closed"] and outcome["reaped"] and outcome["forced"] is None
    assert outcome["quit_sent"] is not crash
    assert (outcome["returncode"] == 0) is not crash
    assert window.close() == outcome


def test_ipc_watchdog_reaps_stalled_child_before_another_step(tmp_path, monkeypatch):
    evidence = Evidence(tmp_path, SimpleNamespace(id="CAP-004"), "local")
    window = HeadlessProcessWindow(
        1,
        tmp_path / "unused",
        "/monitor",
        "/monitor/marker",
        evidence=evidence,
        index=4,
    )
    try:
        assert window.step_timeout > window.timeout
        monkeypatch.setattr(HeadlessProcessWindow, "step_timeout", 0.05)
        window.process.send_signal(signal.SIGSTOP)
        _, status = os.waitpid(window.process.pid, os.WUNTRACED)
        assert os.WIFSTOPPED(status)
        with pytest.raises(TimeoutError, match="IPC timed out"):
            window.step("refresh", False)
        with pytest.raises(RunError, match="exited before replying"):
            window.step("refresh", False)
        assert window.rpc_evidence()["rpc_calls"] == 0
    finally:
        if window.process.poll() is None:
            window.process.send_signal(signal.SIGCONT)
        outcome = window.close()

    assert outcome["ipc_timed_out"] and outcome["forced"] == "kill"
    assert outcome["reaped"] and outcome["ipc_closed"]
    assert evidence.report["monitor_headless_cleanup"] == [outcome]
