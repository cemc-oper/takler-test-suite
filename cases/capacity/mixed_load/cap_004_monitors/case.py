"""Independent authenticated readers on a live workflow with periodic saves."""

import getpass
import os
import secrets
import subprocess
import time
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

from harness import monitor_workload
from harness.monitor_tree import tree_counts
from harness.monitor_tui import TuiWindow
from harness.monitor_windows import HeadlessWindow, MonitorGroup
from harness.recovery import cleanup_service_helpers, require_helper_runtime
from harness.runtime import require


def prepare(evidence, service):
    monitor_workload.prepare(evidence, service)
    require_helper_runtime()
    cfg = service.monitor_config
    cycles = cfg.get("poll_cycles", 0)
    require(type(cycles) is int and cycles >= 0, "Invalid steady refresh count")
    if cycles:
        require(
            type(cfg.get("refresh_seconds")) is int and cfg["refresh_seconds"] > 0,
            "A steady run requires a positive refresh interval",
        )
    # The server needs these files until shutdown, including a failed startup.
    keys = TemporaryDirectory(prefix="monitor-keys-", dir=evidence.directory)
    service.monitor_key_directory = keys
    directory = Path(keys.name)
    readers = []
    tokens = []
    for i in range(service.monitor_config["windows"] + 1):
        token = secrets.token_urlsafe(32)
        path = directory / ("writer" if i == 0 else f"observer-{i}")
        path.touch(mode=0o600)
        path.write_text(token + "\n")
        tokens.append(token)
        if i:
            readers.append(path)
    allow = directory / "operators"
    allow.touch(mode=0o600)
    allow.write_text("\n".join(tokens) + "\n")
    users = directory / "users"
    users.write_text(getpass.getuser() + "\n")
    service.security.update(
        auth_mode="enabled",
        operator_secret_file=str(allow),
        operator_whitelist_file=str(users),
        audit_file=str(evidence.directory / "audit.jsonl"),
    )
    service.env["TAKLER_SECRET_FILE"] = str(directory / "writer")
    service.monitor_secrets = tuple(readers)


def cleanup(evidence, service):
    keys = getattr(service, "monitor_key_directory", None)
    if keys is not None:
        keys.cleanup()
        removed = not Path(keys.name).exists()
        evidence.report["monitor_credentials_removed"] = removed
        require(removed, "Monitor credentials survived service shutdown")
    stopped = evidence.report.get("cleanup")
    if stopped:
        helpers = cleanup_service_helpers(
            evidence,
            stopped["pid"],
            service.directory,
            event="monitor_helpers_after_stop",
        )
        require(stopped["port_free"], "Monitor service port still in use")
        require(
            not any(item["signals"] for item in helpers),
            "Checkpoint helpers required forced cleanup",
        )


def check_connections(evidence, port, tui_pid, windows, phase):
    result = subprocess.run(
        ["ss", "-Hntp", "state", "established", f"dport = :{port}"],
        check=True,
        text=True,
        capture_output=True,
        timeout=5,
    )
    expected = {tui_pid: 1, os.getpid(): windows - 1}
    by_pid = {
        pid: sorted(
            {
                line.split()[2]
                for line in result.stdout.splitlines()
                if f"pid={pid}," in line
            }
        )
        for pid in expected
    }
    evidence.record("monitor_tcp_connections", phase=phase, local_endpoints=by_pid)
    require(
        all(len(by_pid[pid]) == count for pid, count in expected.items()),
        "Independent TCP peers differ from the planned readers",
    )


def steady_refreshes(workload, group, cfg):
    # Keep successive deadlines fixed even when a preceding wave finishes late.
    cycles = cfg.get("poll_cycles", 0)
    deadline = time.monotonic()
    for cycle in range(cycles):
        deadline += cfg["refresh_seconds"]
        while (remaining := deadline - time.monotonic()) > 0:
            workload.check()
            time.sleep(min(remaining, cfg["sample_seconds"]))
        workload.report_meter(1 + cycle % 99)
        suspended = cycle % 2 == 1
        ack = workload.set_marker(suspended)
        group.wave(
            f"steady-{cycle + 1}",
            "refresh",
            suspended,
            stagger_seconds=0.1 if cycle < cycles // 2 else 0,
            receipt_ns=ack,
        )


def run(evidence, service):
    scope, marker = "/monitor", "/monitor/marker"
    timeout = service.capacity["rpc_timeout_seconds"]
    with monitor_workload.MonitorWorkload(evidence, service) as workload:
        with ExitStack() as opening:
            windows = []
            for index, secret in enumerate(service.monitor_secrets):
                args = service.port, secret, scope, marker, timeout
                window = (
                    TuiWindow(*args, evidence=evidence)
                    if index == 0
                    else HeadlessWindow(*args)
                )
                opening.callback(window.close)
                windows.append(window)
            group = MonitorGroup(evidence, windows, workload.check)
            with group:
                opening.pop_all()  # The group now owns all readers.
                opened = group.wave("cold-open", "open", False)
                expected = tree_counts(service.monitor_config)["nodes"] - 1
                require(
                    all(row["nodes"] == expected for row in opened),
                    "A window missed nodes in its initial subtree",
                )
                check_connections(
                    evidence, service.port, windows[0].process.pid, len(windows), "open"
                )
                group.wave("unchanged", "refresh", False)
                workload.report_meter()
                ack = workload.set_marker(True)
                group.wave("synchronous-change", "refresh", True, receipt_ns=ack)
                group.wave(
                    "scope-denial",
                    "fault",
                    True,
                    indices=[1],
                    expected_failure=(1, "permission_denied"),
                )
                ack = workload.set_marker(False)
                group.wave(
                    "staggered-change",
                    "refresh",
                    False,
                    stagger_seconds=0.1,
                    receipt_ns=ack,
                )
                group.wave("disconnect", "disconnect", False)
                ack = workload.set_marker(True)
                group.wave("catch-up", "reconnect", True, receipt_ns=ack)
                check_connections(
                    evidence,
                    service.port,
                    windows[0].process.pid,
                    len(windows),
                    "reconnect",
                )
                steady_refreshes(workload, group, service.monitor_config)
                checks = workload.finish(group.rows)
        evidence.record_checks(windows=len(windows), **checks)
