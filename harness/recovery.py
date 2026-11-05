"""Checkpoint references and public service observations for recovery scenarios."""

import hashlib
import json
import os
import subprocess
import stat
import sys
import time
from datetime import datetime
from pathlib import Path

from takler.core import NodeStatus
from takler.serialization.checkpoint import restore_checkpoint

from .jobs import process_identity
from .runtime import EnvironmentBlocked, require, wait_for

IDENTITY_FIELDS = (
    "attempt_id",
    "task_id",
    "try_no",
    "submitted_at",
    "started_at",
    "ended_at",
    "aborted_reason",
    "submission_unknown_reason",
)


def instant(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def status(evidence, service, label):
    value = json.loads(service.client("server-status").stdout)
    evidence.write_json(f"{label}.status.json", value)
    return value


def await_status(evidence, service, label, predicate, timeout=35):
    def sample():
        value = status(evidence, service, "latest-status")
        return value if predicate(value) else None

    value = wait_for(label, sample, timeout=timeout)
    evidence.write_json(f"{label}.status.json", value)
    return value


def checkpoint(evidence, path, label, expected):
    raw = path.read_bytes()
    restore_checkpoint(
        raw
    )  # Public full-document validation; never installs state in the server.
    document = json.loads(raw)
    require(
        document["format_version"] == 3 and stat.S_IMODE(path.stat().st_mode) == 0o600,
        "Checkpoint version or file permissions differ",
    )
    private = evidence.directory / f"private-{label}.check"
    private.write_bytes(raw)
    private.chmod(0o600)
    nodes = document["runtime"]["nodes"]
    require(
        all(
            NodeStatus(nodes[p]["state"]["status"]).name == value
            for p, value in expected.items()
        ),
        "Checkpoint states differ from the independent reference",
    )
    # Never export secrets or the credential-bearing original checkpoint.
    public = {
        "format_version": 3,
        "saved_at": document["saved_at"],
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "mode": "0600",
        "server_state": document["runtime"]["server_state"],
        "nodes": {
            p: {
                "status": NodeStatus(item["state"]["status"]).name,
                **{key: item[key] for key in IDENTITY_FIELDS if key in item},
                "limit_usage": item["limit_usage"],
                "definition_generation": item.get("definition_generation"),
                "event_values": item["event_values"],
                "meter_values": item["meter_values"],
            }
            for p, item in nodes.items()
        },
        "in_flight_secret_entries": len(document["secrets"]["job_passwords"]),
    }
    evidence.write_json(f"{label}.checkpoint.json", public)
    return raw, public


def same_runtime(reference, nodes, paths):
    for path in paths:
        disk, node = reference["nodes"][path], nodes[path]
        require(
            node["summary"]["status"] == disk["status"],
            "Restored status differs from checkpoint",
        )
        require(
            all(node["groups"]["runtime"][key] == disk[key] for key in IDENTITY_FIELDS),
            "Restored task identity/times differ from checkpoint",
        )
        require(
            node["summary"]["definition_generation"] == disk["definition_generation"],
            "Restored definition generation differs",
        )


def recovered(evidence, service, reference, *, source="primary", in_flight=1):
    value = status(evidence, service, f"recovered-{service.generation}")
    summary = value["restore_summary"]
    require(
        value["status"] == "halted" and value["halt_causes"] == ["recovery"],
        "Restart did not default to recovery halt",
    )
    require(
        summary["source"] == source
        and summary["fallback"] == (source == "backup")
        and summary["in_flight_count"] == in_flight,
        "Wrong recovery source or in-flight count",
    )
    require(
        summary["node_count"] == len(reference["nodes"]) - 1
        and summary["flow_count"]
        == sum(p != "/" and p.count("/") == 1 for p in reference["nodes"]),
        "Recovery summary counts differ from the saved tree",
    )
    require(
        instant(summary["saved_at"])
        == instant(reference["saved_at"])
        == instant(value["last_checkpoint_at"]),
        "Recovery selected a different generation",
    )
    require(
        summary["snapshot_status"] == reference["server_state"]["status"]
        and summary["effective_status"] == "halted",
        "Saved/effective service states were conflated",
    )
    return value


def external(evidence, jobs, nodes, paths, label):
    result = {}
    for path in paths:
        runtime = nodes[path]["groups"]["runtime"]
        key = jobs.key(path, runtime["attempt_id"])
        interval = jobs.intervals[key]
        ticks = process_identity(interval["pid"])
        result[path] = {
            "attempt_id": runtime["attempt_id"],
            "pid": interval["pid"],
            "start_ticks": interval["start_ticks"],
            "observed_ticks": ticks,
            "same_process_alive": ticks == interval["start_ticks"],
        }
    evidence.write_json(f"{label}.external.json", result)
    return result


def helper_command():
    return [
        "/usr/bin/python3",
        "-I",
        "-S",
        str(Path(__file__).resolve().parents[1] / "assets/cleanup_service_helpers.py"),
    ]


def require_helper_runtime():
    try:
        result = subprocess.run(
            [*helper_command(), "--check"], capture_output=True, text=True, timeout=5
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise EnvironmentBlocked(
            "RCV-003 requires WSL system Python with Linux pidfd APIs"
        ) from exc
    if result.returncode:
        raise EnvironmentBlocked(
            "RCV-003 requires WSL system Python with Linux pidfd APIs"
        )


def cleanup_service_helpers(evidence, server_pid, directory):
    """Clean only helpers bound to the dead owned service's group and cwd."""
    require(
        process_identity(server_pid) is None,
        "Refusing helper cleanup while the server is still alive",
    )
    require(
        directory.is_relative_to(evidence.directory),
        "Helper directory is outside this run",
    )
    result = subprocess.run(
        [
            *helper_command(),
            "--server-pid",
            str(server_pid),
            "--directory",
            str(directory),
            "--python",
            sys.executable,
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    if result.returncode:
        (evidence.directory / "helper-error.log").write_text(
            result.stdout + result.stderr
        )
        require(False, "Owned checkpoint helper cleanup failed; see helper-error.log")
    value = json.loads(result.stdout)
    evidence.record(
        "service_helpers_after_sigkill",
        server_pid=server_pid,
        observed_at=value["observed_at"],
        helpers=value["helpers"],
    )
    for item in value["cleanup"]:
        evidence.record("service_helper_cleanup", **item)
    evidence.report.setdefault("helper_cleanup", []).extend(value["cleanup"])
    return value["cleanup"]


def read_only_directory(evidence, directory, label):
    require(
        os.geteuid() != 0
        and directory.is_relative_to(evidence.directory)
        and not directory.is_symlink(),
        "Permission fault requires an owned directory and non-root WSL user",
    )
    directory.chmod(0o500)
    probe = directory / "write-probe"
    try:
        with probe.open("xb"):
            pass
    except PermissionError:
        evidence.record(
            "storage_fault",
            label=label,
            directory=str(directory),
            mode="0500",
            create_probe="PermissionError",
        )
    else:
        probe.unlink()
        require(False, "Checkpoint directory is still writable")


def hold_pair(
    evidence, service, jobs, states, main, backup, saved_at, causes, label, seconds=13
):
    start = time.monotonic()
    samples = 0
    while True:
        nodes = jobs.view("latest")
        require(
            all(nodes[p]["summary"]["status"] == value for p, value in states.items()),
            "State changed while checkpoint failure halted service",
        )
        for path, value in states.items():
            if value == "queued":
                require(
                    nodes[path]["groups"]["runtime"]["attempt_id"] is None,
                    "Paused queued task acquired an attempt",
                )
                prefix = path.removeprefix("/").replace("/", ".")
                require(
                    not any(jobs.data.glob(prefix + ".*.pid")),
                    "Paused queued task actually started",
                )
                script = evidence.directory / path.removeprefix("/")
                require(
                    not any(script.parent.glob(script.name + ".job*")),
                    "Paused queued task generated a script",
                )
        value = status(evidence, service, "latest-status")
        require(
            value["status"] == "halted"
            and set(value["halt_causes"]) == set(causes)
            and value["last_checkpoint_at"] == saved_at,
            "Checkpoint fault halt or saved time changed",
        )
        require(
            service.checkpoint.read_bytes() == main
            and service.checkpoint.with_name(
                service.checkpoint.name + ".b"
            ).read_bytes()
            == backup,
            "Failed save damaged the checkpoint pair",
        )
        samples += 1
        if time.monotonic() - start >= seconds:
            break
        time.sleep(0.25)
    evidence.write_json(f"{label}.json", nodes)
    evidence.record(
        "checkpoint_failure_held",
        label=label,
        elapsed_s=time.monotonic() - start,
        samples=samples,
    )
    return nodes
