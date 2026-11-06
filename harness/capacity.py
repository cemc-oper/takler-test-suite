"""Frozen capacity configuration and external Linux resource observation."""

import ctypes
import hashlib
import json
import os
import re
import select
import struct
import threading
import time
import tomllib
from pathlib import Path

from .runtime import EnvironmentBlocked, require, timestamp


def prepare(evidence, service):
    if service.client_kind != "python" or service.transport != "grpc":
        raise EnvironmentBlocked("First capacity baseline requires Python/gRPC")
    source = Path(
        os.environ.get(
            "SCENARIO_CAPACITY_CONFIG",
            Path(__file__).resolve().parents[1] / "assets/capacity.toml",
        )
    ).resolve()
    config = tomllib.loads(source.read_text())
    require(
        config["schema_version"] == 1
        and config["mixed"]["concurrency"] <= 8
        and config["sample_seconds"] >= 0.5,
        "Invalid capacity configuration",
    )
    require(
        all(
            config["large"][k] > 0
            for k in ("tasks_per_flow", "tasks_per_group", "samples")
        ),
        "Invalid large-tree dimensions",
    )
    require(
        config["endurance"]["duration_seconds"] > 0
        and config["mixed"]["duration_seconds"] > 0,
        "Invalid measurement duration",
    )
    service.capacity = config
    service.checkpoint_interval = config["checkpoint_seconds"]
    service.stop_timeout = config["stop_timeout_seconds"]
    evidence.write_json(
        "capacity-config.json",
        {
            "frozen_at": timestamp(),
            "source": str(source),
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "config": config,
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "meminfo": Path("/proc/meminfo").read_text(),
            "byte_definition": "gRPC QueryDocument JSON payload, excluding HTTP/2 and protobuf framing",
            "checkpoint_overlap": "externally observed temp-create to primary-rename envelope; uncertainty bounded by inotify read interval, not internal capture duration",
        },
    )


def descendants(root, *, extra_roots=()):
    from .jobs import process_identity

    pending, result = [(root, None), *extra_roots], []
    seen = set()
    while pending:
        pid, expected_identity = pending.pop()
        base = Path(f"/proc/{pid}")
        for attempt in range(4):
            try:
                fields = (base / "stat").read_text().rsplit(")", 1)[1].split()
                if fields[0] in {"Z", "X"}:
                    break
                identity = int(fields[19])
                if expected_identity is not None and expected_identity != identity:
                    break
                if (pid, identity) in seen:
                    break
                status = dict(
                    line.split(":", 1)
                    for line in (base / "status").read_text().splitlines()
                )
                row = {
                    "pid": pid,
                    "start_ticks": identity,
                    "rss_bytes": int(status.get("VmRSS", "0 kB").split()[0]) * 1024,
                    "fds": len(list((base / "fd").iterdir())),
                    "threads": int(status["Threads"]),
                    "cpu_ticks": int(fields[11]) + int(fields[12]),
                }
                children = [
                    int(p) for p in (base / f"task/{pid}/children").read_text().split()
                ]
                if process_identity(pid) == identity:
                    seen.add((pid, identity))
                    result.append(row)
                    pending.extend((child, None) for child in children)
                break
            except (FileNotFoundError, ProcessLookupError):
                break
            except (PermissionError, KeyError):
                # Linux can deny /proc/PID/fd during exit/exec. Do not discard a
                # persistently unreadable live process or count a reused PID.
                if process_identity(pid) is None:
                    break
                if attempt == 3:
                    raise
                time.sleep(0.002)
    return result


class Resources:
    def __init__(self, evidence, service, *, job_roots=None):
        self.evidence, self.service, self.config = evidence, service, service.capacity
        self.job_roots = job_roots
        self.phase, self.error, self.samples, self.windows = "setup", None, [], []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.observe, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.done.set()
        self.thread.join(timeout=5)
        self.evidence.write_json(
            "resource-summary.json",
            {
                "samples": len(self.samples),
                "rss_peak_bytes": max(
                    (s["rss_bytes"] for s in self.samples), default=0
                ),
                "fd_peak": max((s["fds"] for s in self.samples), default=0),
                "owned_process_peak": max(
                    (s["processes"] for s in self.samples), default=0
                ),
                "error": self.error,
                "scope": "runner and its descendants; for detached jobs also include each observed PID/start time and its descendants; summed RSS may double-count shared pages; CPU ticks are per process identity",
                "checkpoint_windows": self.windows,
            },
        )
        if not exc[0]:
            self.check()

    def check(self):
        require(self.error is None, f"Capacity resource guard: {self.error}")

    def observe(self):
        descriptor = None
        try:
            library = ctypes.CDLL("libc.so.6", use_errno=True)
            descriptor = library.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
            require(descriptor >= 0, "Cannot open inotify")
            require(
                library.inotify_add_watch(
                    descriptor,
                    os.fsencode(self.evidence.directory),
                    0x100 | 0x80 | 0x4000,
                )
                >= 0,
                "Cannot watch private checkpoint directory",
            )
            pending = None
            previous = time.monotonic_ns()
            sample_at = disk_at = 0
            disk_bytes = 0
            with (
                (self.evidence.directory / "resources.jsonl").open("w") as output,
                (self.evidence.directory / "checkpoint-events.jsonl").open(
                    "w"
                ) as events,
            ):
                while not self.done.is_set():
                    select.select([descriptor], [], [], 0.1)
                    now = time.monotonic_ns()
                    try:
                        raw = os.read(descriptor, 65536)
                    except BlockingIOError:
                        raw = b""
                    offset = 0
                    while offset < len(raw):
                        _, mask, _, length = struct.unpack_from("iIII", raw, offset)
                        name = os.fsdecode(
                            raw[offset + 16 : offset + 16 + length].split(b"\0")[0]
                        )
                        offset += 16 + length
                        require(
                            not mask & 0x4000,
                            "inotify queue overflow invalidates measurement",
                        )
                        if (
                            name.startswith(self.service.checkpoint.name + ".tmp.")
                            and mask & 0x100
                        ):
                            pending = {
                                "start_lower_ns": previous,
                                "start_upper_ns": now,
                            }
                        if name == self.service.checkpoint.name and mask & 0x80:
                            value = {
                                **(
                                    pending
                                    or {
                                        "start_lower_ns": previous,
                                        "start_upper_ns": now,
                                    }
                                ),
                                "end_lower_ns": previous,
                                "end_upper_ns": now,
                                "observed_at": timestamp(),
                                "bytes": self.service.checkpoint.stat().st_size,
                            }
                            self.windows.append(value)
                            events.write(json.dumps(value) + "\n")
                            events.flush()
                            pending = None
                    previous = now
                    if now < sample_at:
                        continue
                    sample_at = now + int(self.config["sample_seconds"] * 1e9)
                    job_roots = self.job_roots() if self.job_roots else ()
                    processes = descendants(os.getpid(), extra_roots=job_roots)
                    if now >= disk_at:
                        disk_bytes = 0
                        for path in self.evidence.directory.rglob("*"):
                            try:
                                if path.is_file():
                                    disk_bytes += path.stat().st_size
                            except FileNotFoundError:
                                pass  # A completed atomic write removed its temporary file.
                        disk_at = now + 30_000_000_000
                    memory = Path("/proc/meminfo").read_text()
                    available = (
                        int(re.search(r"MemAvailable:\s+(\d+)", memory)[1]) * 1024
                    )
                    sample = {
                        "at": timestamp(),
                        "mono_ns": now,
                        "phase": self.phase,
                        "rss_bytes": sum(p["rss_bytes"] for p in processes),
                        "fds": sum(p["fds"] for p in processes),
                        "threads": sum(p["threads"] for p in processes),
                        "processes": len(processes),
                        "artifact_bytes": disk_bytes,
                        "available_bytes": available,
                        "identities": processes,
                    }
                    if self.job_roots:
                        sample["job_roots"] = job_roots
                    self.samples.append(sample)
                    output.write(json.dumps(sample) + "\n")
                    output.flush()
                    require(
                        sample["rss_bytes"] <= self.config["max_rss_bytes"],
                        "Owned summed RSS exceeded frozen limit",
                    )
                    require(
                        disk_bytes <= self.config["max_artifact_bytes"],
                        "Evidence size exceeded frozen limit",
                    )
                    require(
                        available >= self.config["min_available_bytes"],
                        "Available memory below frozen stop condition",
                    )
                    require(
                        len(processes) <= self.config["max_owned_processes"],
                        "Owned process count exceeded frozen limit",
                    )
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
        finally:
            if descriptor is not None and descriptor >= 0:
                os.close(descriptor)


def wait_until(target, resources):
    while time.monotonic() < target:
        resources.check()
        time.sleep(min(0.2, max(0, target - time.monotonic())))
