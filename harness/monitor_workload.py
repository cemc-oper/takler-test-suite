"""Small-tree monitor workload, real child receipts, and checkpoint observations."""

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import threading
import time
import tomllib

import takler
from takler.serialization.checkpoint import restore_checkpoint

from .capacity import Resources
from .capacity_load import Commands
from .jobs import Jobs
from .monitor_tree import add_backdrop, tree_counts
from .online import write_definition
from .recovery import status
from .runtime import EnvironmentBlocked, require, timestamp, wait_for


ASSETS = Path(__file__).resolve().parents[1] / "assets"
SCOPE = "/monitor"
MARKER = "/monitor/marker"


def host_sample(directory):
    cpu = Path("/proc/stat").read_text().splitlines()[0].split()
    memory = {
        line.split(":")[0]: int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if line.startswith(("MemAvailable:", "SwapTotal:", "SwapFree:"))
    }
    swap = {
        name: int(value)
        for name, value in map(str.split, Path("/proc/vmstat").read_text().splitlines())
        if name in {"pswpin", "pswpout"}
    }
    device_id = directory.stat().st_dev
    disk = None
    for line in Path("/proc/diskstats").read_text().splitlines():
        cols = line.split()
        if (int(cols[0]), int(cols[1])) == (os.major(device_id), os.minor(device_id)):
            disk = {
                "device": cols[2],
                "read_bytes": int(cols[5]) * 512,
                "write_bytes": int(cols[9]) * 512,
                "busy_ms": int(cols[12]),
            }
            break
    return {
        "mono_ns": time.monotonic_ns(),
        "cpu_ticks": list(map(int, cpu[1:9])),
        "available_bytes": memory["MemAvailable"],
        "swap_used_bytes": memory["SwapTotal"] - memory["SwapFree"],
        "swap_pages": swap,
        "disk_free_bytes": shutil.disk_usage(directory).free,
        "disk_io": disk,
    }


def check_host(sample, initial_swap, config):
    require(
        sample["available_bytes"] >= config["min_available_bytes"],
        "Host memory below stop condition",
    )
    require(
        sample["swap_used_bytes"] - initial_swap <= config["max_swap_growth_bytes"],
        "Host swap growth exceeded stop condition",
    )
    require(
        sample["disk_free_bytes"] >= config["min_disk_free_bytes"],
        "Private run filesystem below free-space stop condition",
    )


def source_version(root, inputs):
    """Fingerprint the executable sources including changes absent from HEAD."""
    manifest = {}
    for name in inputs:
        target = root / name
        for path in sorted(target.rglob("*")) if target.is_dir() else [target]:
            if path.is_file() and "__pycache__" not in path.parts:
                manifest[str(path.relative_to(root))] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
    head = None
    if (root / ".git").exists() and shutil.which("git"):
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            head = result.stdout.strip()
    return {
        "git_head": head,
        "source_sha256": hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode()
        ).hexdigest(),
        "inputs": inputs,
    }


def prepare(evidence, service):
    if service.client_kind != "python" or service.transport != "grpc":
        raise EnvironmentBlocked("Monitor scenario requires Python/gRPC")
    path = Path(
        os.environ.get("SCENARIO_MONITOR_CONFIG", ASSETS / "monitoring.toml")
    ).resolve()
    raw = path.read_bytes()
    cfg = tomllib.loads(raw.decode())
    require(
        cfg["schema_version"] == 1
        and cfg["tree_kind"] in {"synthetic_flat", "operations_proxy"},
        "Unsupported monitor tree schema",
    )
    for key in (
        "windows",
        "rpc_timeout_seconds",
        "job_concurrency",
        "checkpoint_seconds",
        "min_periodic_publications",
        "stop_timeout_seconds",
        "min_available_bytes",
        "min_disk_free_bytes",
        "max_swap_growth_bytes",
        "max_rss_bytes",
        "max_artifact_bytes",
        "max_owned_processes",
    ):
        require(
            type(cfg.get(key)) is int and cfg[key] > 0,
            f"Expected positive integer for {key}",
        )
    require(cfg["windows"] >= 2, "Monitor scenario needs a TUI and a second reader")
    if "load_timeout_seconds" in cfg:
        require(
            type(cfg["load_timeout_seconds"]) is int
            and cfg["load_timeout_seconds"] > 0,
            "Expected positive integer for load_timeout_seconds",
        )
    counts = tree_counts(cfg)
    require(cfg["job_concurrency"] < counts["tasks"], "Reserve a distinct marker task")
    # The server falls back to its default if given a shorter interval.
    require(
        cfg["checkpoint_seconds"] >= 10,
        "Checkpoint interval must be at least 10 seconds",
    )
    require(
        type(cfg["sample_seconds"]) in (int, float) and cfg["sample_seconds"] >= 0.5,
        "Invalid resource sample interval",
    )
    require(
        cfg["min_available_bytes"] >= 4 << 30
        and cfg["min_disk_free_bytes"] >= 20 << 30
        and cfg["max_swap_growth_bytes"] <= 1 << 30
        and cfg["max_rss_bytes"] <= 8 << 30,
        "Requested thresholds relax the monitor resource budget",
    )
    baseline = host_sample(evidence.directory)
    package = Path(takler.__file__).resolve().parent
    product_root = (
        package.parent.parent if package.parent.name == "src" else package.parent
    )
    evidence.write_json(
        "monitor-config.json",
        {
            "source": str(path),
            "source_sha256": hashlib.sha256(raw).hexdigest(),
            "frozen_at": timestamp(),
            "config": cfg,
            "host_preflight": baseline,
            "tree": {
                **counts,
                "seed": None,
                "generation": "deterministic insertion order",
                "scope": SCOPE,
                "marker": MARKER,
            },
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "python": sys.executable,
            "versions": {
                "product": {
                    "installed_version": getattr(takler, "__version__", None),
                    **source_version(
                        product_root, [str(package.relative_to(product_root))]
                    ),
                },
                "suite": source_version(
                    ASSETS.parent,
                    [
                        "run.py",
                        "catalog.toml",
                        "harness",
                        "cases/capacity/mixed_load/cap_004_monitors",
                        "assets/cleanup_service_helpers.py",
                        "assets/controlled.takler",
                        "assets/monitor_child.py",
                    ],
                ),
            },
            "child_time": "CLOCK_MONOTONIC: CLI launch to exit, and separately the unary gRPC call inside that CLI",
            "checkpoint_time": "possible checkpoint file I/O overlap, temp create to primary rename; bounds include inotify sampling uncertainty",
        },
    )
    check_host(baseline, baseline["swap_used_bytes"], cfg)
    service.monitor_baseline_swap = baseline["swap_used_bytes"]
    service.capacity = service.monitor_config = cfg
    service.checkpoint_interval = cfg["checkpoint_seconds"]
    service.stop_timeout = cfg["stop_timeout_seconds"]


class HostObserver:
    """Sample host contention during blocked RPCs, alongside the owned-process sampler."""

    def __init__(self, evidence, service):
        self.directory = evidence.directory
        self.config = service.monitor_config
        self.initial_swap = service.monitor_baseline_swap
        self.done = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.observe, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def observe(self):
        try:
            with (self.directory / "host-resources.jsonl").open("w") as stream:
                while not self.done.is_set():
                    value = host_sample(self.directory)
                    stream.write(json.dumps(value) + "\n")
                    stream.flush()
                    check_host(value, self.initial_swap, self.config)
                    self.done.wait(self.config["sample_seconds"])
        except Exception as exc:
            self.error = exc

    def check(self):
        if self.error is not None:
            raise self.error

    def __exit__(self, kind, value, traceback):
        self.done.set()
        self.thread.join()
        if kind is None:
            self.check()


def overlap_counts(rows, windows):
    timed = [
        row
        for row in rows
        if row.get("start_ns") is not None and row.get("end_ns") is not None
    ]
    return {
        "timed": len(timed),
        "possible_checkpoint_io_overlap": sum(
            any(
                row["start_ns"] <= w["end_upper_ns"]
                and row["end_ns"] >= w["start_lower_ns"]
                for w in windows
            )
            for row in timed
        ),
    }


class MonitorWorkload:
    def __init__(self, evidence, service):
        self.evidence, self.service = evidence, service
        self.cfg = service.monitor_config
        self.paths = [f"{SCOPE}/worker{i}" for i in range(self.cfg["job_concurrency"])]
        self.meter_values = []
        self.stack = ExitStack()

    def __enter__(self):
        with ExitStack() as setup:
            self.jobs = Jobs(self.evidence, self.service, scope=SCOPE, depth=1)
            detached = bool(
                self.cfg.get("poll_cycles")
                or self.cfg.get("checkpoint_sync_waves")
                or self.cfg.get("slow_reader")
            )
            self.resources = setup.enter_context(
                Resources(
                    self.evidence,
                    self.service,
                    job_roots=self.jobs.live_processes if detached else None,
                )
            )
            self.host = setup.enter_context(HostObserver(self.evidence, self.service))
            setup.enter_context(self.jobs)
            wait_for("monitor process sampler", self._sample_ready, timeout=5)
            self.resources.phase = "load"
            flow = self.jobs.flow(SCOPE[1:], detached=detached)
            self.receipts = self.evidence.directory / "child-receipts.jsonl"
            wrapper = shlex.join(
                [
                    sys.executable,
                    "-B",
                    str(ASSETS / "monitor_child.py"),
                    str(self.receipts),
                    str(self.service.client_bin),
                ]
            )
            flow.add_parameter("SCENARIO_CHILD", wrapper)
            flow.add_limit("work", len(self.paths))
            for path in self.paths:
                task = self.jobs.task(flow, path.rsplit("/", 1)[1])
                task.add_meter("step", min_value=0, max_value=100)
                task.add_in_limit("work")
            flow.add_event("enable_background")
            marker = flow.add_task("marker")
            marker.add_parameter("LABEL", "marker")
            marker.add_trigger("/monitor:enable_background == set")
            add_backdrop(flow, self.cfg)
            self.load_flow(flow)
            self.loaded_ns = time.monotonic_ns()
            self.active = self.jobs.phase(
                "monitor-active", {p: "active" for p in self.paths}
            )
            self.jobs.usage(self.active, SCOPE, self.paths)
            self.resources.phase = "monitor"
            self.stack = setup.pop_all()
        return self

    def __exit__(self, kind, value, traceback):
        return self.stack.__exit__(kind, value, traceback)

    def _sample_ready(self):
        self.check()
        return bool(self.resources.samples)

    def check(self):
        self.host.check()
        self.resources.check()

    def load_flow(self, flow):
        target = write_definition(self.evidence, flow, f"{flow.name}.json", indent=None)
        with ExitStack() as setup:
            call = self.service.client
            if "load_timeout_seconds" in self.cfg:
                writer = setup.enter_context(
                    Commands(
                        self.service, secret_file=self.service.env["TAKLER_SECRET_FILE"]
                    )
                )
                call = writer.call
            for command, argument in (("load", str(target)), ("begin", flow.name)):
                self.check()
                measurement = {
                    "command": command,
                    "start_ns": time.monotonic_ns(),
                    "success": False,
                }
                if command == "load":
                    measurement["definition_bytes"] = target.stat().st_size
                try:
                    call(command, argument)
                    measurement["success"] = True
                finally:
                    self.evidence.record(
                        "monitor_setup_command",
                        **measurement,
                        end_ns=time.monotonic_ns(),
                    )

    def set_marker(self, suspended):
        self.check()
        command = "suspend" if suspended else "resume"
        start = time.monotonic_ns()
        self.service.client(command, MARKER)
        end = time.monotonic_ns()
        self.evidence.record(
            "marker_write", command=command, path=MARKER, start_ns=start, end_ns=end
        )
        return end

    def report_meter(self, value=100):
        self.check()
        require(type(value) is int and 0 <= value <= 100, "Meter must be 0..100")
        for path in self.paths:
            self.jobs.action(self.active, path, f"meter:{value}")
        latest = self.jobs.phase("monitor-meter", {p: "active" for p in self.paths})
        for path in self.paths:
            actual = latest[path]["groups"]["runtime"]
            require(
                actual["attempt_id"]
                == self.active[path]["groups"]["runtime"]["attempt_id"]
                and actual["meter_values"]["step"] == value,
                "Actual child meter or attempt changed",
            )
        self.meter_values.append(value)

    def finish(self, rows):
        self.check()
        require(self.meter_values, "Missing real child meter report")
        expected = {
            self.jobs.action(self.active, p, "success"): "success" for p in self.paths
        }
        final = self.jobs.phase("monitor-complete", {p: "complete" for p in self.paths})
        self.jobs.usage(final, SCOPE, [])
        checks = self.jobs.finish(expected, capacity=len(self.paths))
        receipts = [json.loads(line) for line in self.receipts.read_text().splitlines()]
        for path in self.paths:
            records = [r for r in receipts if r["path"] == path]
            require(
                [r["command"] for r in records]
                == ["init"] + ["meter"] * len(self.meter_values) + ["complete"]
                and [r["meter_value"] for r in records if r["command"] == "meter"]
                == self.meter_values
                and all(
                    r["returncode"] == 0
                    and r["end_ns"] >= r["start_ns"]
                    and r["attempt"]
                    == self.active[path]["groups"]["runtime"]["attempt_id"]
                    for r in records
                ),
                "Real CLI receipts differ from the validated attempt",
            )
        child_rpcs = []
        for receipt in receipts:
            calls = receipt["rpcs"]
            require(len(calls) == 1, "Expected one gRPC attempt for each child report")
            rpc = calls[0]
            require(
                rpc["method"] == "RunCommand" + receipt["command"].capitalize()
                and rpc["code"] == "OK"
                and receipt["start_ns"]
                <= rpc["start_ns"]
                <= rpc["end_ns"]
                <= receipt["end_ns"],
                "Child gRPC evidence differs from its CLI receipt",
            )
            child_rpcs.append(rpc)
        last_complete_ns = max(
            r["end_ns"] for r in receipts if r["command"] == "complete"
        )

        def observed():
            self.check()
            publications = [
                w
                for w in self.resources.windows
                if w["end_lower_ns"] >= self.loaded_ns and w["bytes"] > 0
            ]
            if (
                len(publications) >= self.cfg["min_periodic_publications"]
                and publications[-1]["start_lower_ns"] >= last_complete_ns
            ):
                return publications

        publications = wait_for(
            "periodic checkpoint after the last real COMPLETE",
            observed,
            timeout=self.cfg["checkpoint_seconds"]
            * (2 + self.cfg["min_periodic_publications"])
            + 5,
        )
        raw = self.service.checkpoint.read_bytes()
        saved = restore_checkpoint(raw)
        for path in self.paths:
            node = saved.find_node(path)
            require(
                node is not None
                and node.state.node_status.name == "complete"
                and node.attempt_id
                == self.active[path]["groups"]["runtime"]["attempt_id"]
                and node.find_meter("step").value == self.meter_values[-1],
                "Periodic save lacks completed attempt or final meter value",
            )
        require(
            saved.find_node(MARKER).is_suspended()
            == final[MARKER]["summary"]["suspended"],
            "Periodic save lacks final visible monitor marker",
        )
        state = status(self.evidence, self.service, "monitor-finish")
        require(
            state["status"] == "running"
            and not state["halt_causes"]
            and state["last_checkpoint_at"],
            "Service lacks a successful live checkpoint",
        )
        measurements = {
            "windows": overlap_counts(
                [r for r in rows if r["action"] in {"open", "refresh", "reconnect"}],
                publications,
            ),
            "fault_windows": overlap_counts(
                [r for r in rows if r["action"] == "fault"], publications
            ),
            "child_cli": overlap_counts(receipts, publications),
            "child_rpc": overlap_counts(child_rpcs, publications),
            "periodic_publications": len(publications),
            "verified_checkpoint": {
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
                "completed_attempts": len(self.paths),
                "last_meter": self.meter_values[-1],
                "marker_suspended": saved.find_node(MARKER).is_suspended(),
            },
            "server_pid": self.service.process.pid,
            "runner_pid": os.getpid(),
            "scope": "host-resources.jsonl contains WSL CPU, swap and filesystem; resources.jsonl contains owned identities, RSS, FD, threads, CPU ticks. In-process observers share their runner's resources.",
        }
        self.evidence.write_json("monitor-measurements.json", measurements)
        return {"workload": checks, "observations": measurements}
