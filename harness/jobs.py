"""Gated real Shell jobs and Linux process evidence for scheduling scenarios."""

import os
import shlex
import threading
import time
from pathlib import Path

from takler.core import Flow
from takler.tasks.shell import ShellScriptTask

from .online import snapshot, write_definition
from .runtime import require, wait_for


def process_identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        return None
    return int(fields[19]) if fields[0] not in {"Z", "X"} else None


def peak(intervals, start, end):
    changes = []
    for item in intervals.values():
        require(item[start] < item[end], "Empty process observation interval")
        changes.extend(((item[start], 1), (item[end], -1)))
    count = maximum = 0
    for _, delta in sorted(changes):
        count += delta
        maximum = max(maximum, count)
    require(count == 0, "Unbalanced process intervals")
    return maximum


class Jobs:
    def __init__(self, evidence, service):
        self.evidence = evidence
        self.service = service
        self.data = evidence.directory / "data"
        self.data.mkdir()
        self.intervals = {}
        self.outputs = {}
        self.sequences = {}
        self.error = None
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.observe, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, kind, value, traceback):
        (self.data / "stop").touch()
        try:
            # Release only this run's jobs; the outer runner owns server shutdown.
            wait_for(
                "owned controlled jobs to exit",
                lambda: self.all_exited(),
                timeout=10,
                interval=0.02,
            )
        finally:
            self.done.set()
            self.thread.join(timeout=2)
            self.evidence.write_json(
                "process-intervals.json",
                {
                    "clock": "CLOCK_BOOTTIME",
                    "ticks_per_second": os.sysconf("SC_CLK_TCK"),
                    "intervals": self.intervals,
                    "observer_error": self.error,
                },
            )
        require(not self.thread.is_alive() and self.error is None, "Observer failed")

    def observe(self):
        ticks = os.sysconf("SC_CLK_TCK")
        try:
            while not self.done.is_set():
                for path in self.data.glob("*.pid"):
                    key = path.stem
                    item = self.intervals.get(key)
                    if item is not None and "exit_upper_ns" in item:
                        continue
                    pid = int(path.read_text())
                    before = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
                    identity = process_identity(pid)
                    after = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
                    if item is None:
                        require(identity is not None, "Job exited before observation")
                        self.intervals[key] = {
                            "pid": pid,
                            "start_ticks": identity,
                            "start_floor_ns": identity * 1_000_000_000 // ticks,
                            "first_seen_ns": after,
                            "last_alive_ns": before,
                        }
                    elif identity == item["start_ticks"]:
                        item["last_alive_ns"] = before
                    else:
                        item["exit_upper_ns"] = after
                self.done.wait(0.01)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"

    def all_exited(self):
        require(self.error is None, f"Process observer: {self.error}")
        return len(list(self.data.glob("*.pid"))) == len(self.intervals) and all(
            "exit_upper_ns" in item for item in self.intervals.values()
        )

    def flow(self, name):
        flow = Flow(name)
        flow.add_parameter(
            {
                "TAKLER_HOME": str(self.evidence.directory),
                "SCENARIO_DATA_SHELL": shlex.quote(str(self.data)),
                "TAKLER_SHELL_JOB_CMD": (
                    "{{ TAKLER_JOB | shquote }} 1> {{ TAKLER_JOBOUT | shquote }} 2>&1"
                ),
            }
        )
        return flow

    def task(self, parent, name, trigger=None, body=":", script=None):
        script = (
            script or Path(__file__).resolve().parents[1] / "assets/controlled.takler"
        )
        task = parent.add_task(ShellScriptTask(name, script_path=script))
        task.add_parameter("BODY", body)
        if trigger:
            task.add_trigger(trigger)
        return task

    def load(self, *flows, begin=True):
        for flow in flows:
            target = write_definition(self.evidence, flow, f"{flow.name}.json")
            self.service.client("load", str(target))
        if begin:
            for flow in flows:
                self.service.client("begin", flow.name)

    def view(self, label):
        nodes = snapshot(self.evidence, self.service, label)
        for path, node in nodes.items():
            groups = node["groups"]
            runtime = groups.get("runtime", {})
            attempt = runtime.get("attempt_id")
            reference = groups.get("artifacts", {}).get("jobout_ref")
            if attempt and reference:
                self.outputs[self.key(path, attempt)] = Path(reference["path"])
        return nodes

    def phase(self, label, conditions, *, timeout=40):
        def sample():
            require(self.error is None, f"Process observer: {self.error}")
            nodes = self.view("latest")
            for path, state in conditions.items():
                if state != "aborted" and nodes[path]["summary"]["status"] == "aborted":
                    reason = nodes[path]["groups"]["runtime"]["aborted_reason"]
                    require(False, f"Unexpected aborted task {path}: {reason}")
            if not all(
                nodes[path]["summary"]["status"] == state
                for path, state in conditions.items()
            ):
                return None
            for path, state in conditions.items():
                if state == "active":
                    runtime = nodes[path]["groups"]["runtime"]
                    key = self.key(path, runtime["attempt_id"])
                    if key not in self.intervals:
                        return None
                    require(
                        self.intervals[key]["pid"] == int(runtime["task_id"])
                        and runtime["try_no"] == 1
                        and runtime["started_at"],
                        "Task identity or child lifecycle differs from expectation",
                    )
            return nodes

        nodes = wait_for(label, sample, timeout=timeout)
        self.evidence.write_json(f"{label}.json", nodes)
        return nodes

    def hold(self, label, conditions, seconds=13):
        started = time.monotonic()
        count = 0
        while True:
            nodes = self.view("latest")
            require(
                all(
                    nodes[path]["summary"]["status"] == state
                    for path, state in conditions.items()
                ),
                f"State changed during {label}",
            )
            for path, state in conditions.items():
                if state == "queued":
                    runtime = nodes[path]["groups"]["runtime"]
                    require(
                        not runtime["attempt_id"], f"Queued task {path} was attempted"
                    )
                    prefix = path.removeprefix("/").replace("/", ".") + "."
                    require(
                        not any(self.data.glob(prefix + "*.pid")), "Queued job started"
                    )
                    script = self.evidence.directory / path.removeprefix("/")
                    require(
                        not any(script.parent.glob(script.name + ".job*")),
                        "Queued task generated a job script",
                    )
            count += 1
            elapsed = time.monotonic() - started
            if elapsed >= seconds:
                break
            time.sleep(min(0.25, seconds - elapsed))
        self.evidence.write_json(f"{label}.json", nodes)
        self.evidence.record(
            "condition_held", label=label, elapsed_s=elapsed, samples=count
        )
        return nodes

    @staticmethod
    def key(path, attempt):
        return path.removeprefix("/").replace("/", ".") + "." + attempt

    def action(self, nodes, path, action):
        runtime = nodes[path]["groups"]["runtime"]
        key = self.key(path, runtime["attempt_id"])
        require(key in self.intervals, "Action requires an observed real job")
        sequence = self.sequences.get(key, 0) + 1
        self.sequences[key] = sequence
        target = self.data / f"{key}.cmd{sequence}"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(action + "\n")
        temporary.replace(target)
        self.evidence.record(
            "job_action", node=path, attempt=runtime["attempt_id"], action=action
        )
        if action.startswith(("event:", "meter:")):
            wait_for(
                "child attribute acknowledgment",
                lambda: (self.data / f"{key}.ack{sequence}").exists(),
                timeout=10,
            )
        else:
            wait_for(
                "job process exit",
                lambda: "exit_upper_ns" in self.intervals[key],
                timeout=10,
                interval=0.02,
            )
        return key

    @staticmethod
    def usage(nodes, owner, holders):
        usage = nodes[owner]["groups"]["runtime"]["limit_usage"]["work"]
        require(
            usage["value"] == len(holders) and set(usage["node_paths"]) == set(holders),
            "Limit value or holders differ",
        )

    def finish(self, expected, capacity=None):
        wait_for("all observed jobs to exit", self.all_exited, timeout=10)
        require(
            len(self.intervals) == len(expected),
            "Unexpected extra or missing execution",
        )
        require(
            set(self.outputs) == set(expected),
            "Missing output reference for an attempt",
        )
        require(
            len(list(self.evidence.directory.rglob("*.job*"))) == len(expected),
            "Unexpected extra generated job script",
        )
        child_successes = {}
        for key, outcome in expected.items():
            lines = self.outputs[key].read_text().splitlines()
            path, attempt = key.rsplit(".", 1)
            path = "/" + path.replace(".", "/")
            marker = {
                "success": f"SCN05_OK {path} {attempt}",
                "failure": f"SCN05_FAIL {path} exit=7",
                "quiet": f"SCN05_QUIET {path}",
            }[outcome]
            require(lines.count(marker) == 1, "Missing or repeated business marker")
            reports = self.sequences[key] + (0 if outcome == "quiet" else 1)
            require(
                lines.count("received: success") == reports,
                f"Expected {reports} successful child reports for {key}",
            )
            child_successes[key] = reports
        lower = peak(self.intervals, "first_seen_ns", "last_alive_ns")
        upper = peak(self.intervals, "start_floor_ns", "exit_upper_ns")
        if capacity is not None:
            require(
                lower == upper == capacity,
                "Real process concurrency differs from capacity",
            )
        self.evidence.write_json(
            "job-outputs.json", {key: str(path) for key, path in self.outputs.items()}
        )
        return {
            "jobs": len(expected),
            "outcomes": expected,
            "child_successes": child_successes,
            "process_peak_lower": lower,
            "process_peak_upper": upper,
        }
