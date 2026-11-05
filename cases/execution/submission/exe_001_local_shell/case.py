import json
import stat
import subprocess
from importlib.metadata import version
from pathlib import Path

import takler
from takler.core import Flow, NodeStatus
from takler.serialization import export_definition
from takler.tasks.shell import ShellScriptTask

from harness.runtime import RunError, wait_for

FLOW = "/test"
TASK = "/test/t1"


def require(condition, message):
    if not condition:
        raise RunError(message)


def run(evidence, service):
    home = evidence.directory
    source = Path(takler.__file__).resolve()
    evidence.report["takler"] = {
        "package_version": version("takler"),
        "module": str(source),
    }
    if source.parent.parent.name == "src":
        checkout = source.parent.parent.parent
        revision = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            timeout=3,
        )
        if revision.returncode == 0:
            evidence.report["takler"]["revision"] = revision.stdout.strip()

    flow = Flow("test")
    flow.add_parameter("TAKLER_HOME", str(home))
    flow.add_parameter(
        "TAKLER_SHELL_JOB_CMD",
        "{{ TAKLER_JOB | shquote }} 1> {{ TAKLER_JOBOUT | shquote }} 2>&1",
    )
    task = flow.add_task(ShellScriptTask("t1"))
    task.add_parameter("TAKLER_SCRIPT", str(Path(__file__).with_name("task.takler")))
    definition = export_definition(flow)
    require(definition.schema_version == 1, "Unexpected definition version")
    target = home / "definition.json"
    target.write_text(definition.model_dump_json(indent=2) + "\n")

    def snapshot():
        return json.loads(
            service.client(
                "snapshot", "--scope", FLOW, "--fields", "summary,runtime,artifacts"
            ).stdout
        )

    status = json.loads(service.client("server-status").stdout)
    require(status["status"] == "running", "Fresh service must be running")
    service.client("load", str(target))
    loaded = snapshot()
    nodes = loaded["nodes_by_path"]
    require(set(nodes) == {FLOW, TASK}, "Loaded tree differs from definition")
    require(
        all(node["summary"]["status"] == "unknown" for node in nodes.values())
        and not nodes[FLOW]["groups"]["runtime"]["begun"],
        "Loaded flow must be idle before begin",
    )
    evidence.write_json("loaded.json", loaded)
    service.client("begin", "test")

    def complete():
        view = snapshot()
        nodes = view["nodes_by_path"]
        require(nodes[TASK]["summary"]["status"] != "aborted", "Shell task aborted")
        return (
            view
            if all(node["summary"]["status"] == "complete" for node in nodes.values())
            else None
        )

    final = wait_for("flow and task to complete", complete, timeout=30)
    evidence.write_json("complete.json", final)
    groups = final["nodes_by_path"][TASK]["groups"]
    runtime = groups["runtime"]
    attempt, pid = runtime["attempt_id"], runtime["task_id"]
    require(attempt and pid and runtime["try_no"] == 1, "Expected one real attempt")
    require(
        runtime["started_at"] and runtime["ended_at"],
        "Missing child lifecycle timestamps",
    )
    job = Path(groups["artifacts"]["job_ref"]["path"])
    output = Path(groups["artifacts"]["jobout_ref"]["path"])
    require(
        (job, output)
        == (home / "test" / f"t1.job{attempt}", home / "test" / f"t1.{attempt}")
        and set((home / "test").glob("t1.*")) == {job, output},
        "Expected exactly one job script and output for the recorded attempt",
    )
    lines = output.read_text().splitlines()
    require(
        lines.count("SCN01_OK") == 1 and lines.count("received: success") == 2,
        "Expected one business marker and two successful child reports",
    )

    def job_exited():
        try:
            state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
        except FileNotFoundError:
            return True
        return state == "Z"

    wait_for("the task process to exit", job_exited, timeout=5)
    service.stop()
    saved = json.loads(service.checkpoint.read_bytes())
    stored = saved["runtime"]["nodes"]
    require(
        saved["format_version"] == 3
        and stat.S_IMODE(service.checkpoint.stat().st_mode) == 0o600
        and stored[FLOW]["begun"]
        and all(
            NodeStatus(stored[path]["state"]["status"]) == NodeStatus.complete
            for path in (FLOW, TASK)
        )
        and stored[TASK]["attempt_id"] == attempt,
        "Final checkpoint does not match completed runtime",
    )
    summary = {
        "attempt": attempt,
        "task_pid": pid,
        "try_no": 1,
        "child_successes": 2,
        "marker": "SCN01_OK",
        "output": str(output.relative_to(home)),
        "task_exited": True,
        "checkpoint_version": 3,
    }
    evidence.report["checks"] = summary
    evidence.record("case_checks", **summary)
