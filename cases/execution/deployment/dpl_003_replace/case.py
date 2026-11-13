import re
from pathlib import Path

from takler.core import Flow
from takler.tasks.shell import ShellScriptTask

from takler_test_suite.harness.online import rejected, snapshot, write_definition
from takler_test_suite.harness.runtime import require, wait_for

SCOPE = "/update_demo"
TASK = f"{SCOPE}/work"


def process_ticks(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        return None
    return fields[19] if fields[0] not in ("Z", "X") else None


def run(evidence, service):
    data = evidence.directory / "data"
    data.mkdir()
    source = Path(__file__).with_name("task.takler")

    def definition(version):
        flow = Flow("update_demo")
        flow.add_parameter(
            {
                "TAKLER_HOME": str(evidence.directory),
                "SCENARIO_DATA": str(data),
                "VERSION": str(version),
                "TAKLER_SHELL_JOB_CMD": (
                    "{{ TAKLER_JOB | shquote }} 1> {{ TAKLER_JOBOUT | shquote }} 2>&1"
                ),
            }
        )
        flow.add_limit("slots", 1)
        task = flow.add_task(ShellScriptTask("work", script_path=source))
        task.add_in_limit("slots")
        return write_definition(evidence, flow, f"v{version}.json")

    versions = [definition(version) for version in (1, 2, 3)]

    def view(label):
        return snapshot(evidence, service, label, SCOPE)

    def generation(nodes):
        return nodes[SCOPE]["summary"]["definition_generation"]

    def phase(label, state):
        def sample():
            nodes = view("latest")
            status = nodes[TASK]["summary"]["status"]
            require(status != "aborted", "Controlled Shell task aborted")
            return nodes if status == state else None

        nodes = wait_for(label, sample, timeout=30)
        evidence.write_json(f"{label}.json", nodes)
        return nodes

    def verify_usage(nodes, value):
        usage = nodes[SCOPE]["groups"]["runtime"]["limit_usage"]["slots"]
        require(
            usage["value"] == value
            and set(usage["node_paths"]) == ({TASK} if value else set()),
            "Unexpected limit occupancy",
        )

    running = {}
    executions = []
    try:
        service.client("load", str(versions[0]))
        original = view("original")
        service.client("suspend", SCOPE)
        response = service.client("replace", SCOPE, str(versions[1]))
        require("checkpoint pending" in response.stdout, "Missing replacement response")
        idle = view("idle-replaced")
        require(
            generation(idle) != generation(original)
            and idle[SCOPE]["summary"]["suspended"] is True
            and idle[SCOPE]["groups"]["runtime"]["begun"] is True
            and idle[TASK]["summary"]["status"] == "queued",
            "Idle replacement did not create a suspended, begun new generation",
        )
        service.client("resume", SCOPE)
        active = phase("first-active", "active")
        fields = active[TASK]["groups"]["runtime"]
        old_attempt, old_pid = fields["attempt_id"], fields["task_id"]
        running[old_pid] = process_ticks(old_pid)
        require(
            running[old_pid] is not None and fields["try_no"] == 1,
            "Missing real first attempt",
        )
        verify_usage(active, 1)
        rejected_active = service.client(
            "replace", SCOPE, str(versions[2]), allow_failure=True
        )
        active_result = rejected(rejected_active, "flow_state")
        after = view("after-rejected-replace")
        require(
            generation(after) == generation(active)
            and after[TASK]["groups"]["runtime"] == fields,
            "Rejected replacement changed the active attempt",
        )
        verify_usage(after, 1)
        job = Path(active[TASK]["groups"]["artifacts"]["job_ref"]["path"])
        match = re.search(r'^export TAKLER_PASS="([^"]+)"$', job.read_text(), re.M)
        require(match is not None, "Cannot find the first attempt's test credential")
        old_password = match[1]
        (data / "release-2").touch()
        completed = phase("first-complete", "complete")
        verify_usage(completed, 0)
        wait_for(
            "first job exit",
            lambda: process_ticks(old_pid) != running[old_pid],
            timeout=5,
        )
        executions.append((2, completed))

        service.client("suspend", SCOPE)
        service.client("replace", SCOPE, str(versions[2]))
        new_tree = view("second-idle-replaced")
        require(
            generation(new_tree) != generation(active)
            and new_tree[SCOPE]["summary"]["suspended"] is True
            and new_tree[TASK]["groups"]["runtime"]["attempt_id"] is None,
            "Second replacement kept the old generation or identity",
        )
        service.client("resume", SCOPE)
        new_active = phase("second-active", "active")
        new_runtime = new_active[TASK]["groups"]["runtime"]
        new_pid = new_runtime["task_id"]
        running[new_pid] = process_ticks(new_pid)
        require(
            new_runtime["attempt_id"] != old_attempt
            and new_runtime["try_no"] == 1
            and running[new_pid] is not None,
            "New execution did not receive a fresh identity",
        )
        verify_usage(new_active, 1)
        stale = service.client(
            "complete",
            "--node-path",
            TASK,
            "--attempt-id",
            old_attempt,
            env={**service.env, "TAKLER_PASS": old_password},
            allow_failure=True,
        )
        stale_result = rejected(stale, "attempt_mismatch")
        unchanged = view("after-stale-complete")
        require(
            unchanged[TASK]["groups"]["runtime"] == new_runtime
            and generation(unchanged) == generation(new_active),
            "Stale child report changed the new instance",
        )
        verify_usage(unchanged, 1)
        (data / "release-3").touch()
        final = phase("second-complete", "complete")
        verify_usage(final, 0)
        wait_for(
            "second job exit",
            lambda: process_ticks(new_pid) != running[new_pid],
            timeout=5,
        )
        executions.append((3, final))

        attempts = []
        for version, nodes in executions:
            task = nodes[TASK]["groups"]
            runtime = task["runtime"]
            lines = (
                Path(task["artifacts"]["jobout_ref"]["path"]).read_text().splitlines()
            )
            require(
                lines.count("received: success") == 2
                and lines.count(f"DPL003_OK {version}") == 1,
                "Expected two successful child reports and one business marker",
            )
            attempts.append(runtime["attempt_id"])
        evidence.record_checks(
            transport=service.transport,
            idle_replacements=2,
            generations=[generation(original), generation(active), generation(final)],
            active_replacement=active_result,
            stale_child=stale_result,
            attempts=attempts,
            try_numbers=[1, 1],
            child_successes=[2, 2],
            limit_final=0,
        )
    finally:
        (data / "stop").touch()
        for pid, ticks in running.items():
            wait_for(
                "owned Shell job cleanup",
                lambda: process_ticks(pid) != ticks,
                timeout=5,
            )
