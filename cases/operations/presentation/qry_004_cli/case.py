from harness.jobs import Jobs
from harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("showcase")
        flow.add_limit("work", 3)
        for name in ("running", "failed", "done"):
            jobs.task(flow, name).add_in_limit("work")
        jobs.task(
            flow,
            "blocked",
            "/showcase/running == complete and /showcase/failed == aborted",
        ).add_in_limit("work")
        jobs.load(flow)
        running, failed, done, blocked = [
            f"/showcase/{name}" for name in ("running", "failed", "done", "blocked")
        ]
        active = jobs.phase(
            "three-active",
            {running: "active", failed: "active", done: "active", blocked: "queued"},
        )
        expected = {
            jobs.action(active, done, "success"): "success",
            jobs.action(active, failed, "failure"): "failure",
        }
        mixed = jobs.phase(
            "four-states",
            {running: "active", failed: "aborted", done: "complete", blocked: "queued"},
        )
        output = service.client("show", "--scope", "/showcase", "--show-all")
        require(
            all(
                word in output.stdout
                for word in (
                    "queued",
                    "active",
                    "aborted",
                    "complete",
                    "running",
                    "failed",
                    "done",
                    "blocked",
                )
            ),
            "CLI did not show the four reference states",
        )
        evidence.write_json(
            "cli-presentation.json",
            {"stdout": output.stdout, "stderr": output.stderr, "reference": mixed},
        )
        expected[jobs.action(mixed, running, "success")] = "success"
        next_nodes = jobs.phase("blocked-released", {blocked: "active"})
        expected[jobs.action(next_nodes, blocked, "success")] = "success"
        jobs.phase("final", {blocked: "complete"})
        evidence.record_checks(
            **jobs.finish(expected, capacity=3),
            cli_four_states=True,
            tui="separate_interactive_check_required",
        )
