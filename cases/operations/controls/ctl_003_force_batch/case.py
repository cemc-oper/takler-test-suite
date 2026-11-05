from harness.control import batch
from harness.jobs import Jobs
from harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("forced")
        flow.add_limit("work", 2)
        for name, trigger in (
            ("running_a", None),
            ("running_b", None),
            ("waiting", "./running_a == complete"),
            ("manual", "./waiting == aborted"),
        ):
            task = jobs.task(flow, name, trigger)
            task.add_in_limit("work")
            if name == "manual":
                task.add_event("flag")
        a, b, waiting, manual = (
            f"/forced/{name}"
            for name in ("running_a", "running_b", "waiting", "manual")
        )
        jobs.load(flow)
        initial = jobs.phase(
            "before-active-force",
            {a: "active", b: "active", waiting: "queued", manual: "queued"},
        )
        jobs.usage(initial, "/forced", [a, b])
        first = batch(
            evidence,
            service,
            "force-active",
            "force",
            "--no-recursive",
            "active",
            a,
            waiting,
            b,
            "/forced/missing",
            expected=[
                (a, "success", "none"),
                (waiting, "flow_state", "none"),
                (b, "success", "none"),
                ("/forced/missing", "node_not_found", "none"),
            ],
        )
        after = jobs.view("after-active-force")
        require(
            all(after[path] == initial[path] for path in (a, b, waiting, manual)),
            "No-op and rejected force changed state",
        )
        jobs.usage(after, "/forced", [a, b])
        service.client("server-halt")
        expected = {jobs.action(initial, a, "quiet"): "quiet"}
        jobs.phase("process-exited-before-force", {a: "active", b: "active"})
        second = batch(
            evidence,
            service,
            "force-complete",
            "force",
            "--no-recursive",
            "complete",
            a,
            manual + ":flag",
            manual,
            expected=[
                (a, "success", "applied"),
                (manual + ":flag", "unsupported_value", "none"),
                (manual, "success", "applied"),
            ],
        )
        terminal = jobs.phase(
            "after-terminal-force",
            {a: "complete", b: "active", waiting: "queued", manual: "complete"},
        )
        jobs.usage(terminal, "/forced", [b])
        require(
            terminal[manual]["groups"]["runtime"]["attempt_id"] is None
            and terminal[manual]["groups"]["runtime"]["event_values"]
            == {"flag": False},
            "Force created a fictitious execution or changed the rejected event",
        )
        require(terminal[b] == initial[b], "Force changed the other in-flight task")
        service.client("server-resume")
        advanced = jobs.phase("waiting-active", {b: "active", waiting: "active"})
        jobs.usage(advanced, "/forced", [b, waiting])
        for path in (b, waiting):
            expected[jobs.action(advanced, path, "success")] = "success"
        final = jobs.phase(
            "final",
            {a: "complete", b: "complete", waiting: "complete", manual: "complete"},
        )
        jobs.usage(final, "/forced", [])
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            active_batch=first,
            terminal_batch=second,
            forced_without_attempt=manual,
            limit_final=0,
        )
