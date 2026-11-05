import json

from harness.control import batch
from harness.jobs import Jobs
from harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("pause")
        flow.add_limit("work", 2)
        for name, trigger in (
            ("inflight", None),
            ("held", None),
            ("followup", "./inflight == complete and ./held == complete"),
        ):
            jobs.task(flow, name, trigger).add_in_limit("work")
        up, held, down = "/pause/inflight", "/pause/held", "/pause/followup"
        jobs.load(flow, begin=False)
        service.client("suspend", held)
        service.client("begin", flow.name)
        initial = jobs.phase(
            "node-paused", {up: "active", held: "queued", down: "queued"}
        )
        require(initial[held]["summary"]["suspended"], "Node pause was lost at begin")
        jobs.hold("node-pause-blocks", {up: "active", held: "queued", down: "queued"})
        jobs.usage(initial, "/pause", [up])
        service.client("resume", held)
        resumed = jobs.phase(
            "node-resumed", {up: "active", held: "active", down: "queued"}
        )
        require(not resumed[held]["summary"]["suspended"], "Node remained suspended")
        expected = {jobs.action(resumed, held, "success"): "success"}
        jobs.phase("held-complete", {up: "active", held: "complete", down: "queued"})
        service.client("server-halt")
        expected[jobs.action(initial, up, "success")] = "success"
        stopped = jobs.phase(
            "halted-child-completed", {up: "complete", held: "complete", down: "queued"}
        )
        jobs.usage(stopped, "/pause", [])
        for label, args in (
            ("halted-run", (down,)),
            ("halted-force-run", ("--force", down)),
        ):
            batch(
                evidence,
                service,
                label,
                "run",
                *args,
                expected=[(down, "flow_state", "none")],
            )
        jobs.hold(
            "service-halt-blocks", {up: "complete", held: "complete", down: "queued"}
        )
        status = json.loads(service.client("server-status").stdout)
        evidence.write_json("halted-status.json", status)
        require(
            status["status"] == "halted" and status["halt_causes"] == ["manual"],
            "Child completion cleared the service halt",
        )
        service.client("server-resume")
        final_run = jobs.phase("service-resumed", {down: "active"})
        expected[jobs.action(final_run, down, "success")] = "success"
        final = jobs.phase(
            "final", {up: "complete", held: "complete", down: "complete"}
        )
        jobs.usage(final, "/pause", [])
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            node_pause_verified=True,
            halted_child_accepted=True,
            halted_manual_run_rejected=True,
            limit_final=0,
        )
