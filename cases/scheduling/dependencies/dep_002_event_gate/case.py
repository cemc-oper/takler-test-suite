from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("event_gate")
        upstream = jobs.task(
            flow, "upstream", body="printf '3\\n7\\n11\\n' > input.txt"
        )
        upstream.add_event("unrelated")
        upstream.add_event("ready")
        jobs.task(
            flow,
            "downstream",
            "./upstream:ready == set",
            "awk '{sum += $1} END {print sum}' input.txt > summary.txt",
        )
        up, down = "/event_gate/upstream", "/event_gate/downstream"
        jobs.load(flow)
        initial = jobs.phase("upstream-active", {up: "active", down: "queued"})
        require(
            initial[up]["groups"]["runtime"]["event_values"]
            == {"ready": False, "unrelated": False},
            "Events were not initially unset",
        )
        jobs.hold("unset-event-blocks", {up: "active", down: "queued"})
        jobs.action(initial, up, "event:unrelated")
        unrelated = jobs.hold("unrelated-event-blocks", {up: "active", down: "queued"})
        require(
            unrelated[up]["groups"]["runtime"]["event_values"]
            == {"ready": False, "unrelated": True},
            "Unrelated event was not reported",
        )
        jobs.action(initial, up, "event:ready")
        released = jobs.phase("downstream-active", {up: "active", down: "active"})
        require(
            released[up]["groups"]["runtime"]["event_values"]["ready"],
            "Ready event missing",
        )
        expected = {jobs.action(released, down, "success"): "success"}
        jobs.phase(
            "downstream-complete-upstream-active", {up: "active", down: "complete"}
        )
        expected[jobs.action(initial, up, "success")] = "success"
        jobs.phase("final", {up: "complete", down: "complete"})
        checks = jobs.finish(expected, capacity=2)
        require(
            (jobs.data / "summary.txt").read_text() == "21\n",
            "Unexpected event-gated result",
        )
        evidence.record_checks(
            **checks,
            unset_blocked=True,
            unrelated_blocked=True,
            released_while_upstream_active=True,
            summary=21,
        )
