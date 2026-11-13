from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.runtime import require, wait_for


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        zone = ZoneInfo("Asia/Shanghai")
        now = datetime.now(zone)
        target = (now + timedelta(seconds=25)).replace(
            second=0, microsecond=0
        ) + timedelta(minutes=1)
        flow = jobs.flow("timed")
        jobs.task(flow, "upstream")
        task = jobs.task(flow, "gated", "./upstream == complete")
        task.add_time(target.strftime("%H:%M"))
        up, gated = "/timed/upstream", "/timed/gated"
        evidence.write_json(
            "time-plan.json",
            {
                "constructed_at": now.isoformat(),
                "target": target.isoformat(),
                "rule": "HH:MM match and calendar latch",
                "zone": str(zone),
            },
        )
        jobs.load(flow, begin=False)
        service.client("suspend", up, gated)
        service.client("begin", flow.name)
        initial = jobs.phase("before-target", {up: "queued", gated: "queued"})
        require(
            not any(initial[gated]["groups"]["runtime"]["time_free"].values()),
            "Future time was already latched",
        )
        jobs.hold("future-time-blocks", {up: "queued", gated: "queued"})

        def latched():
            nodes = jobs.view("latest")
            require(
                nodes[up]["summary"]["status"]
                == nodes[gated]["summary"]["status"]
                == "queued",
                "Suspended task started before release",
            )
            return (
                nodes
                if any(nodes[gated]["groups"]["runtime"]["time_free"].values())
                else None
            )

        ready = wait_for("real wall-clock time latch", latched, timeout=110)
        evidence.write_json("time-latched-paused.json", ready)
        require(
            datetime.now(zone) >= target and ready[gated]["summary"]["suspended"],
            "Time matched early or node pause was cleared",
        )
        service.client("resume", gated)
        held = jobs.hold(
            "latched-time-still-needs-trigger", {up: "queued", gated: "queued"}
        )
        require(
            any(held[gated]["groups"]["runtime"]["time_free"].values()),
            "Time latch was lost",
        )
        service.client("resume", up)
        upstream = jobs.phase("upstream-active", {up: "active", gated: "queued"})
        expected = {jobs.action(upstream, up, "success"): "success"}
        active = jobs.phase("gated-active", {up: "complete", gated: "active"})
        started = datetime.fromisoformat(
            active[gated]["groups"]["runtime"]["started_at"]
        )
        require(started >= target, "Time-gated task started before its target")
        expected[jobs.action(active, gated, "success")] = "success"
        jobs.phase("final", {up: "complete", gated: "complete"})
        evidence.record_checks(
            **jobs.finish(expected, capacity=1),
            target=target.isoformat(),
            actual_started_at=started.isoformat(),
            delay_seconds=(started - target).total_seconds(),
            node_pause_verified=True,
            trigger_verified=True,
            wall_clock_unmodified=True,
        )
