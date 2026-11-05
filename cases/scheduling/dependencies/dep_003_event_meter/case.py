from harness.jobs import Jobs
from harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("combined")
        paths = {}
        for name in ("event_first", "meter_first"):
            group = flow.add_container(name)
            upstream = jobs.task(group, "upstream")
            upstream.add_event("ready")
            upstream.add_meter("step", min_value=0, max_value=100)
            jobs.task(
                group, "downstream", "./upstream:ready == set and ./upstream:step >= 50"
            )
            paths[name] = (f"/combined/{name}/upstream", f"/combined/{name}/downstream")
        blocked = {
            path: state
            for up, down in paths.values()
            for path, state in ((up, "active"), (down, "queued"))
        }
        jobs.load(flow)
        initial = jobs.phase("upstreams-active", blocked)
        jobs.hold("both-unset-blocks", blocked)
        event_up, event_down = paths["event_first"]
        meter_up, meter_down = paths["meter_first"]
        jobs.action(initial, event_up, "meter:49")
        jobs.action(initial, event_up, "event:ready")
        jobs.action(initial, meter_up, "meter:50")
        single = jobs.hold("single-condition-blocks", blocked)
        first = single[event_up]["groups"]["runtime"]
        second = single[meter_up]["groups"]["runtime"]
        require(
            first["event_values"] == {"ready": True}
            and first["meter_values"] == {"step": 49},
            "Expected event set with meter 49",
        )
        require(
            second["event_values"] == {"ready": False}
            and second["meter_values"] == {"step": 50},
            "Expected meter 50 with event unset",
        )
        jobs.action(initial, event_up, "meter:50")
        jobs.action(initial, meter_up, "event:ready")
        active = jobs.phase(
            "both-conditions-release",
            {path: "active" for pair in paths.values() for path in pair},
        )
        for up, _ in paths.values():
            runtime = active[up]["groups"]["runtime"]
            require(
                runtime["event_values"] == {"ready": True}
                and runtime["meter_values"] == {"step": 50},
                "Combined condition not reflected in public state",
            )
        expected = {
            jobs.action(active, down, "success"): "success"
            for _, down in paths.values()
        }
        jobs.phase(
            "downstreams-complete-upstreams-active",
            {
                path: state
                for up, down in paths.values()
                for path, state in ((up, "active"), (down, "complete"))
            },
        )
        for up, _ in paths.values():
            expected[jobs.action(initial, up, "success")] = "success"
        jobs.phase(
            "final", {path: "complete" for pair in paths.values() for path in pair}
        )
        checks = jobs.finish(expected, capacity=4)
        evidence.record_checks(
            **checks,
            event_only_blocked=True,
            meter_only_blocked=True,
            threshold=[49, 50],
            released_while_upstreams_active=True,
        )
