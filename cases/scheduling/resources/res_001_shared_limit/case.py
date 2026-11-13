from takler_test_suite.harness.jobs import Jobs


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("limited")
        flow.add_limit("work", 2)
        paths = [f"/limited/t{number}" for number in range(1, 5)]
        for number in range(1, 5):
            jobs.task(flow, f"t{number}").add_in_limit("work")
        jobs.load(flow)
        states = {
            path: "active" if index < 2 else "queued"
            for index, path in enumerate(paths)
        }
        first = jobs.phase("first-wave", states)
        jobs.usage(first, "/limited", paths[:2])
        held = jobs.hold("capacity-blocks-next-wave", states)
        jobs.usage(held, "/limited", paths[:2])
        service.client("server-halt")
        expected = {
            jobs.action(first, path, "success"): "success" for path in paths[:2]
        }
        released = jobs.phase(
            "first-wave-released",
            {
                path: "complete" if index < 2 else "queued"
                for index, path in enumerate(paths)
            },
        )
        jobs.usage(released, "/limited", [])
        service.client("server-resume")
        second = jobs.phase(
            "second-wave",
            {
                path: "complete" if index < 2 else "active"
                for index, path in enumerate(paths)
            },
        )
        jobs.usage(second, "/limited", paths[2:])
        for path in paths[2:]:
            expected[jobs.action(second, path, "success")] = "success"
        final = jobs.phase("final", {path: "complete" for path in paths})
        jobs.usage(final, "/limited", [])
        evidence.record_checks(
            **jobs.finish(expected, capacity=2), capacity=2, limit_final=0, waves=2
        )
