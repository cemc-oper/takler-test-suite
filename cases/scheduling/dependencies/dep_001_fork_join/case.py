from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("chain")
        jobs.task(flow, "prepare", body="printf '3\\n' > seed.txt")
        jobs.task(
            flow,
            "left",
            "./prepare == complete",
            "echo $(( $(cat seed.txt) * 2 )) > left.txt",
        )
        jobs.task(
            flow,
            "right",
            "./prepare == complete",
            "echo $(( $(cat seed.txt) * 4 )) > right.txt",
        )
        jobs.task(
            flow,
            "join",
            "./left == complete and ./right == complete",
            "awk '{sum += $1} END {print sum}' left.txt right.txt > summary.txt",
        )
        paths = {
            name: f"/chain/{name}" for name in ("prepare", "left", "right", "join")
        }
        jobs.load(flow)
        initial = jobs.phase(
            "prepare-active",
            {
                paths["prepare"]: "active",
                **{paths[name]: "queued" for name in ("left", "right", "join")},
            },
        )
        expected = {jobs.action(initial, paths["prepare"], "success"): "success"}
        parallel = jobs.phase(
            "branches-active",
            {
                paths["prepare"]: "complete",
                paths["left"]: "active",
                paths["right"]: "active",
                paths["join"]: "queued",
            },
        )
        expected[jobs.action(parallel, paths["left"], "success")] = "success"
        jobs.phase(
            "left-complete",
            {
                paths["left"]: "complete",
                paths["right"]: "active",
                paths["join"]: "queued",
            },
        )
        jobs.hold(
            "join-waits-for-right",
            {
                paths["left"]: "complete",
                paths["right"]: "active",
                paths["join"]: "queued",
            },
        )
        expected[jobs.action(parallel, paths["right"], "success")] = "success"
        joined = jobs.phase(
            "join-active",
            {
                paths["left"]: "complete",
                paths["right"]: "complete",
                paths["join"]: "active",
            },
        )
        expected[jobs.action(joined, paths["join"], "success")] = "success"
        jobs.phase("final", {path: "complete" for path in paths.values()})
        checks = jobs.finish(expected, capacity=2)
        intervals = {
            path: jobs.intervals[
                next(
                    key
                    for key in expected
                    if key.startswith(path.removeprefix("/").replace("/", ".") + ".")
                )
            ]
            for path in paths.values()
        }
        prep, left, right, join = (
            intervals[paths[name]] for name in ("prepare", "left", "right", "join")
        )
        require(
            prep["exit_upper_ns"]
            < min(left["start_floor_ns"], right["start_floor_ns"]),
            "Branch started before preparation exited",
        )
        require(
            max(left["exit_upper_ns"], right["exit_upper_ns"]) < join["start_floor_ns"],
            "Join started before both branches exited",
        )
        require(
            (jobs.data / "summary.txt").read_text() == "18\n",
            "Unexpected fork/join business result",
        )
        evidence.record_checks(**checks, summary=18, ordering_verified=True)
