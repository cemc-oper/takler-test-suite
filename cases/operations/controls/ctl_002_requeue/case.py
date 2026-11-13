from takler_test_suite.harness.control import password
from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.online import rejected
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("requeued")
        flow.add_limit("work", 2)
        for name in ("worker", "peer"):
            jobs.task(flow, name).add_in_limit("work")
        worker, peer = "/requeued/worker", "/requeued/peer"
        jobs.load(flow)
        old = jobs.phase("old-active", {worker: "active", peer: "active"})
        credential = password(old, worker)
        old_attempt = old[worker]["groups"]["runtime"]["attempt_id"]
        peer_runtime = old[peer]["groups"]["runtime"]
        service.client("server-halt")
        old_key = jobs.action(old, worker, "quiet")
        expected = {old_key: "quiet"}
        stopped = jobs.phase("old-process-exited", {worker: "active", peer: "active"})
        jobs.usage(stopped, "/requeued", [worker, peer])
        service.client("requeue", worker)
        cleared = jobs.phase("requeued", {worker: "queued", peer: "active"})
        jobs.usage(cleared, "/requeued", [peer])
        runtime = cleared[worker]["groups"]["runtime"]
        require(
            runtime["attempt_id"] is None
            and runtime["task_id"] is None
            and runtime["try_no"] == 0,
            "Requeue retained the old identity",
        )
        service.client("server-resume")
        current = jobs.phase("new-active", {worker: "active", peer: "active"})
        jobs.usage(current, "/requeued", [worker, peer])
        new_attempt = current[worker]["groups"]["runtime"]["attempt_id"]
        require(new_attempt != old_attempt, "Requeue reused the old attempt")
        stale = service.client(
            "complete",
            "--node-path",
            worker,
            "--attempt-id",
            old_attempt,
            env={**service.env, "TAKLER_PASS": credential},
            allow_failure=True,
        )
        rejection = rejected(stale, "attempt_mismatch")
        evidence.write_json(
            "stale-complete.json",
            {**rejection, "stdout": stale.stdout, "stderr": stale.stderr},
        )
        after = jobs.view("after-stale-complete")
        require(
            after[worker] == current[worker]
            and after[peer]["groups"]["runtime"] == peer_runtime,
            "Stale child changed current or peer state",
        )
        jobs.usage(after, "/requeued", [worker, peer])
        for path in (worker, peer):
            expected[jobs.action(current, path, "success")] = "success"
        final = jobs.phase("final", {worker: "complete", peer: "complete"})
        jobs.usage(final, "/requeued", [])
        new_key = jobs.key(worker, new_attempt)
        require(
            jobs.intervals[old_key]["exit_upper_ns"]
            < jobs.intervals[new_key]["start_floor_ns"],
            "Old and new worker processes overlap",
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            old_attempt=old_attempt,
            new_attempt=new_attempt,
            stale_child=rejection,
            peer_preserved=True,
            limit_final=0,
        )
