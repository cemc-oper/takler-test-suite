from takler_test_suite.harness.control import password
from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.runtime import require
from takler_test_suite.harness.security import (
    assert_no_secrets,
    denied,
    private_secret,
    secure,
)


def prepare(evidence, service):
    secure(evidence, service)


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("identities")
        flow.add_limit("work", 2)
        for name in ("worker", "peer"):
            task = jobs.task(flow, name)
            task.add_in_limit("work")
            task.add_event("ready")
            task.add_meter("step", min_value=0, max_value=100)
        worker, peer = "/identities/worker", "/identities/peer"
        jobs.load(flow)
        first = jobs.phase("first-active", {worker: "active", peer: "active"})
        old_attempt = first[worker]["groups"]["runtime"]["attempt_id"]
        old_pass = password(first, worker)
        peer_pass = password(first, peer)
        denials = {}

        def reject(label, node, attempt, credential):
            result = service.client(
                "complete",
                "--node-path",
                node,
                "--attempt-id",
                attempt,
                env={**service.env, "TAKLER_PASS": credential},
                allow_failure=True,
            )
            denials[label] = denied(evidence, result, label)
            assert_no_secrets(evidence, result.stdout, result.stderr)

        reject("missing-pass", worker, old_attempt, "")
        reject(
            "wrong-pass", worker, old_attempt, private_secret(evidence, "wrong_child")
        )
        reject(
            "peer-pass", peer, first[peer]["groups"]["runtime"]["attempt_id"], old_pass
        )
        unchanged = jobs.view("after-current-denials")
        require(
            unchanged[worker] == first[worker] and unchanged[peer] == first[peer],
            "Rejected current child changed a task",
        )
        jobs.usage(unchanged, "/identities", [worker, peer])
        service.client("server-halt")
        expected = {jobs.action(first, worker, "quiet"): "quiet"}
        service.client("requeue", worker)
        service.client("server-resume")
        current = jobs.phase("second-active", {worker: "active", peer: "active"})
        new_attempt = current[worker]["groups"]["runtime"]["attempt_id"]
        new_pass = password(current, worker)
        require(
            new_attempt != old_attempt
            and new_pass != old_pass
            and new_pass != peer_pass,
            "Current attempt reused credentials",
        )
        reject("old-attempt-old-pass", worker, old_attempt, old_pass)
        reject("old-attempt-new-pass", worker, old_attempt, new_pass)
        reject("new-attempt-old-pass", worker, new_attempt, old_pass)
        after = jobs.view("after-stale-denials")
        require(
            after[worker] == current[worker] and after[peer] == current[peer],
            "Invalid identity changed the new or peer instance",
        )
        jobs.usage(after, "/identities", [worker, peer])
        jobs.action(current, worker, "event:ready")
        jobs.action(current, worker, "meter:50")
        attrs = jobs.view("valid-current-attributes")
        require(
            attrs[worker]["groups"]["runtime"]["event_values"]["ready"]
            and attrs[worker]["groups"]["runtime"]["meter_values"]["step"] == 50,
            "Current credential did not update attributes",
        )
        for path in (worker, peer):
            expected[jobs.action(current, path, "success")] = "success"
        final = jobs.phase("final", {worker: "complete", peer: "complete"})
        jobs.usage(final, "/identities", [])
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            denials=denials,
            old_attempt=old_attempt,
            new_attempt=new_attempt,
            current_child_accepted=True,
            peer_preserved=True,
            limit_final=0,
        )
