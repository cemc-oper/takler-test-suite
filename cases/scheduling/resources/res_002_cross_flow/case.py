from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        alpha, beta = jobs.flow("alpha"), jobs.flow("beta")
        alpha.add_limit("work", 2)
        for flow, name, trigger in (
            (alpha, "success", None),
            (alpha, "retry", "./success == complete"),
            (beta, "failure", None),
            (beta, "advance", "./failure == aborted"),
        ):
            jobs.task(flow, name, trigger).add_in_limit("work", node_path="/alpha")
        success, retry, failure, advance = (
            "/alpha/success",
            "/alpha/retry",
            "/beta/failure",
            "/beta/advance",
        )
        jobs.load(alpha, beta)
        initial = jobs.phase(
            "initial",
            {success: "active", failure: "active", retry: "queued", advance: "queued"},
        )
        jobs.usage(initial, "/alpha", [success, failure])
        service.client("server-halt")
        expected = {jobs.action(initial, success, "success"): "success"}
        released = jobs.phase(
            "after-success", {success: "complete", failure: "active", retry: "queued"}
        )
        jobs.usage(released, "/alpha", [failure])
        service.client("server-resume")
        retried = jobs.phase(
            "retry-active", {failure: "active", retry: "active", advance: "queued"}
        )
        jobs.usage(retried, "/alpha", [failure, retry])
        service.client("server-halt")
        expected[jobs.action(initial, failure, "failure")] = "failure"
        failed = jobs.phase(
            "after-failure", {failure: "aborted", retry: "active", advance: "queued"}
        )
        jobs.usage(failed, "/alpha", [retry])
        require(
            "exit=7" in failed[failure]["groups"]["runtime"]["aborted_reason"],
            "Failure reason missing",
        )
        service.client("server-resume")
        advanced = jobs.phase("advance-active", {retry: "active", advance: "active"})
        jobs.usage(advanced, "/alpha", [retry, advance])
        advance_attempt = advanced[advance]["groups"]["runtime"]["attempt_id"]
        service.client("server-halt")
        old_key = jobs.action(retried, retry, "quiet")
        expected[old_key] = "quiet"
        old_exited = jobs.phase(
            "old-process-exited", {retry: "active", advance: "active"}
        )
        jobs.usage(old_exited, "/alpha", [retry, advance])
        service.client("requeue", retry)
        requeued = jobs.phase("after-requeue", {retry: "queued", advance: "active"})
        jobs.usage(requeued, "/alpha", [advance])
        require(
            requeued[retry]["groups"]["runtime"]["attempt_id"] is None,
            "Requeue retained old attempt",
        )
        service.client("server-resume")
        restarted = jobs.phase("new-retry-active", {retry: "active", advance: "active"})
        jobs.usage(restarted, "/alpha", [retry, advance])
        new_key = jobs.key(retry, restarted[retry]["groups"]["runtime"]["attempt_id"])
        require(
            new_key != old_key
            and restarted[advance]["groups"]["runtime"]["attempt_id"]
            == advance_attempt,
            "Requeue identity isolation failed",
        )
        require(
            jobs.intervals[old_key]["exit_upper_ns"]
            < jobs.intervals[new_key]["start_floor_ns"],
            "Old and new retry processes overlap",
        )
        for path in (retry, advance):
            expected[jobs.action(restarted, path, "success")] = "success"
        final = jobs.phase(
            "final",
            {
                success: "complete",
                retry: "complete",
                failure: "aborted",
                advance: "complete",
            },
        )
        jobs.usage(final, "/alpha", [])
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            capacity=2,
            limit_final=0,
            release_paths=["success", "failure", "requeue"],
            old_attempt=old_key.rsplit(".", 1)[1],
            new_attempt=new_key.rsplit(".", 1)[1],
            unrelated_attempt_preserved=True,
        )
