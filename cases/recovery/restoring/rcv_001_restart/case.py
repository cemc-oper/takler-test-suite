from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.recovery import (
    checkpoint,
    external,
    instant,
    recovered,
    same_runtime,
    status,
)
from takler_test_suite.harness.runtime import require, timestamp


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("restart")
        # Explicit external-job fixture: the service owns the submit wrapper;
        # the observed Shell continues across a clean service restart.
        flow.add_parameter(
            "TAKLER_SHELL_JOB_CMD",
            "setsid {{ TAKLER_JOB | shquote }} > {{ TAKLER_JOBOUT | shquote }} 2>&1 < /dev/null &",
        )
        flow.add_limit("work", 2)
        for name, trigger in (
            ("finished", None),
            ("inflight", None),
            ("waiting", "./inflight == complete"),
        ):
            jobs.task(flow, name, trigger).add_in_limit("work")
        finished, inflight, waiting = [
            f"/restart/{name}" for name in ("finished", "inflight", "waiting")
        ]
        jobs.load(flow)
        first = jobs.phase(
            "first-active", {finished: "active", inflight: "active", waiting: "queued"}
        )
        expected = {jobs.action(first, finished, "success"): "success"}
        states = {finished: "complete", inflight: "active", waiting: "queued"}
        before = jobs.phase("before-stop", states)
        jobs.usage(before, "/restart", [inflight])
        require(
            not service.checkpoint.exists(),
            "Unexpected checkpoint before clean shutdown",
        )
        service.stop()
        raw, reference = checkpoint(evidence, service.checkpoint, "shutdown", states)
        same_runtime(reference, before, states)
        offline = external(
            evidence, jobs, before, [finished, inflight], "service-offline"
        )
        require(
            not offline[finished]["same_process_alive"]
            and offline[inflight]["same_process_alive"],
            "External fixture did not survive the clean service stop",
        )
        service.start()
        recovered(evidence, service, reference)
        restored = jobs.phase("restored", states)
        same_runtime(reference, restored, states)
        jobs.usage(restored, "/restart", [inflight])
        require(
            service.checkpoint.read_bytes() == raw,
            "Restart unexpectedly rewrote the checkpoint",
        )
        expected[jobs.action(restored, inflight, "success")] = "success"
        held_states = {finished: "complete", inflight: "complete", waiting: "queued"}
        completed = jobs.phase("child-completed-while-halted", held_states)
        jobs.usage(completed, "/restart", [])
        jobs.hold("recovery-halt-blocks", held_states)
        require(
            status(evidence, service, "still-halted")["halt_causes"] == ["recovery"],
            "Child report cleared recovery halt",
        )
        resume_at = timestamp()
        service.client("server-resume")
        active = jobs.phase("resumed-waiting", {waiting: "active"})
        require(
            instant(active[waiting]["groups"]["runtime"]["submitted_at"])
            >= instant(resume_at),
            "Waiting job started before server-resume",
        )
        expected[jobs.action(active, waiting, "success")] = "success"
        final = jobs.phase("final", {path: "complete" for path in states})
        jobs.usage(final, "/restart", [])
        require(
            final[finished]["groups"]["runtime"]
            == before[finished]["groups"]["runtime"],
            "Completed task was rerun or changed",
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            clean_restart=True,
            saved_service_status=reference["server_state"]["status"],
            recovered_default_halted=True,
            external_job_survived=True,
            restored_child_accepted=True,
            completed_task_not_rerun=True,
            queued_until_resume=True,
            resume_requested_at=resume_at,
            limit_final=0,
            execution_backend="explicit detached local Shell",
        )
