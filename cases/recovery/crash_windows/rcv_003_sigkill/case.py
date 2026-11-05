import time

from harness.jobs import Jobs
from harness.recovery import (
    await_status,
    checkpoint,
    cleanup_service_helpers,
    external,
    instant,
    recovered,
    require_helper_runtime,
    same_runtime,
    status,
)
from harness.runtime import require, timestamp


def prepare(evidence, service):
    require_helper_runtime()
    service.checkpoint_interval = 35


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("crash")
        flow.add_limit("work", 2)
        for name, trigger in (
            ("saved", None),
            ("late_complete", None),
            ("late_submit", "./late_complete == complete"),
        ):
            jobs.task(flow, name, trigger).add_in_limit("work")
        saved, late_complete, late_submit = [
            f"/crash/{name}" for name in ("saved", "late_complete", "late_submit")
        ]
        jobs.load(flow)
        initial = jobs.phase(
            "initial-active",
            {saved: "active", late_complete: "active", late_submit: "queued"},
        )
        expected = {jobs.action(initial, saved, "success"): "success"}
        states = {saved: "complete", late_complete: "active", late_submit: "queued"}
        before = jobs.phase("before-checkpoint", states)
        require(
            not service.checkpoint.exists(),
            "Lost the first periodic checkpoint boundary",
        )
        published = await_status(
            evidence,
            service,
            "first-periodic",
            lambda value: value["last_checkpoint_at"] is not None,
            timeout=45,
        )
        raw, reference = checkpoint(
            evidence, service.checkpoint, "crash-reference", states
        )
        same_runtime(reference, before, states)
        require(
            instant(published["last_checkpoint_at"]) == instant(reference["saved_at"]),
            "Status and published checkpoint differ",
        )
        publication_observed_ns = time.monotonic_ns()
        expected[jobs.action(before, late_complete, "success")] = "success"
        changed = jobs.phase(
            "after-checkpoint",
            {saved: "complete", late_complete: "complete", late_submit: "active"},
        )
        completed = changed[late_complete]["groups"]["runtime"]
        submitted = changed[late_submit]["groups"]["runtime"]
        require(
            instant(reference["saved_at"])
            < instant(completed["ended_at"])
            <= instant(submitted["submitted_at"])
            <= instant(submitted["started_at"]),
            "Cannot prove checkpoint/completion/submission order",
        )
        baseline_external = external(
            evidence, jobs, changed, [late_complete, late_submit], "before-crash"
        )
        require(
            not baseline_external[late_complete]["same_process_alive"]
            and baseline_external[late_submit]["same_process_alive"],
            "External jobs differ from the intended crash window",
        )
        backup = service.checkpoint.with_name(service.checkpoint.name + ".b")
        require(
            service.checkpoint.read_bytes() == raw and not backup.exists(),
            "Another checkpoint closed the intended loss window",
        )
        require(
            status(evidence, service, "before-kill")["last_checkpoint_at"]
            == published["last_checkpoint_at"],
            "Checkpoint changed before SIGKILL",
        )
        killed_at = timestamp()
        observed_to_kill_s = (time.monotonic_ns() - publication_observed_ns) / 1e9
        original_service_pid = service.process.pid
        try:
            service.crash()
        finally:
            helpers = cleanup_service_helpers(
                evidence, original_service_pid, service.directory
            )
        require(
            service.checkpoint.read_bytes() == raw and not backup.exists(),
            "SIGKILL wrote or changed a checkpoint",
        )
        offline_external = external(
            evidence, jobs, changed, [late_complete, late_submit], "after-crash"
        )
        require(
            offline_external[late_submit]["same_process_alive"],
            "SIGKILL did not leave the observed external job alive",
        )
        service.checkpoint_interval = 600
        service.start()
        recovered(evidence, service, reference)
        restored = jobs.view("restored")
        same_runtime(reference, restored, states)
        jobs.usage(restored, "/crash", [late_complete])
        after_external = external(
            evidence, jobs, changed, [late_complete, late_submit], "after-restart"
        )
        require(
            not after_external[late_complete]["same_process_alive"]
            and after_external[late_submit]["same_process_alive"],
            "Recovered state was confused with actual external processes",
        )
        started = time.monotonic()
        samples = 0
        while time.monotonic() - started < 13:
            current = jobs.view("latest")
            same_runtime(reference, current, states)
            require(
                len(jobs.intervals) == 3
                and len(list(evidence.directory.rglob("*.job*"))) == 3,
                "Recovery automatically resubmitted a job",
            )
            require(
                status(evidence, service, "latest-status")["halt_causes"]
                == ["recovery"],
                "Recovery halt disappeared",
            )
            samples += 1
            time.sleep(0.25)
        evidence.record(
            "recovery_halt_held",
            elapsed_s=time.monotonic() - started,
            samples=samples,
            original_external_job_present=True,
        )
        expected[jobs.action(changed, late_submit, "quiet")] = "quiet"
        final = jobs.view("external-cleaned-without-report")
        same_runtime(reference, final, states)
        require(
            service.checkpoint.read_bytes() == raw,
            "Evidence checkpoint changed during reconciliation",
        )
        wall_window = (
            instant(killed_at) - instant(reference["saved_at"])
        ).total_seconds()
        evidence.write_json(
            "crash-timeline.json",
            {
                "checkpoint_saved_at": reference["saved_at"],
                "late_complete_ended_at": completed["ended_at"],
                "late_submit_submitted_at": submitted["submitted_at"],
                "late_submit_started_at": submitted["started_at"],
                "kill_requested_at": killed_at,
                "capture_to_kill_request_s": wall_window,
                "observed_publication_to_kill_request_s": observed_to_kill_s,
                "lost_changes": [
                    "late_complete completion",
                    "late_submit submission and init",
                ],
                "restored_states": states,
                "external_before": baseline_external,
                "external_offline": offline_external,
                "external_after_restart": after_external,
                "state_repaired_from_logs": False,
            },
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=2),
            sigkill_returncode=-9,
            checkpoint_unchanged=True,
            recovered_default_halted=True,
            capture_to_kill_request_s=wall_window,
            observed_publication_to_kill_request_s=observed_to_kill_s,
            completed_state_lost=True,
            submission_state_lost=True,
            external_process_reconciled=True,
            no_automatic_resubmission=True,
            cleanup_without_child_report=True,
            service_helpers_cleaned=True,
            service_helper_count=len(helpers),
            state_repaired_from_logs=False,
        )
