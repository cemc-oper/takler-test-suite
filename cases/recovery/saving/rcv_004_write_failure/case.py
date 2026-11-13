from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.recovery import (
    await_status,
    checkpoint,
    hold_pair,
    instant,
    read_only_directory,
    status,
)
from takler_test_suite.harness.runtime import require, timestamp


def prepare(evidence, service):
    directory = evidence.directory / "checkpoint"
    directory.mkdir()
    service.checkpoint = directory / "takler.check"
    service.checkpoint_interval = 20


def resume_rejected(evidence, service, label):
    result = service.client("server-resume", allow_failure=True)
    evidence.write_json(
        f"{label}.json",
        {
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    )
    require(
        result.returncode == 1 and "server_halted" in result.stderr,
        "server-resume bypassed checkpoint failure",
    )


def run(evidence, service):
    directory = service.checkpoint.parent
    backup = directory / "takler.check.b"
    try:
        with Jobs(evidence, service) as jobs:
            flow = jobs.flow("saving")
            flow.add_limit("work", 1)
            for name, trigger in (
                ("baseline", None),
                ("inflight", "./baseline == complete"),
                ("downstream", "./inflight == complete"),
            ):
                jobs.task(flow, name, trigger).add_in_limit("work")
            baseline, inflight, downstream = [
                f"/saving/{name}" for name in ("baseline", "inflight", "downstream")
            ]
            jobs.load(flow)
            first_states = {
                baseline: "active",
                inflight: "queued",
                downstream: "queued",
            }
            active = jobs.phase("first-active", first_states)
            first_status = await_status(
                evidence,
                service,
                "first-periodic",
                lambda value: value["last_checkpoint_at"] is not None,
            )
            first_raw, first = checkpoint(
                evidence, service.checkpoint, "first", first_states
            )
            require(
                not backup.exists()
                and instant(first["saved_at"])
                == instant(first_status["last_checkpoint_at"]),
                "First periodic checkpoint baseline is ambiguous",
            )
            expected = {jobs.action(active, baseline, "success"): "success"}
            second_states = {
                baseline: "complete",
                inflight: "active",
                downstream: "queued",
            }
            active = jobs.phase("second-active", second_states)
            second_status = await_status(
                evidence,
                service,
                "second-periodic",
                lambda value: (
                    value["last_checkpoint_at"] != first_status["last_checkpoint_at"]
                ),
            )
            second_raw, second = checkpoint(
                evidence, service.checkpoint, "second", second_states
            )
            backup_raw, _ = checkpoint(evidence, backup, "first-backup", first_states)
            require(
                backup_raw == first_raw
                and instant(second["saved_at"]) > instant(first["saved_at"]),
                "Two periodic saves did not establish the expected primary/backup pair",
            )
            read_only_directory(evidence, directory, "automatic-resume-fault")
            failed = await_status(
                evidence,
                service,
                "first-write-failure",
                lambda value: "checkpoint_failure" in value["halt_causes"],
            )
            require(
                failed["status"] == "halted"
                and failed["halt_causes"] == ["checkpoint_failure"]
                and failed["message"] == "checkpoint write failed: permission denied",
                "Write failure did not halt with the expected cause",
            )
            resume_rejected(evidence, service, "failure-resume-rejected")
            expected[jobs.action(active, inflight, "success")] = "success"
            held_states = {
                baseline: "complete",
                inflight: "complete",
                downstream: "queued",
            }
            held = jobs.phase("child-completed-during-failure", held_states)
            jobs.usage(held, "/saving", [])
            hold_pair(
                evidence,
                service,
                jobs,
                held_states,
                second_raw,
                first_raw,
                second_status["last_checkpoint_at"],
                ["checkpoint_failure"],
                "failure-blocks-downstream",
            )
            fault_main, _ = checkpoint(
                evidence, service.checkpoint, "failed-primary", second_states
            )
            fault_backup, _ = checkpoint(
                evidence, backup, "failed-backup", first_states
            )
            require(
                fault_main == second_raw and fault_backup == first_raw,
                "Fault changed the fully validated checkpoint pair",
            )
            directory.chmod(0o700)
            evidence.record("storage_repaired", cycle=1, mode="0700")
            resumed_status = await_status(
                evidence,
                service,
                "periodic-recovered-running",
                lambda value: (
                    value["status"] == "running"
                    and not value["halt_causes"]
                    and value["last_checkpoint_at"]
                    != second_status["last_checkpoint_at"]
                ),
            )
            repaired_raw, repaired = checkpoint(
                evidence, service.checkpoint, "repaired", held_states
            )
            require(
                backup.read_bytes() == second_raw
                and instant(repaired["saved_at"])
                == instant(resumed_status["last_checkpoint_at"]),
                "Successful periodic save did not rotate the previous valid primary",
            )
            down_active = jobs.phase("downstream-auto-started", {downstream: "active"})
            require(
                instant(down_active[downstream]["groups"]["runtime"]["submitted_at"])
                >= instant(repaired["saved_at"]),
                "Downstream started before the successful retry",
            )

            # Combine the same storage fault with an independent manual halt.
            service.client("server-halt")
            manual = jobs.flow("manual_gate")
            jobs.task(manual, "work")
            jobs.load(manual)
            gate = "/manual_gate/work"
            combo_status = status(evidence, service, "manual-before-second-fault")
            combo_raw, combo_ref = checkpoint(
                evidence, service.checkpoint, "combined-baseline", {}
            )
            combo_backup_raw, _ = checkpoint(evidence, backup, "combined-backup", {})
            require(
                instant(combo_ref["saved_at"])
                == instant(combo_status["last_checkpoint_at"]),
                "Combined-fault checkpoint baseline changed",
            )
            read_only_directory(evidence, directory, "manual-halt-fault")
            combined = await_status(
                evidence,
                service,
                "combined-write-failure",
                lambda value: (
                    set(value["halt_causes"]) == {"manual", "checkpoint_failure"}
                ),
            )
            require(
                combined["status"] == "halted", "Combined fault did not remain halted"
            )
            resume_rejected(evidence, service, "combined-resume-rejected")
            attempted = status(evidence, service, "combined-resume-side-effect")
            require(
                attempted["status"] == "halted"
                and attempted["halt_causes"] == ["checkpoint_failure"],
                "server-resume did not expose its documented halt-cause effect",
            )
            service.client("server-halt")
            reasserted = status(evidence, service, "manual-reasserted")
            require(
                set(reasserted["halt_causes"]) == {"manual", "checkpoint_failure"},
                "Explicit manual halt was not restored",
            )
            expected[jobs.action(down_active, downstream, "success")] = "success"
            combo_states = {
                baseline: "complete",
                inflight: "complete",
                downstream: "complete",
                gate: "queued",
            }
            jobs.phase("child-completed-during-combined-failure", combo_states)
            hold_pair(
                evidence,
                service,
                jobs,
                combo_states,
                combo_raw,
                combo_backup_raw,
                combo_status["last_checkpoint_at"],
                ["manual", "checkpoint_failure"],
                "combined-failure-holds",
                seconds=2,
            )
            directory.chmod(0o700)
            evidence.record("storage_repaired", cycle=2, mode="0700")
            cleared = await_status(
                evidence,
                service,
                "failure-cleared-manual-remains",
                lambda value: (
                    value["status"] == "halted"
                    and value["halt_causes"] == ["manual"]
                    and value["last_checkpoint_at"]
                    != combo_status["last_checkpoint_at"]
                ),
            )
            jobs.hold("manual-halt-still-blocks", {gate: "queued"})
            require(
                status(evidence, service, "manual-still-present")["halt_causes"]
                == ["manual"],
                "Successful save cleared an independent manual halt",
            )
            resume_at = timestamp()
            service.client("server-resume")
            gate_active = jobs.phase("manual-resumed", {gate: "active"})
            require(
                instant(gate_active[gate]["groups"]["runtime"]["submitted_at"])
                >= instant(resume_at),
                "Manual gate started before explicit resume",
            )
            expected[jobs.action(gate_active, gate, "success")] = "success"
            final = jobs.phase("final", {path: "complete" for path in combo_states})
            jobs.usage(final, "/saving", [])
            evidence.record_checks(
                **jobs.finish(expected, capacity=1),
                two_periodic_generations=True,
                write_permission_faults=2,
                primary_backup_preserved=True,
                halted_child_reports_accepted=True,
                checkpoint_failure_blocks_resume=True,
                automatic_resume_after_save=True,
                manual_halt_survives_successful_save=True,
                resume_clears_manual_but_not_checkpoint_failure=True,
                first_saved_at=first["saved_at"],
                second_saved_at=second["saved_at"],
                automatic_recovery_saved_at=repaired["saved_at"],
                manual_recovery_saved_at=cleared["last_checkpoint_at"],
                limit_final=0,
            )
    finally:
        directory.chmod(0o700)
        evidence.record("storage_permissions_restored", mode="0700")
