from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.recovery import (
    checkpoint,
    recovered,
    same_runtime,
    status,
)
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        seed = jobs.flow("seed")
        jobs.task(seed, "work")
        jobs.load(seed)
        seed_path, primary_path = "/seed/work", "/primary_only/work"
        first_active = jobs.phase("seed-active", {seed_path: "active"})
        expected = {jobs.action(first_active, seed_path, "success"): "success"}
        first_done = jobs.phase("seed-done", {seed_path: "complete"})
        service.stop()
        first_raw, first = checkpoint(
            evidence, service.checkpoint, "first", {seed_path: "complete"}
        )
        service.start()
        recovered(evidence, service, first, in_flight=0)
        service.client("server-resume")
        primary = jobs.flow("primary_only")
        jobs.task(primary, "work")
        jobs.load(primary)
        active = jobs.phase(
            "primary-active", {seed_path: "complete", primary_path: "active"}
        )
        expected[jobs.action(active, primary_path, "success")] = "success"
        jobs.phase("both-done", {seed_path: "complete", primary_path: "complete"})
        service.stop()
        second_raw, second = checkpoint(
            evidence,
            service.checkpoint,
            "second",
            {seed_path: "complete", primary_path: "complete"},
        )
        backup = service.checkpoint.with_name(service.checkpoint.name + ".b")
        backup_raw, _ = checkpoint(
            evidence, backup, "first-backup", {seed_path: "complete"}
        )
        require(
            backup_raw == first_raw and first_raw != second_raw,
            "Primary/backup generations did not differ as intended",
        )
        fallback_dir = evidence.directory / "fallback"
        fallback_dir.mkdir()
        service.checkpoint = fallback_dir / "takler.check"
        service.checkpoint.write_bytes(second_raw[:64])
        fallback_backup = fallback_dir / "takler.check.b"
        fallback_backup.write_bytes(first_raw)
        service.start()
        recovered(evidence, service, first, source="backup", in_flight=0)
        restored = jobs.view("backup-restored")
        require(
            set(restored) == {"/", "/seed", seed_path},
            "Backup fallback retained primary-only nodes",
        )
        same_runtime(first, restored, [seed_path])
        require(
            restored[seed_path]["groups"]["runtime"]
            == first_done[seed_path]["groups"]["runtime"],
            "Backup changed completed task identity",
        )
        jobs.hold("backup-remains-halted", {seed_path: "complete"})
        require(
            status(evidence, service, "fallback-still-halted")["halt_causes"]
            == ["recovery"],
            "Backup recovery did not remain halted",
        )
        require(
            service.checkpoint.read_bytes() == second_raw[:64]
            and fallback_backup.read_bytes() == first_raw,
            "Fallback wrote files before shutdown",
        )
        service.stop()
        reject_dir = evidence.directory / "reject"
        reject_dir.mkdir()
        service.checkpoint = reject_dir / "takler.check"
        reject_backup = reject_dir / "takler.check.b"
        service.checkpoint.write_bytes(second_raw[:64])
        reject_backup.write_bytes(first_raw[:64])
        service.start(expect_rejection=True)
        ping = service.client("ping", allow_failure=True)
        require(
            ping.returncode == 4 and ping.stderr.strip(),
            "Refused service did not produce a bounded connection error",
        )
        require(
            service.checkpoint.read_bytes() == second_raw[:64]
            and reject_backup.read_bytes() == first_raw[:64],
            "Refused startup changed corrupt files",
        )
        evidence.write_json(
            "startup-refusal.json",
            {
                "exit_code": service.process.returncode,
                "ping_exit_code": ping.returncode,
                "stdout": ping.stdout,
                "stderr": ping.stderr,
                "corrupt_files_unchanged": True,
            },
        )
        service.stop()
        evidence.record_checks(
            **jobs.finish(expected, capacity=1),
            backup_is_previous_generation=True,
            primary_saved_at=second["saved_at"],
            backup_saved_at=first["saved_at"],
            backup_fallback_halted=True,
            primary_only_nodes_absent=True,
            both_corrupt_startup_refused=True,
            refused_exit=1,
            refused_ping_exit=4,
        )
