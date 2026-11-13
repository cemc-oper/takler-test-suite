from pathlib import Path

from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        flow = jobs.flow("outputs")
        flow.add_limit("work", 1)
        jobs.task(flow, "worker").add_in_limit("work")
        worker = "/outputs/worker"
        jobs.load(flow)
        old = jobs.phase("old-active", {worker: "active"})
        service.client("server-halt")
        expected = {jobs.action(old, worker, "quiet"): "quiet"}
        service.client("requeue", worker)
        service.client("server-resume")
        current = jobs.phase("new-active", {worker: "active"})
        old_runtime = old[worker]["groups"]["runtime"]
        new_runtime = current[worker]["groups"]["runtime"]
        old_ref = old[worker]["groups"]["artifacts"]["jobout_ref"]
        new_ref = current[worker]["groups"]["artifacts"]["jobout_ref"]
        require(
            old_runtime["attempt_id"] != new_runtime["attempt_id"]
            and old_ref["path"] != new_ref["path"],
            "Output reference was reused across attempts",
        )
        expected[jobs.action(current, worker, "success")] = "success"
        final = jobs.phase("final", {worker: "complete"})
        require(
            final[worker]["groups"]["artifacts"]["jobout_ref"] == new_ref,
            "Current task points to an old attempt output",
        )
        old_text, new_text = (
            Path(old_ref["path"]).read_text(),
            Path(new_ref["path"]).read_text(),
        )
        require(
            "SCN05_QUIET /outputs/worker" in old_text
            and "SCN05_OK /outputs/worker " + new_runtime["attempt_id"] in new_text,
            "Local output contents do not match their observed attempts",
        )
        path = Path(new_ref["path"])
        moved = path.with_suffix(path.suffix + ".temporarily-missing")
        path.rename(moved)
        try:
            try:
                path.read_text()
            except FileNotFoundError:
                missing = "FileNotFoundError"
            else:
                require(False, "Missing local output was treated as an empty success")
        finally:
            moved.rename(path)
        invalid = service.client(
            "snapshot", "--scope", "/outputs/absent", allow_failure=True
        )
        require(
            invalid.returncode != 0, "Invalid scope was presented as an empty success"
        )
        evidence.write_json(
            "output-sources.json",
            {
                "old_attempt": old_runtime["attempt_id"],
                "current_attempt": new_runtime["attempt_id"],
                "old_reference": old_ref,
                "current_reference": new_ref,
                "old_output": old_text,
                "current_output": new_text,
                "missing_local_file": missing,
                "invalid_scope": {
                    "exit_code": invalid.returncode,
                    "stdout": invalid.stdout,
                    "stderr": invalid.stderr,
                },
                "read_method": "local pathlib.Path.read_text on the same WSL filesystem",
                "remote_file_api": "not claimed",
            },
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=1),
            attempt_specific_references=True,
            actual_local_output_read=True,
            missing_file_error=missing,
            invalid_scope_rejected=True,
            remote_file_api="not claimed",
        )
