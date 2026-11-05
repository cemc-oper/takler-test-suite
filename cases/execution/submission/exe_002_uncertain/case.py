import hashlib
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

from harness.control import batch
from harness.jobs import Jobs
from harness.runtime import EnvironmentBlocked, require, wait_for


def run(evidence, service):
    binary = Path(os.environ.get("SCENARIO_ORVIX_BIN", ""))
    if (
        not binary.is_absolute()
        or not binary.is_file()
        or not os.access(binary, os.X_OK)
    ):
        raise EnvironmentBlocked(
            "EXE-002 requires an absolute executable SCENARIO_ORVIX_BIN; see records/scn-06"
        )
    version = subprocess.check_output(
        [str(binary), "version"], text=True, timeout=5
    ).strip()
    evidence.write_json(
        "orvix-version.json",
        {
            "binary": str(binary),
            "version": version,
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        },
    )
    with Jobs(evidence, service) as jobs:
        failed = jobs.flow("definite")
        failed.add_limit("work", 1)
        task = jobs.task(failed, "worker")
        task.add_in_limit("work")
        task.add_parameter("TAKLER_SHELL_JOB_CMD", "/bin/sh -c 'exit 17'")
        jobs.load(failed)
        failure = jobs.phase("definite-failure", {"/definite/worker": "aborted"})
        jobs.usage(failure, "/definite", [])
        bad = failure["/definite/worker"]["groups"]["runtime"]
        require(
            bad["try_no"] == 1
            and bad["started_at"] is None
            and "CalledProcessError" in bad["aborted_reason"],
            "Definite local submission failure was not recorded",
        )
        require(not jobs.intervals, "Definite non-start launched a business process")

        source = Path(__file__).resolve().parents[4] / "assets/controlled.takler"
        script = evidence.directory / "uncertain.takler"
        original = source.read_text()
        gate = """cd {{ SCENARIO_DATA_SHELL }}
key="${TAKLER_NAME#/}"
key="${key//\\//.}.$TAKLER_ATTEMPT_ID"
printf '%s\\n' "$$" > "$key.pid.tmp"
mv -- "$key.pid.tmp" "$key.pid"
timeout --foreground 120s bash -c 'until test -f stop || test -f allow-init; do sleep 0.05; done'
if test -f stop; then exit 0; fi
"""
        script.write_text(
            original.replace(
                "#!/bin/bash\n", '#!/bin/bash\n#ORVIX output="{{ TAKLER_JOBOUT }}"\n', 1
            ).replace(
                'child init --task-id "$$" --node-path "$TAKLER_NAME"',
                gate + 'child init --task-id "$$" --node-path "$TAKLER_NAME"',
                1,
            )
        )
        fault_bin = evidence.directory / "fault-bin"
        fault_bin.mkdir()
        shim = fault_bin / "orvix"
        # Preserve the public direct-orvix command shape. Real orvix submits first;
        # only its successful reply is replaced with a deliberate nonzero exit.
        shim.write_text(f"""#!{sys.executable}
import json
import subprocess
import sys
from pathlib import Path
result = subprocess.run([{str(binary)!r}, *sys.argv[1:]], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
job = Path(sys.argv[-1])
receipt = {{"real_returncode": result.returncode, "sidecar_exists": Path(str(job) + ".info.yaml").is_file(), "injected_exit": 73}}
Path({str(jobs.data / "submit-receipt.json")!r}).write_text(json.dumps(receipt) + "\\n")
with Path({str(jobs.data / "submit-calls.jsonl")!r}).open("a") as stream:
    stream.write(json.dumps(receipt) + "\\n")
raise SystemExit(73 if result.returncode == 0 else result.returncode)
""")
        shim.chmod(0o700)
        flow = jobs.flow("uncertain")
        flow.add_limit("work", 1)
        worker = jobs.task(flow, "worker", script=script)
        worker.add_in_limit("work")
        worker.add_parameter(
            "TAKLER_SHELL_JOB_CMD",
            shlex.quote(str(shim))
            + " submit --scheduler local {{ TAKLER_JOB | shquote }}",
        )
        jobs.task(flow, "waiting").add_in_limit("work")
        path, queued = "/uncertain/worker", "/uncertain/waiting"
        jobs.load(flow)

        def unknown():
            nodes = jobs.view("latest")
            runtime = nodes[path]["groups"]["runtime"]
            key = jobs.key(path, runtime["attempt_id"]) if runtime["attempt_id"] else ""
            return (
                nodes
                if runtime["submission_unknown_reason"] and key in jobs.intervals
                else None
            )

        uncertain = wait_for(
            "real external process and unknown submission", unknown, timeout=40
        )
        evidence.write_json("submission-unknown.json", uncertain)
        runtime = uncertain[path]["groups"]["runtime"]
        attempt = runtime["attempt_id"]
        key = jobs.key(path, attempt)
        require(
            uncertain[path]["summary"]["status"] == "submitted"
            and runtime["task_id"] is None
            and runtime["started_at"] is None,
            "Unknown submission pretended to be active",
        )
        jobs.usage(uncertain, "/uncertain", [path])
        artifact = uncertain[path]["groups"]["artifacts"]
        info = Path(artifact["orvix_info_path"])
        require(
            info == Path(artifact["job_ref"]["path"] + ".info.yaml"),
            "Unexpected default sidecar path",
        )
        sidecar = yaml.safe_load(info.read_text())
        require(
            sidecar["scheduler"] == "local"
            and int(sidecar["job_id"]) == jobs.intervals[key]["pid"],
            "Sidecar differs from the observed external process",
        )
        status = subprocess.run(
            [str(binary), "status", "--format", "json", str(info)],
            env=service.env,
            text=True,
            capture_output=True,
            timeout=5,
            check=True,
        )
        evidence.write_json("external-status.json", json.loads(status.stdout))
        require(
            json.loads(status.stdout)["raw_state"] == "RUNNING",
            "orvix did not confirm the real external job",
        )
        receipt = json.loads((jobs.data / "submit-receipt.json").read_text())
        require(
            receipt
            == {"real_returncode": 0, "sidecar_exists": True, "injected_exit": 73},
            "Fault happened before successful real submission",
        )
        jobs.hold("unknown-holds-reservation", {path: "submitted", queued: "queued"})
        for label, command, args in (
            ("unknown-requeue", "requeue", (path,)),
            ("unknown-force-run", "run", ("--force", path)),
        ):
            batch(
                evidence,
                service,
                label,
                command,
                *args,
                expected=[(path, "flow_state", "none")],
            )
        service.client("server-halt")
        service.client("server-resume")
        unchanged = jobs.hold(
            "resume-does-not-resubmit", {path: "submitted", queued: "queued"}
        )
        require(
            unchanged[path] == uncertain[path],
            "Unknown submission identity or state changed",
        )
        jobs.usage(unchanged, "/uncertain", [path])
        require(
            len(jobs.intervals) == 1
            and len((jobs.data / "submit-calls.jsonl").read_text().splitlines()) == 1,
            "Unexpected extra external submission",
        )

        (jobs.data / "allow-init").touch()
        active = jobs.phase(
            "child-confirms-submission", {path: "active", queued: "queued"}
        )
        require(
            active[path]["groups"]["runtime"]["attempt_id"] == attempt
            and active[path]["groups"]["runtime"]["submission_unknown_reason"] is None,
            "Current child did not resolve uncertainty",
        )
        jobs.action(active, path, "success")
        waiting = jobs.phase("waiting-active", {path: "complete", queued: "active"})
        jobs.usage(waiting, "/uncertain", [queued])
        waiting_key = jobs.action(waiting, queued, "success")
        final = jobs.phase("final", {path: "complete", queued: "complete"})
        jobs.usage(final, "/uncertain", [])
        wait_for("external jobs to exit", jobs.all_exited, timeout=10)
        require(
            len(jobs.intervals) == 2 and set(jobs.intervals) == {key, waiting_key},
            "External job repeated or disappeared",
        )
        outputs = {}
        for node_path in (path, queued):
            groups = final[node_path]["groups"]
            output = Path(groups["artifacts"]["jobout_ref"]["path"])
            lines = output.read_text().splitlines()
            current_attempt = groups["runtime"]["attempt_id"]
            require(
                lines.count(f"SCN05_OK {node_path} {current_attempt}") == 1
                and lines.count("received: success") == 2,
                "Unexpected external business output or child count",
            )
            outputs[jobs.key(node_path, current_attempt)] = str(output)
        evidence.write_json("job-outputs.json", outputs)
        evidence.write_json(
            "orvix-sidecar.json",
            {
                "scheduler": sidecar["scheduler"],
                "job_id": str(sidecar["job_id"]),
                "yaml": info.read_text(),
            },
        )
        evidence.record_checks(
            definite_failure=True,
            definite_attempt=bad["attempt_id"],
            unknown_attempt=attempt,
            real_submit_exit=0,
            injected_wrapper_exit=73,
            real_external_pid=jobs.intervals[key]["pid"],
            jobs=2,
            no_automatic_retry=True,
            child_resolved_unknown=True,
            limit_final=0,
        )
