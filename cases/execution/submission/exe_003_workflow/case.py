import hashlib
import json
import os
import shlex
import subprocess
from importlib.metadata import version
from datetime import datetime
from pathlib import Path

import yaml

from harness.jobs import Jobs
from harness.runtime import EnvironmentBlocked, require, wait_for

ASSETS = Path(__file__).resolve().parents[4] / "assets"
TASKS = ("prepare", "left", "right", "merge")
EXPECTED_RESULT = b"left=10\nright=31\ntotal=41\n"


def prepare(evidence, service):
    binary = Path(os.environ.get("SCENARIO_ORVIX_BIN", ""))
    if (
        not binary.is_absolute()
        or binary.name != "orvix"
        or not os.access(binary, os.X_OK)
    ):
        raise EnvironmentBlocked(
            "EXE-003 requires an absolute executable SCENARIO_ORVIX_BIN named orvix"
        )
    try:
        import takflow
        from takflow.spec.jobspec import contract_version
    except ImportError as exc:
        raise EnvironmentBlocked(
            "EXE-003 requires takflow installed beside Takler"
        ) from exc
    orvix_version = subprocess.check_output(
        [str(binary), "version"], text=True, timeout=5
    ).strip()
    evidence.write_json(
        "components.json",
        {
            "takflow": {"version": version("takflow"), "module": takflow.__file__},
            "takler": {"version": version("takler")},
            "orvix": {
                "version": orvix_version,
                "binary": str(binary),
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            },
            "jobspec": str(contract_version()),
        },
    )
    # Preserve takflow's public direct-orvix command; PATH points to the real binary.
    service.env["PATH"] = f"{binary.parent}:{service.env['PATH']}"


def export(jobs, name):
    from takflow.backends.runtime.orvix import orvix_job
    from takflow.backends.takler import TaklerBackend
    from takflow.flow import WorkflowEngine
    from takflow.jobspec import ResourceSpec, render_resource_block

    engine = WorkflowEngine(TaklerBackend())
    suite = engine.Suite(name)
    suite.add_variables(
        {
            "TAKLER_HOME": str(jobs.evidence.directory),
            "SCENARIO_DATA_SHELL": shlex.quote(str(jobs.data)),
            "SCENARIO_CHILD": shlex.quote(str(jobs.service.client_bin)),
            "BODY": 'export SCN09_FLOW="'
            + name
            + '"\n'
            + (ASSETS / "workflow_body.sh").read_text(),
            **orvix_job(engine, "local"),
        }
    )
    for task_name in TASKS:
        task = suite.add_task(task_name)
        source = jobs.evidence.directory / f"{name}-{task_name}.takler"
        directives = render_resource_block(
            ResourceSpec(job_name=f"{name}_{task_name}", output="{{ TAKLER_JOBOUT }}")
        )
        template = (ASSETS / "controlled.takler").read_text()
        source.write_text(
            template.replace("#!/bin/bash\n", f"#!/bin/bash\n{directives}\n", 1)
        )
        task.add_variable("TAKLER_SCRIPT", str(source))
        if task_name in ("left", "right"):
            task.add_trigger("./prepare == complete")
        elif task_name == "merge":
            task.add_trigger("./left == complete and ./right == complete")
    target = jobs.evidence.directory / f"{name}.json"
    engine.save_suite(suite, target)
    document = json.loads(target.read_bytes())
    require(
        document["kind"] == "takler.definition" and document["schema_version"] == 1,
        "takflow did not export DefinitionDocument v1",
    )
    jobs.evidence.record(
        "takflow_export",
        definition=target.name,
        sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
        api="WorkflowEngine(TaklerBackend()).save_suite",
    )
    return target


def sidecar(jobs, node, label, *, running=False):
    runtime, artifacts = node["groups"]["runtime"], node["groups"]["artifacts"]
    job = Path(artifacts["job_ref"]["path"])
    output = Path(artifacts["jobout_ref"]["path"])
    path = Path(artifacts["orvix_info_path"])
    require(
        path == Path(f"{job}.info.yaml")
        and path.is_relative_to(jobs.evidence.directory),
        "Unexpected default sidecar path",
    )
    wait_for("orvix local sidecar", path.is_file, timeout=5)
    info = yaml.safe_load(path.read_bytes())
    info["submitted_at"] = info["submitted_at"].isoformat()
    components = json.loads((jobs.evidence.directory / "components.json").read_text())
    require(
        info["version"] == components["orvix"]["version"],
        "Sidecar orvix version differs",
    )
    require(
        info["scheduler"] == "local" and str(info["job_id"]) == runtime["task_id"],
        "Sidecar PID differs from child init",
    )
    require(
        info["script_source"] == str(job)
        and info["script_generated"] == f"{job}.submit",
        "Sidecar scripts differ",
    )
    require(
        Path(f"{job}.submit").is_file() and Path(f"{job}.submit.log").is_file(),
        "Missing real orvix submit artifacts",
    )
    require(
        any(
            item["key"] == "output" and item["value"] == str(output)
            for item in info["directives"]
        ),
        "Jobspec output directive differs",
    )
    binary = os.environ["SCENARIO_ORVIX_BIN"]
    result = subprocess.run(
        [binary, "status", "--format", "json", str(path)],
        cwd=jobs.evidence.directory,
        env=jobs.service.env,
        text=True,
        capture_output=True,
        timeout=5,
    )
    require(result.returncode == 0, "orvix status failed")
    state = json.loads(result.stdout)
    require(
        str(state["job_id"]) == runtime["task_id"] and state["scheduler"] == "local",
        "orvix status identity differs",
    )
    if running:
        require(
            state["state"] == "RUNNING" and not state["terminal"],
            "Parallel branch is not independently RUNNING",
        )
    jobs.evidence.write_json(f"{label}.orvix.json", {"sidecar": info, "status": state})


def run(evidence, service, *, observe=None):
    with Jobs(evidence, service) as jobs:
        expected, parallel = {}, {}
        for name, fail in (("workflow_ok", False), ("workflow_fail", True)):
            target = export(jobs, name)
            service.client("load", str(target))
            paths = {task: f"/{name}/{task}" for task in TASKS}
            loaded = jobs.phase(
                f"{name}-loaded", {p: "unknown" for p in paths.values()}
            )
            require(
                not loaded[f"/{name}"]["groups"]["runtime"]["begun"],
                "Load unexpectedly began the workflow",
            )
            service.client("begin", name)
            states = {
                paths["prepare"]: "active",
                **{paths[t]: "queued" for t in TASKS[1:]},
            }
            active = jobs.phase(f"{name}-prepare", states)
            expected[jobs.action(active, paths["prepare"], "success")] = "success"
            states = {
                paths["prepare"]: "complete",
                paths["left"]: "active",
                paths["right"]: "active",
                paths["merge"]: "queued",
            }
            both = jobs.phase(f"{name}-parallel", states)
            for branch, value in (("left", b"10\n"), ("right", b"31\n")):
                require(
                    (jobs.data / f"{name}.{branch}.sum").read_bytes() == value,
                    "Partial sum differs from the business input",
                )
                sidecar(
                    jobs, both[paths[branch]], f"{name}-{branch}-running", running=True
                )
            left_key = jobs.action(both, paths["left"], "success")
            expected[left_key] = "success"
            states[paths["left"]] = "complete"
            jobs.phase(f"{name}-left-complete", states)
            jobs.hold(f"{name}-merge-blocked", states)
            outcome = "failure" if fail else "success"
            right_key = jobs.action(both, paths["right"], outcome)
            expected[right_key] = outcome
            parallel[name] = {"left": left_key, "right": right_key}
            states[paths["right"]] = "aborted" if fail else "complete"
            if fail:
                failed = jobs.phase("business-failure", states)
                require(
                    "exit=7"
                    in failed[paths["right"]]["groups"]["runtime"]["aborted_reason"],
                    "Controlled shell failure was not reported",
                )
                jobs.hold("failed-branch-blocks-merge", states)
                require(
                    not (jobs.data / f"{name}.result.txt").exists(),
                    "Failed workflow produced a merge result",
                )
                final = failed
            else:
                states[paths["merge"]] = "active"
                merge = jobs.phase("merge-started", states)
                require(
                    (jobs.data / f"{name}.result.txt").read_bytes() == EXPECTED_RESULT,
                    "Business result differs from independent expected bytes",
                )
                expected[jobs.action(merge, paths["merge"], "success")] = "success"
                final = jobs.phase(
                    "workflow-complete", {p: "complete" for p in paths.values()}
                )
                times = {
                    task: final[path]["groups"]["runtime"]
                    for task, path in paths.items()
                }
                instant = datetime.fromisoformat
                require(
                    all(
                        instant(times["prepare"]["ended_at"])
                        <= instant(times[branch]["submitted_at"])
                        for branch in ("left", "right")
                    ),
                    "A branch submitted before prepare completed",
                )
                require(
                    max(
                        instant(times[branch]["ended_at"])
                        for branch in ("left", "right")
                    )
                    <= instant(times["merge"]["submitted_at"])
                    <= instant(times["merge"]["started_at"]),
                    "Merge submitted before both branches completed",
                )
                evidence.write_json("business-timeline.json", times)
            for task in TASKS[:3] if fail else TASKS:
                sidecar(jobs, final[paths[task]], f"{name}-{task}-final")
                require(
                    final[paths[task]]["groups"]["runtime"]["try_no"] == 1,
                    "Business task was retried",
                )
        capacity = 2
        if observe is not None:
            extra = observe(jobs)
            expected.update(extra)
            capacity = 3
        checks = jobs.finish(expected, capacity=capacity)
        for name, pair in parallel.items():
            left, right = [jobs.intervals[pair[branch]] for branch in ("left", "right")]
            overlap = min(left["last_alive_ns"], right["last_alive_ns"]) - max(
                left["first_seen_ns"], right["first_seen_ns"]
            )
            require(overlap > 0, "Branches did not run in parallel")
            pair["observed_overlap_s"] = overlap / 1e9
        evidence.write_json("parallel-evidence.json", parallel)
        evidence.record_checks(
            **checks,
            takflow_export=True,
            orvix_local=True,
            success_result=EXPECTED_RESULT.decode(),
            failure_exit=7,
            failed_merge_queued=True,
            no_force_complete=True,
            exercise=observe is not None,
            tui="manual_observation_recorded_separately",
        )
