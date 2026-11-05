"""Run a declared automatic selection in a new directory; keep every outcome."""

import argparse
import concurrent.futures
import json
import os
import signal
import subprocess
import sys
import threading
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from harness.catalog import load_catalog

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--selection", type=Path, default=ROOT / "selections/wsl-delivery.toml"
    )
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, choices=range(1, 5), default=1)
    args = parser.parse_args()
    selection = tomllib.loads(args.selection.read_text())
    catalog = load_catalog(ROOT / "catalog.toml")
    plan = []
    for group in selection["groups"]:
        for case_id in group["cases"]:
            case = catalog[case_id]
            if case.status != "executable" or group["profile"] not in case.profiles:
                parser.error(f"Unsupported selection: {case_id}/{group['profile']}")
            if group["profile"] == "offline" and (
                "client" in group or "transport" in group
            ):
                parser.error("Offline selection cannot choose client or transport")
            if not group["name"].replace("-", "").isalnum():
                parser.error("Invalid group name")
            plan.append(
                {
                    "group": group["name"],
                    "case_id": case_id,
                    "profile": group["profile"],
                    **{k: group[k] for k in ("client", "transport") if k in group},
                }
            )
    if (
        selection["schema_version"] != 1
        or not plan
        or len({(p["group"], p["case_id"]) for p in plan}) != len(plan)
    ):
        parser.error("Invalid or duplicate selection")
    os.umask(0o077)
    root = args.runs_dir.resolve()
    if root.exists():
        parser.error("Use a new runs directory; prior results are never overwritten")
    root.mkdir(parents=True, mode=0o700)
    (root / "selection.json").write_text(
        json.dumps(selection, ensure_ascii=False, indent=2) + "\n"
    )
    (root / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    processes, lock, stopping = {}, threading.Lock(), threading.Event()

    def execute(item):
        group_root = root / item["group"]
        group_root.mkdir(exist_ok=True)
        argv = [
            sys.executable,
            "-B",
            str(ROOT / "run.py"),
            "--runs-dir",
            str(group_root),
            "run",
            item["case_id"],
            "--profile",
            item["profile"],
        ]
        for key in ("client", "transport"):
            if key in item:
                argv.extend(("--" + key, item[key]))
        output_path = group_root / f"{item['case_id'].lower()}.log"
        with output_path.open("w") as output:
            with lock:
                if stopping.is_set():
                    return {**item, "result": "not_run", "exit_code": None}
                process = subprocess.Popen(
                    argv,
                    cwd=ROOT,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                processes[process.pid] = process
            code = process.wait()
            with lock:
                processes.pop(process.pid, None)
        run_ids = [
            line.removeprefix("RUN_ID=")
            for line in output_path.read_text().splitlines()
            if line.startswith("RUN_ID=")
        ]
        report = group_root / run_ids[0] / "report.json" if len(run_ids) == 1 else None
        value = json.loads(report.read_text()) if report and report.is_file() else {}
        result = {
            **item,
            "argv": argv,
            "exit_code": code,
            "run_id": value.get("run_id"),
            "result": value.get("result", "missing_report"),
            "reason": value.get("reason"),
        }
        print(
            f"{item['group']} {item['case_id']} {result['result']} exit={code}",
            flush=True,
        )
        return result

    results, interrupted = [], False
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    futures = [executor.submit(execute, item) for item in plan]
    try:
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    except KeyboardInterrupt:
        interrupted = True
        stopping.set()
        with lock:
            for process in processes.values():
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
    finally:
        executor.shutdown(wait=True)
    if interrupted:
        results = [future.result() for future in futures]
    passed = len(results) == len(plan) and all(
        item["result"] == "passed" and item["exit_code"] == 0 for item in results
    )
    summary = {
        "at": datetime.now(timezone.utc).isoformat(),
        "scope": "automatic selection only; manual checks are recorded separately",
        "selection": selection["name"],
        "expected_runs": len(plan),
        "passed_runs": sum(
            r["result"] == "passed" and r["exit_code"] == 0 for r in results
        ),
        "result": "interrupted" if interrupted else "passed" if passed else "failed",
        "results": sorted(results, key=lambda r: (r["group"], r["case_id"])),
    }
    (root / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    )
    return 130 if interrupted else 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
