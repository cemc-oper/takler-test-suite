"""Compose EXE-003 and the QRY-004 fixture; --keep-service enables observation.

Evidence directories are always retained. A live service is kept only with the
explicit flag, until control=finish/stop, Ctrl-C, or a bounded deadline.
"""

import argparse
import os
import shlex
import signal
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.catalog import load_catalog
from harness.loader import load_entry
from harness.presentation import finish_showcase, start_showcase
from harness.runtime import (
    EnvironmentBlocked,
    Evidence,
    LocalService,
    RunError,
    require,
)

ROOT = Path(__file__).resolve().parents[1]


def observe(jobs, *, keep_service, max_seconds):
    evidence, service = jobs.evidence, jobs.service
    script = evidence.directory / "interactive.takler"
    script.write_text(
        Path(__file__)
        .with_name("controlled.takler")
        .read_text()
        .replace("150s", "1000s")
    )
    mixed, expected = start_showcase(jobs, script=script, detached=True)
    output = service.client("show", "--scope", "/", "--show-all")
    require(
        all(
            word in output.stdout
            for word in (
                "workflow_ok",
                "workflow_fail",
                "showcase",
                "active",
                "aborted",
                "complete",
                "queued",
            )
        ),
        "Composed CLI view lacks reference states",
    )
    evidence.write_json(
        "exercise-cli.json", {"stdout": output.stdout, "reference": mixed}
    )
    control = evidence.directory / "control"
    tui = [
        str(service.bin / "takler-tui"),
        "--connect-file",
        str(service.config),
        "--scope",
        "/",
    ]
    ready = {
        "config": str(service.config),
        "control": str(control),
        "pid": service.process.pid,
        "keep_service": keep_service,
        "tui_command": shlex.join(tui),
        "commands": ["finish", "stop", "pause-listener", "resume-listener"],
        "manual_tui_result": "pending_observation",
        "max_seconds": max_seconds,
    }
    evidence.write_json("exercise-ready.json", ready)
    print(
        f"READY={evidence.directory}\nCONTROL={control}\nTUI_COMMAND={ready['tui_command']}",
        flush=True,
    )
    paused = False
    try:
        if keep_service:
            deadline = time.monotonic() + max_seconds
            while time.monotonic() < deadline:
                if control.exists():
                    command = control.read_text().strip()
                    control.unlink()
                    evidence.record("interactive_control", command=command)
                    if command == "finish":
                        break
                    if command == "stop":
                        raise KeyboardInterrupt
                    if command == "pause-listener":
                        require(not paused, "Listener already paused")
                        os.kill(service.process.pid, signal.SIGSTOP)
                        paused = True
                    elif command == "resume-listener":
                        require(paused, "Listener is not paused")
                        os.kill(service.process.pid, signal.SIGCONT)
                        paused = False
                    else:
                        require(False, "Unknown exercise control")
                time.sleep(0.1)
            else:
                raise RunError("Operator exercise timed out")
    finally:
        if paused:
            os.kill(service.process.pid, signal.SIGCONT)
    return finish_showcase(jobs, mixed, expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--transport", choices=("grpc", "http"), default="grpc")
    parser.add_argument("--client", choices=("python", "go"), default="python")
    parser.add_argument("--keep-service", action="store_true")
    parser.add_argument(
        "--max-seconds", type=int, choices=range(1, 901), default=900, metavar="1..900"
    )
    args = parser.parse_args()
    os.umask(0o077)
    case = load_catalog(ROOT / "catalog.toml")["EXE-003"]
    evidence = Evidence(args.runs_dir.resolve(), case, "local")
    evidence.report.update(
        client=args.client,
        transport=args.transport,
        mode="operator_exercise",
        keep_service=args.keep_service,
    )
    print(f"RUN_ID={evidence.directory.name}\nRUN_DIR={evidence.directory}", flush=True)
    service = LocalService(evidence)
    result, reason, code = "passed", None, 0
    try:
        entry = load_entry(case.entry)
        entry.prepare(evidence, service)
        service.start()
        entry.run(
            evidence,
            service,
            observe=lambda jobs: observe(
                jobs, keep_service=args.keep_service, max_seconds=args.max_seconds
            ),
        )
    except KeyboardInterrupt:
        result, reason, code = "interrupted", "Exercise stopped", 130
    except EnvironmentBlocked as exc:
        result, reason, code = "environment_blocked", str(exc), 1
    except Exception as exc:
        (evidence.directory / "error.log").write_text(traceback.format_exc())
        result, reason, code = (
            "failed",
            str(exc) if isinstance(exc, RunError) else "See error.log",
            1,
        )
    finally:
        try:
            service.stop()
        except Exception:
            (evidence.directory / "cleanup-error.log").write_text(
                traceback.format_exc()
            )
            result, reason, code = "failed", "Exercise service cleanup failed", 1
        evidence.finish(result, reason)
    print(f"RESULT={result}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
