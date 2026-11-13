"""Keep one owned QRY-004 fixture alive for actual WSL terminal interaction.

Run from any directory with --runs-dir and --transport; wait for READY=...
then launch the printed public takler-tui command in another WSL terminal.
Write pause-listener/resume-listener/finish to the printed CONTROL file.
This process owns cleanup and exits after 15 minutes even without finish.
"""

import argparse
import os
import shlex
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from takler_test_suite.harness.catalog import load_catalog
from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.presentation import start_showcase, finish_showcase
from takler_test_suite.harness.runtime import Evidence, LocalService, require
from takler_test_suite.paths import ASSETS, ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--transport", choices=("grpc", "http"), required=True)
    args = parser.parse_args()
    os.umask(0o077)
    case = load_catalog(ROOT / "catalog.toml")["QRY-004"]
    evidence = Evidence(args.runs_dir, case, "local")
    evidence.report.update(
        client="python", transport=args.transport, mode="interactive_fixture"
    )
    service = LocalService(evidence)
    paused = False
    result, reason = "failed", "Interactive fixture did not finish"
    try:
        service.start()
        with Jobs(evidence, service) as jobs:
            script = evidence.directory / "interactive.takler"
            template = (ASSETS / "controlled.takler").read_text()
            script.write_text(template.replace("150s", "900s"))
            mixed, expected = start_showcase(jobs, script=script, detached=True)
            visual = evidence.directory / "visual"
            visual.mkdir()
            control = evidence.directory / "control"
            evidence.write_json(
                "interactive-ready.json",
                {
                    "config": str(service.config),
                    "control": str(control),
                    "pid": service.process.pid,
                    "visual": str(visual),
                },
            )
            print(f"READY={evidence.directory}\nCONTROL={control}", flush=True)
            print(
                "TUI_COMMAND="
                + shlex.join(
                    [
                        str(service.bin / "takler-tui"),
                        "--connect-file",
                        str(service.config),
                        "--scope",
                        "/showcase",
                    ]
                ),
                flush=True,
            )
            deadline = time.monotonic() + 900
            while time.monotonic() < deadline:
                if control.exists():
                    command = control.read_text().strip()
                    control.unlink()
                    if command == "pause-listener":
                        require(not paused, "Listener already paused")
                        os.kill(service.process.pid, signal.SIGSTOP)
                        paused = True
                    elif command == "resume-listener":
                        os.kill(service.process.pid, signal.SIGCONT)
                        paused = False
                    elif command == "finish":
                        break
                    else:
                        require(False, "Unknown interactive control")
                    evidence.record("interactive_control", command=command)
                time.sleep(0.1)
            else:
                require(False, "Interactive fixture timed out")
            if paused:
                os.kill(service.process.pid, signal.SIGCONT)
                paused = False
            expected = finish_showcase(jobs, mixed, expected)
            evidence.record_checks(
                **jobs.finish(expected, capacity=3),
                fixture_completed=True,
                visual_result="recorded separately in observation table",
            )
            result, reason = "passed", None
    finally:
        if paused and service.process:
            os.kill(service.process.pid, signal.SIGCONT)
        service.stop()
        evidence.finish(result, reason)
    return int(result != "passed")


if __name__ == "__main__":
    sys.exit(main())
