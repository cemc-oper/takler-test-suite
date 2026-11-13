import argparse
import os
import re
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().with_name("src")))

from takler_test_suite.harness.catalog import CatalogError, load_catalog
from takler_test_suite.harness.loader import load_entry
from takler_test_suite.harness.runtime import (
    EnvironmentBlocked,
    Evidence,
    LocalService,
    RunError,
)
from takler_test_suite.paths import ROOT


def execute(case, profile, runs_dir, transport="grpc", client="python"):
    evidence = Evidence(runs_dir, case, profile)
    if profile == "local":
        evidence.report.update(client=client, transport=transport)
    print(f"RUN_ID={evidence.directory.name}\nRUN_DIR={evidence.directory}", flush=True)
    service = None
    entry = None
    result, reason, exit_code = "passed", None, 0
    try:
        entry = load_entry(case.entry)
        if profile == "offline":
            entry.run(evidence)
        else:
            service = LocalService(evidence)
            if callable(getattr(entry, "prepare", None)):
                entry.prepare(evidence, service)
            service.start()
            entry.run(evidence, service)
    except KeyboardInterrupt:
        result, reason, exit_code = "interrupted", "Interrupted by Ctrl-C", 130
    except EnvironmentBlocked as exc:
        result, reason, exit_code = "environment_blocked", str(exc), 1
    except RunError as exc:
        result, reason, exit_code = "failed", str(exc), 1
    except Exception as exc:
        (evidence.directory / "error.log").write_text(traceback.format_exc())
        result, reason, exit_code = "failed", f"{type(exc).__name__}; see error.log", 1
    finally:
        if service is not None:
            try:
                try:
                    service.stop()
                finally:
                    cleanup = getattr(entry, "cleanup", None)
                    if cleanup is not None:
                        cleanup(evidence, service)
            except Exception as exc:
                (evidence.directory / "cleanup-error.log").write_text(
                    traceback.format_exc()
                )
                evidence.report["cleanup_error"] = type(exc).__name__
                if exit_code == 0:
                    result, reason, exit_code = "failed", "Service cleanup failed", 1
        evidence.finish(result, reason)

    print(f"RESULT={result}" + (f" ({reason})" if reason else ""), flush=True)
    return exit_code


def main():
    devel_root = ROOT.parent.parent
    if ROOT.parent.name == "repo" and devel_root.name == "takler-devel":
        evidence_root = devel_root.parent / "workspace"
    else:
        evidence_root = Path(os.environ.get("XDG_STATE_HOME", ""))
        if not evidence_root.is_absolute():
            evidence_root = Path.home() / ".local" / "state"
    parser = argparse.ArgumentParser(description="Takler scenario runner")
    parser.add_argument(
        "--runs-dir",
        type=Path,
        default=evidence_root / "takler-test-suite" / "runs",
        help="Private evidence directory (default: %(default)s)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate", help="Validate catalog entries and case paths")
    commands.add_parser(
        "list", help="List catalog cases and their implementation status"
    )
    run = commands.add_parser("run", help="Run one executable case")
    run.add_argument("case_id")
    run.add_argument("--profile", choices=("local", "offline"), required=True)
    run.add_argument("--client", choices=("python", "go"))
    run.add_argument("--transport", choices=("grpc", "http"))
    inspect = commands.add_parser("inspect", help="Read a run's recorded result")
    inspect.add_argument("run_id")
    args = parser.parse_args()
    runs_dir = args.runs_dir.resolve()

    if args.command == "inspect":
        if re.fullmatch(r"[a-z]{3}-[0-9]{3}-[a-z0-9_]+", args.run_id) is None:
            parser.error("invalid run ID")
        report = runs_dir / args.run_id / "report.json"
        if report.resolve() != report or not report.is_file():
            parser.error(f"run report not found: {args.run_id}")
        print(report.read_text(), end="")
        return 0

    try:
        cases = load_catalog(ROOT / "catalog.toml")
    except CatalogError as exc:
        parser.error(str(exc))
    if args.command == "validate":
        print(f"Catalog valid: {len(cases)} cases")
    elif args.command == "list":
        for case in cases.values():
            print(
                f"{case.id}\t{case.status}\t{case.kind}\t{','.join(case.profiles)}\t{case.title}"
            )
    else:
        case = cases.get(args.case_id)
        if case is None or case.status != "executable":
            parser.error(f"{args.case_id} is not an executable case")
        if args.profile not in case.profiles:
            parser.error(f"{case.id} does not support profile {args.profile}")
        if args.profile == "offline" and (args.client or args.transport):
            parser.error("offline cases do not accept client or transport")
        os.umask(0o077)
        return execute(
            case,
            args.profile,
            runs_dir,
            args.transport or "grpc",
            args.client or "python",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
