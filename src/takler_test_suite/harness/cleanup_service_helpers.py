"""Use WSL's system Python pidfd APIs to clean this run's orphaned helpers."""

import argparse
import json
import os
import select
import signal
from datetime import datetime, timezone
from pathlib import Path


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def identity(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        return None
    return int(fields[19]) if fields[0] not in {"Z", "X"} else None


def helper_role(argv):
    command = b" ".join(argv[1:])
    if b"multiprocessing.resource_tracker" in command:
        return "resource_tracker"
    if b"multiprocessing.spawn" in command and b"spawn_main" in command:
        return "checkpoint_worker"
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--python", type=Path)
    args = parser.parse_args()
    require(
        hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"),
        "System Python lacks Linux pidfd APIs",
    )
    if args.check:
        print(json.dumps({"pidfd": "available"}))
        return
    require(
        args.server_pid and args.directory and args.python,
        "Missing owned service identity",
    )
    require(
        args.directory.is_absolute()
        and args.directory.is_dir()
        and not args.directory.is_symlink(),
        "Invalid owned directory",
    )
    require(identity(args.server_pid) is None, "Owned service is still alive")
    observed_at = datetime.now(timezone.utc).isoformat()
    helpers = []
    for process in Path("/proc").iterdir():
        if not process.name.isdigit():
            continue
        try:
            fields = (process / "stat").read_text().rsplit(")", 1)[1].split()
            if (
                fields[0] in {"Z", "X"}
                or int(fields[2]) != args.server_pid
                or int(fields[3]) != args.server_pid
            ):
                continue
            if (process / "cwd").resolve(strict=True) != args.directory:
                continue
            argv = (process / "cmdline").read_bytes().split(b"\0")
        except (OSError, RuntimeError):
            continue
        require(
            Path(os.fsdecode(argv[0])).resolve() == args.python.resolve(),
            "Unexpected executable in owned service group",
        )
        role = helper_role(argv)
        require(role is not None, "Unknown helper retained for inspection")
        helpers.append(
            {
                "pid": int(process.name),
                "start_ticks": int(fields[19]),
                "service_group": args.server_pid,
                "session": args.server_pid,
                "directory": str(args.directory),
                "role": role,
            }
        )
    results = []
    for helper in sorted(helpers, key=lambda item: item["role"] == "resource_tracker"):
        signals = []
        try:
            descriptor = os.pidfd_open(helper["pid"])
        except ProcessLookupError:
            results.append({**helper, "signals": signals, "exited": True})
            continue
        try:
            if identity(helper["pid"]) == helper["start_ticks"]:
                ready = select.select(
                    [descriptor],
                    [],
                    [],
                    1 if helper["role"] == "resource_tracker" else 0,
                )[0]
                if not ready and helper["role"] == "checkpoint_worker":
                    signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                    signals.append("SIGTERM")
                    ready = select.select([descriptor], [], [], 3)[0]
                if not ready:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                    signals.append("SIGKILL")
                    ready = select.select([descriptor], [], [], 3)[0]
                require(bool(ready), "Owned checkpoint helper did not exit")
            results.append({**helper, "signals": signals, "exited": True})
        finally:
            os.close(descriptor)
    print(
        json.dumps({"observed_at": observed_at, "helpers": helpers, "cleanup": results})
    )


if __name__ == "__main__":
    main()
