"""Record public CLI batch outcomes and replay credentials privately."""

import re

from .runtime import require


def batch(evidence, service, label, command, *args, expected):
    result = service.client(command, *args, allow_failure=True)
    failed = any(category != "success" for _, category, _ in expected)
    require(result.returncode == int(failed), f"Unexpected {label} exit status")
    rows = re.findall(r"^\[(\d+)\] (\S+) (\w+) effect=(\w+):", result.stdout, re.M)
    actual = [(target, category, effect) for _, target, category, effect in rows]
    require(
        [int(index) for index, *_ in rows] == list(range(len(expected)))
        and actual == expected,
        f"Unexpected per-item outcomes for {label}: {actual}",
    )
    evidence.write_json(
        f"{label}.json",
        {
            "command": command,
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "items": actual,
        },
    )
    return actual


def password(nodes, path):
    from pathlib import Path

    job = Path(nodes[path]["groups"]["artifacts"]["job_ref"]["path"])
    match = re.search(r'^export TAKLER_PASS="([^"]+)"$', job.read_text(), re.M)
    require(match is not None, "Missing generated test child credential")
    return match[1]
