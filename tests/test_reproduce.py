import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_selection_preserves_blocked_result_and_refuses_overwrite(tmp_path):
    pytest.importorskip("takler")
    selection = tmp_path / "selection.toml"
    selection.write_text("""schema_version = 1
name = "failure aggregation"
[[groups]]
name = "blocked"
profile = "local"
cases = ["EXE-003"]
[[groups]]
name = "offline"
profile = "offline"
cases = ["DEF-001"]
""")
    destination = tmp_path / "new runs"
    env = dict(os.environ)
    env.pop("SCENARIO_ORVIX_BIN", None)
    argv = [
        sys.executable,
        "-B",
        str(ROOT / "reproduce.py"),
        "--selection",
        str(selection),
        "--runs-dir",
        str(destination),
        "--workers",
        "2",
    ]
    result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 1, result.stderr
    summary = destination / "summary.json"
    original = summary.read_bytes()
    report = json.loads(original)
    assert report["result"] == "failed" and report["passed_runs"] == 1
    assert {item["result"] for item in report["results"]} == {
        "passed",
        "environment_blocked",
    }
    assert not list(destination.glob("blocked/*/server*.log"))
    again = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=5)
    assert again.returncode == 2 and "new runs directory" in again.stderr
    assert summary.read_bytes() == original


def test_selection_rejects_client_for_offline_before_creating_runs(tmp_path):
    selection = tmp_path / "invalid.toml"
    selection.write_text("""schema_version = 1
name = "invalid offline selection"
[[groups]]
name = "offline"
profile = "offline"
client = "go"
cases = ["DEF-001"]
""")
    destination = tmp_path / "absent"
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(ROOT / "reproduce.py"),
            "--selection",
            str(selection),
            "--runs-dir",
            str(destination),
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 2 and "Offline selection" in result.stderr
    assert not destination.exists()
