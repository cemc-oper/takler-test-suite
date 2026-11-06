import hashlib
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cases.capacity.mixed_load.cap_004_monitors import case
from harness.runtime import RunError


@pytest.mark.parametrize("pids", [[11, 12, 12], [11, 12, 13]])
def test_connections_require_every_reader_on_its_planned_process(pids, monkeypatch):
    readers = [{"pid": pid} for pid in pids]
    endpoints = [f"127.0.0.1:{i + 20000}" for i in range(len(readers))]
    lines = [
        f'0 0 {endpoint} 127.0.0.1:15000 users:(("python",pid={pid},fd=6)) sk:a{i:x}'
        for i, (pid, endpoint) in enumerate(zip(pids, endpoints, strict=True))
    ]
    evidence = Mock()
    result = SimpleNamespace(stdout="\n".join(lines))
    monkeypatch.setattr(case.subprocess, "run", lambda *args, **kwargs: result)
    sockets = case.check_connections(evidence, 15000, readers, "open")
    assert sockets == {"a0", "a1", "a2"}
    assert evidence.record.call_args.kwargs["local_endpoints"][pids[1]] == [
        ep for ep, pid in zip(endpoints, pids, strict=True) if pid == pids[1]
    ]
    result.stdout = result.stdout.replace("sk:a", "sk:b")
    assert case.check_connections(evidence, 15000, readers, "reconnect").isdisjoint(
        sockets
    )
    result.stdout = "\n".join(lines[:-1])
    with pytest.raises(RunError, match="Independent TCP peers"):
        case.check_connections(evidence, 15000, readers, "reconnect")


def test_subtree_inventory_checked_against_loaded_definition(tmp_path):
    (tmp_path / "monitor.json").write_text(
        json.dumps(
            {
                "root": {
                    "name": "monitor",
                    "children": [{"name": "marker", "children": []}],
                }
            }
        )
    )
    evidence = Mock(directory=tmp_path)
    fixture = {"flows": 1, "tasks": 1, "containers": 0}
    paths_hash = hashlib.sha256(b"/monitor\n/monitor/marker").hexdigest()
    rows = [
        {"query_id": "q1", "snapshot_id": "s1"},
        {"query_id": "q2", "snapshot_id": "s2"},
    ]
    readers = []
    for row in rows:
        window = Mock()
        window.step.return_value = {
            **row,
            "node_kinds": {"flow": 1, "task": 1},
            "paths_sha256": paths_hash,
        }
        readers.append(window)
    case.record_inventory(evidence, readers, rows, fixture, full_tree=False)
    for window in readers:
        window.step.return_value["paths_sha256"] = "same-wrong-paths"
    with pytest.raises(RunError, match="fixture"):
        case.record_inventory(evidence, readers, rows, fixture, full_tree=False)
