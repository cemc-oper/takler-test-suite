import tomllib
from pathlib import Path

from takler.core import Bunch, NodeStatus

from harness.monitor_tree import add_backdrop, tree_counts

PROFILE = Path(__file__).resolve().parents[1] / "assets/monitoring-operations.toml"


def test_frozen_profile_counts_without_building_large_tree():
    cfg = tomllib.loads(PROFILE.read_text())
    assert tree_counts(cfg) == {
        "kind": "operations_proxy",
        "flows": 1,
        "containers": 448,
        "tasks": 9509,
        "nodes": 9959,
        "max_depth": 5,
    }


def test_tiny_operations_proxy_counts_and_triggers():
    cfg = tomllib.loads(PROFILE.read_text())
    cfg["structure"].update(cycles=["06"], lead_count=2, products_per_lead=2)
    bunch = Bunch()
    flow = bunch.add_flow("monitor")
    flow.add_event("enable_background")
    flow.add_task("marker")
    for worker in range(cfg["job_concurrency"]):
        flow.add_task(f"worker{worker}")
    add_backdrop(flow, cfg)
    pending = [(flow, 1)]
    nodes, depth = 1, 0
    while pending:
        node, level = pending.pop()
        nodes += 1
        depth = max(level, depth)
        pending.extend((child, level + 1) for child in node.children)
    assert (nodes, depth) == (tree_counts(cfg)["nodes"], 5)
    data = "/monitor/cycle06/data/h003"
    ready = bunch.find_node(data + "/prepare")
    process = bunch.find_node(data + "/process")
    product = bunch.find_node(data + "/product01")
    next_stage = bunch.find_node("/monitor/cycle06/graph/h003/prepare")
    assert not any(n.evaluate_trigger() for n in (ready, process, product, next_stage))
    flow.set_event("enable_background", True)
    assert ready.evaluate_trigger()
    ready.set_node_status_only(NodeStatus.complete)
    assert process.evaluate_trigger()
    process.set_node_status_only(NodeStatus.complete)
    assert product.evaluate_trigger() and next_stage.evaluate_trigger()


def test_business_polling_clock_and_marker_without_running_business_tree(monkeypatch):
    from unittest.mock import Mock, call
    from run import load_entry
    from harness.catalog import load_catalog

    case = load_entry(
        load_catalog(PROFILE.parents[1] / "catalog.toml")["CAP-004"].entry
    )
    elapsed = [0]

    def advance(seconds):
        elapsed[0] += seconds

    monkeypatch.setattr(case, "time", Mock(monotonic=lambda: elapsed[0], sleep=advance))
    workload, group = Mock(), Mock()
    workload.set_marker.side_effect = lambda value: elapsed[0] * 1_000_000_000
    case.steady_refreshes(
        workload, group, dict(poll_cycles=2, refresh_seconds=60, sample_seconds=1)
    )
    assert elapsed[0] == 120
    assert workload.report_meter.call_args_list == [call(1), call(2)]
    assert group.wave.call_args_list == [
        call(
            "steady-1", "refresh", False, stagger_seconds=0.1, receipt_ns=60_000_000_000
        ),
        call(
            "steady-2", "refresh", True, stagger_seconds=0, receipt_ns=120_000_000_000
        ),
    ]
