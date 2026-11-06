import tomllib
from pathlib import Path

from takler.core import Bunch, Flow, NodeStatus, Task

from harness.monitor_tree import add_backdrop, monitor_flows, tree_counts

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


def test_headroom_freeze_and_partial_flow_preserve_count_and_dependencies():
    cfg = tomllib.loads((PROFILE.parent / "monitoring-100k.toml").read_text())
    assert tree_counts(cfg) == dict(
        kind="operations_headroom",
        flows=11,
        containers=4716,
        tasks=100000,
        nodes=104728,
        max_depth=5,
    )
    cfg["structure"].update(cycles=["00"], lead_count=1, products_per_lead=1)
    cfg["job_concurrency"] = 1
    for background in (1, 2, 3, 8, 9, 10, 18, 19):
        cfg["leaf_tasks"] = 2 + background
        bunch = Bunch()
        monitor = Flow("monitor")
        monitor.add_event("enable_background")
        monitor.add_task("worker0")
        monitor.add_task("marker")
        for flow in monitor_flows(monitor, cfg):
            bunch.add_flow(flow)
        pending = list(bunch.flows.values())
        nodes, tasks = 1, 0
        while pending:
            n = pending.pop()
            tasks += isinstance(n, Task)
            nodes += 1
            pending.extend(n.children)
        expected = tree_counts(cfg)
        assert (nodes, tasks, len(bunch.flows)) == (
            expected["nodes"],
            cfg["leaf_tasks"],
            expected["flows"],
        )
        if background > 9:
            gated = bunch.find_node("/backdrop001/cycle00/data/h000/prepare")
            assert not gated.evaluate_trigger()
            bunch.find_flow("backdrop001").set_event("enable_background", True)
            assert gated.evaluate_trigger()
