"""Deterministic monitor fixtures. Only the separately defined workers submit jobs."""

from math import ceil

from takler.core import Flow

from ..runtime import require


STAGES = ("data", "graph", "archive")


def flow_capacity(shape):
    return (
        len(shape["cycles"])
        * len(STAGES)
        * shape["lead_count"]
        * (2 + shape["products_per_lead"])
    )


def tree_counts(cfg):
    """Count the root, flows and leaves without instantiating the tree."""
    workers = cfg["job_concurrency"]
    kind = cfg["tree_kind"]
    if kind == "synthetic_flat":
        tasks = cfg["leaf_tasks"]
        require(
            type(tasks) is int and tasks >= workers + 1, "Need room for jobs and marker"
        )
        flows, containers, depth = 1, 0, 2
    else:
        require(
            kind in {"operations_proxy", "operations_headroom"}, "Unknown tree kind"
        )
        shape = cfg["structure"]
        cycles = shape["cycles"]
        require(
            isinstance(cycles, list)
            and cycles
            and all(
                isinstance(c, str)
                and len(c) == 2
                and c.isascii()
                and c.isdigit()
                and int(c) < 24
                for c in cycles
            )
            and len(set(cycles)) == len(cycles),
            "Expected distinct cycle hours",
        )
        for key in ("lead_count", "lead_interval_hours", "products_per_lead"):
            require(type(shape[key]) is int and shape[key] > 0, f"Invalid {key}")
        background = flow_capacity(shape)
        if kind == "operations_headroom":
            target = cfg["leaf_tasks"]
            require(
                type(target) is int and target > workers + 1, "Need background leaves"
            )
            background = target - workers - 1
        leaves_per_lead = 2 + shape["products_per_lead"]
        leads = ceil(background / leaves_per_lead)
        stages = ceil(leads / shape["lead_count"])
        hours = ceil(stages / len(STAGES))
        flows = ceil(hours / len(cycles))
        containers = hours + stages + leads
        tasks = background + workers + 1
        depth = 5
    return dict(
        kind=kind,
        flows=flows,
        containers=containers,
        tasks=tasks,
        nodes=1 + flows + containers + tasks,
        max_depth=depth,
    )


def add_backdrop(flow, cfg, *, leaves=None):
    gate = f"/{flow.name}:enable_background == set"
    if cfg["tree_kind"] == "synthetic_flat":
        for i in range(cfg["leaf_tasks"] - cfg["job_concurrency"] - 1):
            flow.add_task(f"item{i}").add_trigger(gate)
        return
    shape = cfg["structure"]
    if leaves is None:
        leaves = flow_capacity(shape)
    per_lead = 2 + shape["products_per_lead"]
    for group in range(ceil(leaves / per_lead)):
        stage_number, step = divmod(group, shape["lead_count"])
        hour, index = divmod(stage_number, len(STAGES))
        if step == 0:
            if index == 0:
                cycle = flow.add_container(f"cycle{shape['cycles'][hour]}")
            stage = cycle.add_container(STAGES[index])
        lead_name = f"h{step * shape['lead_interval_hours']:03d}"
        lead = stage.add_container(lead_name)
        prepare = lead.add_task("prepare")
        if index == 0:
            prepare.add_trigger(gate)
        else:
            previous = f"../../{STAGES[index - 1]}/{lead_name}"
            prepare.add_trigger(f"{previous}/process == complete")
        count = min(leaves - group * per_lead, per_lead)
        if count >= 2:
            lead.add_task("process").add_trigger("./prepare == complete")
        for product in range(count - 2):
            lead.add_task(f"product{product:02d}").add_trigger("./process == complete")


def monitor_flows(monitor, cfg):
    """Yield each complete LOAD document; only /monitor contains live jobs."""
    if cfg["tree_kind"] != "operations_headroom":
        add_backdrop(monitor, cfg)
        yield monitor
        return
    capacity = flow_capacity(cfg["structure"])
    background = cfg["leaf_tasks"] - cfg["job_concurrency"] - 1
    for start in range(0, background, capacity):
        if start == 0:
            flow = monitor
        else:
            flow = Flow(f"backdrop{start // capacity:03d}")
            flow.add_event("enable_background")
        add_backdrop(flow, cfg, leaves=min(capacity, background - start))
        yield flow
