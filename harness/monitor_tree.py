"""Deterministic monitor fixtures. Only the separately defined workers submit jobs."""

from .runtime import require


STAGES = ("data", "graph", "archive")


def tree_counts(cfg):
    """Count the root, flow and leaves without instantiating the tree."""
    workers = cfg["job_concurrency"]
    if cfg["tree_kind"] == "synthetic_flat":
        tasks = cfg["leaf_tasks"]
        require(
            type(tasks) is int and tasks >= workers + 1, "Need room for jobs and marker"
        )
        containers, depth = 0, 2
    else:
        require(cfg["tree_kind"] == "operations_proxy", "Unknown tree kind")
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
        stages = len(cycles) * len(STAGES)
        leads = stages * shape["lead_count"]
        containers = len(cycles) + stages + leads
        tasks = leads * (2 + shape["products_per_lead"]) + workers + 1
        depth = 5
    return dict(
        kind=cfg["tree_kind"],
        flows=1,
        containers=containers,
        tasks=tasks,
        nodes=2 + containers + tasks,
        max_depth=depth,
    )


def add_backdrop(flow, cfg):
    gate = f"/{flow.name}:enable_background == set"
    if cfg["tree_kind"] == "synthetic_flat":
        for i in range(cfg["leaf_tasks"] - cfg["job_concurrency"] - 1):
            flow.add_task(f"item{i}").add_trigger(gate)
        return
    shape = cfg["structure"]
    for hour in shape["cycles"]:
        cycle = flow.add_container(f"cycle{hour}")
        for index, stage_name in enumerate(STAGES):
            stage = cycle.add_container(stage_name)
            for step in range(shape["lead_count"]):
                lead_name = f"h{step * shape['lead_interval_hours']:03d}"
                lead = stage.add_container(lead_name)
                prepare = lead.add_task("prepare")
                if index == 0:
                    prepare.add_trigger(gate)
                else:
                    previous = f"../../{STAGES[index - 1]}/{lead_name}"
                    prepare.add_trigger(f"{previous}/process == complete")
                lead.add_task("process").add_trigger("./prepare == complete")
                for product in range(shape["products_per_lead"]):
                    lead.add_task(f"product{product:02d}").add_trigger(
                        "./process == complete"
                    )
