import math
import re
import time

from takler.core import Flow

from takler_test_suite.harness.capacity import Resources, prepare as prepare, wait_until
from takler_test_suite.harness.capacity_load import Commands, analysis
from takler_test_suite.harness.capacity_query import FULL_GROUPS, Queries
from takler_test_suite.harness.online import write_definition
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    cfg = service.capacity
    dimensions = cfg["large"]
    results, loaded = [], 0
    with (
        Resources(evidence, service) as resources,
        Commands(service) as commands,
        Queries(evidence, service, resources, "large") as query,
    ):
        # Capacity reads do not submit these tasks. Replace begins a new Flow;
        # keep the real server manually halted and every input tree queued.
        commands.call("server_halt")
        for size in dimensions["tasks"]:
            require(size > 0, "Tree size must be positive")
            resources.phase = f"build-{size}"
            flow_count = math.ceil(size / dimensions["tasks_per_flow"])
            for flow_index in range(flow_count):
                resources.check()
                flow = Flow(f"tree{flow_index:03}")
                start = flow_index * dimensions["tasks_per_flow"]
                stop = min(size, start + dimensions["tasks_per_flow"])
                for group_start in range(start, stop, dimensions["tasks_per_group"]):
                    group = flow.add_container(
                        f"g{(group_start - start) // dimensions['tasks_per_group']:03}"
                    )
                    for index in range(
                        group_start,
                        min(stop, group_start + dimensions["tasks_per_group"]),
                    ):
                        task = group.add_task(f"n{index:06}")
                        task.add_parameter(
                            "PAYLOAD", "x" * dimensions["parameter_bytes"]
                        )
                        if index == size - 1:
                            task.add_parameter(
                                "WIDE", "w" * dimensions["wide_field_bytes"]
                            )
                target = write_definition(
                    evidence, flow, f"input-{size}-{flow.name}.json"
                )
                require(
                    target.stat().st_size < 4 * 1024 * 1024 - 1024,
                    "Definition shard exceeds ordinary gRPC load envelope",
                )
                if flow_index < loaded:
                    commands.call("replace", f"/{flow.name}", str(target))
                else:
                    commands.call("load", str(target))
                    commands.call("begin", flow.name)
            loaded = flow_count
            groups = sum(
                math.ceil(
                    min(
                        dimensions["tasks_per_flow"],
                        size - f * dimensions["tasks_per_flow"],
                    )
                    / dimensions["tasks_per_group"]
                )
                for f in range(flow_count)
            )
            expected_count = 1 + flow_count + groups + size

            def validate(path, node):
                if path == "/":
                    observed["root"] += 1
                    return
                parts = path.strip("/").split("/")
                require(
                    re.fullmatch(r"tree\d{3}", parts[0]) is not None
                    and int(parts[0][4:]) < flow_count,
                    "Tree path differs from independent input",
                )
                flow_number = int(parts[0][4:])
                if len(parts) == 1:
                    observed["flows"] += 1
                if len(parts) >= 2:
                    require(
                        re.fullmatch(r"g\d{3}", parts[1]) is not None,
                        "Group name differs",
                    )
                    require(
                        int(parts[1][1:])
                        < math.ceil(
                            min(
                                dimensions["tasks_per_flow"],
                                size - flow_number * dimensions["tasks_per_flow"],
                            )
                            / dimensions["tasks_per_group"]
                        ),
                        "Unexpected group index",
                    )
                    if len(parts) == 2:
                        observed["groups"] += 1
                if len(parts) == 3:
                    observed["tasks"] += 1
                    require(
                        re.fullmatch(r"n\d{6}", parts[2]) is not None,
                        "Task name differs",
                    )
                    number = int(parts[2][1:])
                    require(
                        number < size
                        and number // dimensions["tasks_per_flow"] == flow_number
                        and (number % dimensions["tasks_per_flow"])
                        // dimensions["tasks_per_group"]
                        == int(parts[1][1:]),
                        "Task hierarchy differs",
                    )
                    if node:
                        parameters = {
                            p["name"]: p["value"]
                            for p in node["groups"]["parameters"]["user"]
                        }
                        require(
                            parameters["PAYLOAD"]
                            == "x" * dimensions["parameter_bytes"],
                            "Full parameter field differs",
                        )
                        if number == size - 1:
                            require(
                                parameters["WIDE"]
                                == "w" * dimensions["wide_field_bytes"],
                                "Wide field was truncated or not reassembled",
                            )
                        require(
                            node["summary"]["status"] == "queued"
                            and node["groups"]["runtime"]["attempt_id"] is None,
                            "Read-only capacity tree executed a task",
                        )
                require(len(parts) <= 3, "Unexpected tree depth")

            resources.phase = f"measure-{size}"
            for sample in range(dimensions["samples"]):
                for mode in ("summary", "full"):
                    observed = {"root": 0, "flows": 0, "groups": 0, "tasks": 0}
                    row = query.read(mode, validator=validate)
                    require(
                        observed
                        == {
                            "root": 1,
                            "flows": flow_count,
                            "groups": groups,
                            "tasks": size,
                        },
                        "Independent type/path counts differ",
                    )
                    require(
                        row["nodes"] == expected_count,
                        "Read omitted/duplicated independent input nodes",
                    )
                    require(
                        row["elapsed_s"] <= dimensions[f"{mode}_deadline_seconds"],
                        "Large query exceeded frozen deadline",
                    )
                    results.append(
                        {
                            "tasks": size,
                            "total_nodes": expected_count,
                            "sample": sample,
                            **row,
                        }
                    )
            # Wait for two post-load publications so an older in-flight capture
            # cannot be mistaken for saving this whole tree.
            previous = commands.status()["last_checkpoint_at"]
            for _ in range(2):
                deadline = (
                    time.monotonic()
                    + cfg["checkpoint_seconds"]
                    + cfg["load_timeout_seconds"]
                )
                while time.monotonic() < deadline:
                    wait_until(time.monotonic() + 1, resources)
                    current = commands.status()
                    require(
                        current["status"] == "halted"
                        and current["halt_causes"] == ["manual"],
                        "Read-only halt changed",
                    )
                    if current["last_checkpoint_at"] != previous:
                        previous = current["last_checkpoint_at"]
                        break
                else:
                    require(False, "Periodic checkpoint made no progress")
            require(
                not list(evidence.directory.rglob("*.job*")),
                "Large-tree read generated a real job",
            )
        evidence.write_json(
            "large-tree-results.json", {"field_groups": FULL_GROUPS, "results": results}
        )
        analysis(evidence, resources, query.rows)
        evidence.record_checks(
            capacity_profile=cfg["name"],
            tasks=dimensions["tasks"],
            samples_per_mode=dimensions["samples"],
            reads=len(results),
            full_field_groups=FULL_GROUPS,
            wide_field_bytes=dimensions["wide_field_bytes"],
            real_jobs=0,
            checkpoint_enabled=True,
            nodes_and_parameters_verified=True,
        )
