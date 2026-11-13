from takler.core import Flow

from takler_test_suite.harness.online import snapshot, write_definition
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    flow = Flow("deployed")
    flow.add_parameter("MODE", "load-only")
    flow.add_limit("slots", 2)
    group = flow.add_container("members")
    prepare = group.add_task("prepare")
    prepare.add_event("ready", True)
    prepare.add_meter("progress", 5, 100)
    downstream = group.add_task("downstream")
    downstream.add_trigger("./prepare == complete")
    downstream.add_in_limit("slots", "/deployed")
    target = write_definition(evidence, flow)
    service.client("load", str(target))
    nodes = snapshot(evidence, service, "loaded", "/deployed")
    expected = {
        "/deployed": ("takler.flow", "/"),
        "/deployed/members": ("takler.container", "/deployed"),
        "/deployed/members/prepare": ("takler.task", "/deployed/members"),
        "/deployed/members/downstream": ("takler.task", "/deployed/members"),
    }
    require(set(nodes) == set(expected), "Loaded paths differ from definition")
    for path, (kind, parent) in expected.items():
        node = nodes[path]
        require(
            node["summary"]["type_id"] == kind
            and node["parent_path"] == parent
            and node["summary"]["status"] == "unknown",
            f"Wrong type, parent or initial state at {path}",
        )
    root = nodes["/deployed"]["groups"]
    require(
        root["runtime"]["begun"] is False
        and root["definition"]["limits"] == [{"name": "slots", "limit": 2}]
        and {item["name"]: item.get("value") for item in root["parameters"]["user"]}[
            "MODE"
        ]
        == "load-only",
        "Flow definition differs after load",
    )
    source = nodes["/deployed/members/prepare"]["groups"]
    require(
        source["definition"]["events"] == [{"name": "ready", "initial_value": True}]
        and source["definition"]["meters"]
        == [{"name": "progress", "min_value": 5, "max_value": 100}],
        "Task attributes differ after load",
    )
    for path in expected:
        if nodes[path]["summary"]["node_kind"] == "task":
            runtime = nodes[path]["groups"]["runtime"]
            require(
                runtime["attempt_id"] is None and runtime["try_no"] == 0,
                "Loading unexpectedly created an execution attempt",
            )
    evidence.record_checks(
        transport=service.transport,
        paths=sorted(nodes),
        begun=False,
        task_attempts=0,
        attributes_match=True,
    )
