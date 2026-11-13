from takler.core import Bunch, Flow, NodeContainer, NodeStatus, Task
from takler.tasks.shell import ShellScriptTask

from takler_test_suite.harness.runtime import require


def run(evidence):
    bunch = Bunch()
    forecast = bunch.add_flow("forecast")
    prepare = forecast.add_task("prepare")
    members = forecast.add_container("members")
    member = members.add_container("m01")
    solve = member.add_task(ShellScriptTask("solve", script_path="jobs/solve.takler"))
    archive = bunch.add_flow("archive")
    archive_prepare = archive.add_task("prepare")
    ready = prepare.add_event("ready", True)
    slots = forecast.add_limit("slots", 2)
    claim = solve.add_in_limit("slots", "/forecast", tokens=1)

    expected = (
        ("/forecast", Flow, forecast, None),
        ("/forecast/prepare", Task, prepare, forecast),
        ("/forecast/members", NodeContainer, members, forecast),
        ("/forecast/members/m01", NodeContainer, member, members),
        ("/forecast/members/m01/solve", ShellScriptTask, solve, member),
        ("/archive", Flow, archive, None),
        ("/archive/prepare", Task, archive_prepare, archive),
    )
    require(bunch.node_path == "/", "Unexpected Bunch root path")
    require(list(bunch.flows) == ["forecast", "archive"], "Wrong flow membership")
    require(forecast.bunch is bunch and archive.bunch is bunch, "Wrong Bunch ownership")
    for path, kind, node, parent in expected:
        require(type(node) is kind, f"Unexpected type at {path}")
        require(
            bunch.find_node(path) is node and node.node_path == path,
            f"Wrong lookup at {path}",
        )
        require(node.parent is parent, f"Wrong parent at {path}")
    require(
        forecast.children == [prepare, members]
        and members.children == [member]
        and member.children == [solve]
        and archive.children == [archive_prepare],
        "Wrong child order",
    )
    require(prepare.is_leaf_node() and solve.is_leaf_node(), "Task is not a leaf")
    require(
        bunch.find_node("/archive/solve") is None, "Missing node unexpectedly resolved"
    )
    require(prepare.find_event("ready") is ready and ready.value is True, "Wrong event")
    require(
        solve.script_path == "jobs/solve.takler"
        and slots.limit == 2
        and slots.value == 0
        and (claim.limit_name, claim.node_path, claim.tokens)
        == ("slots", "/forecast", 1)
        and not bunch.validate_limit_references(),
        "Wrong shell definition or resource reference",
    )
    require(
        not forecast.begun
        and not archive.begun
        and solve.attempt_id is None
        and all(
            node.state.node_status == NodeStatus.unknown for _, _, node, _ in expected
        ),
        "Construction produced execution state",
    )
    evidence.record_checks(
        root="/",
        nodes={"/": "Bunch", **{path: kind.__name__ for path, kind, _, _ in expected}},
        event_ready=True,
        limit={"name": "slots", "capacity": 2, "used": 0, "tokens": 1},
        flows_begun=False,
    )
