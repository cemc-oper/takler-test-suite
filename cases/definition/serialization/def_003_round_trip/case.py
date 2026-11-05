from takler.core import Flow, NodeStatus, Parameter, RepeatDate
from takler.schema import parse_definition
from takler.serialization import build_definition, export_definition
from takler.tasks.shell import ShellScriptTask

from harness.runtime import require


def run(evidence):
    flow = Flow("forecast")
    flow.add_parameter("MODE", "offline")
    flow.add_parameter(Parameter("OPTIONAL", None))
    flow.add_repeat(RepeatDate("DAY", "20261105", "20261109", 2))
    flow.add_limit("slots", 2)
    prepare = flow.add_task("prepare")
    prepare.set_default_node_status(NodeStatus.complete)
    prepare.add_event("ready", True)
    prepare.add_meter("progress", 5, 100)
    require(prepare.set_event("ready", False), "Could not set in-memory event sample")
    require(prepare.set_meter("progress", 73), "Could not set in-memory meter sample")
    solve = flow.add_task(
        ShellScriptTask("solve", script_path="templates with spaces/solve.takler")
    )
    trigger = "./prepare == complete and ./prepare:ready == set"
    solve.add_trigger(trigger)
    solve.add_in_limit("slots", "/forecast", tokens=2)
    solve.add_time("07:30")

    document = export_definition(flow)
    path = evidence.directory / "definition.json"
    path.write_text(document.model_dump_json(indent=2) + "\n")
    text = path.read_text()
    for field in (
        "runtime",
        "secrets",
        "state",
        "begun",
        "calendar",
        "attempt_id",
        "try_no",
        "task_id",
        "job_password",
        "generated_parameters",
    ):
        require(f'"{field}":' not in text, f"Pure definition includes {field}")

    parsed = parse_definition(path.read_bytes())
    require(
        (parsed.kind, parsed.schema_version, parsed.root.name)
        == ("takler.definition", 1, "forecast"),
        "Wrong versioned definition header",
    )
    wire = parsed.root.model_dump(mode="json")
    require(
        wire["user_parameters"]
        == [{"name": "MODE", "value": "offline"}, {"name": "OPTIONAL", "value": None}]
        and wire["repeat"]
        == {
            "type_id": "takler.repeat.date",
            "name": "DAY",
            "start_date": "20261105",
            "end_date": "20261109",
            "step": 2,
        }
        and wire["limits"] == [{"name": "slots", "limit": 2}],
        "Wrong flow definition values",
    )
    prepare_wire, solve_wire = wire["children"]
    require(
        prepare_wire["default_node_status"] == "complete"
        and prepare_wire["events"] == [{"name": "ready", "initial_value": True}]
        and prepare_wire["meters"]
        == [{"name": "progress", "min_value": 5, "max_value": 100}],
        "Wrong event, meter or default status definition",
    )
    require(
        solve_wire["trigger"] == trigger
        and solve_wire["in_limits"]
        == [{"limit_name": "slots", "node_path": "/forecast", "tokens": 2}]
        and solve_wire["times"] == [{"time": "07:30"}],
        "Wrong trigger, limit reference or time definition",
    )

    rebuilt = build_definition(parsed)
    new_prepare = rebuilt.find_node("/forecast/prepare")
    new_solve = rebuilt.find_node("/forecast/solve")
    require(
        type(rebuilt) is Flow
        and rebuilt is not flow
        and not rebuilt.begun
        and type(new_solve) is ShellScriptTask
        and new_solve.script_path == "templates with spaces/solve.takler"
        and new_solve.trigger_expression.expression_str == trigger
        and new_solve.attempt_id is None
        and new_solve.try_no == 0,
        "Rebuilt flow differs from the expected offline structure",
    )
    require(
        new_prepare.default_node_status == NodeStatus.complete
        and new_prepare.state.node_status == NodeStatus.unknown
        and new_prepare.find_event("ready").value is True
        and new_prepare.find_meter("progress").value == 5
        and rebuilt.find_limit("slots").value == 0
        and not rebuilt.validate_limit_references(),
        "Rebuild did not restore fresh definition defaults",
    )
    require(export_definition(rebuilt) == parsed, "Definition changed on re-export")
    require(
        prepare.find_event("ready").value is False
        and prepare.find_meter("progress").value == 73,
        "Export or rebuild modified the source runtime sample",
    )
    evidence.record_checks(
        schema_version=1,
        flow=rebuilt.node_path,
        tasks=["/forecast/prepare", "/forecast/solve"],
        source_sample={"ready": False, "progress": 73},
        rebuilt_defaults={"ready": True, "progress": 5, "begun": False, "try_no": 0},
        re_export_equal=True,
    )
