from takler.core import Bunch, Parameter

from harness.runtime import require


def run(evidence):
    root = Bunch()
    root.add_parameter({"LEVEL": 1, "REGION": "north", "OPTIONAL": "root-default"})
    root_only = root.add_flow("root_only").add_task("work")
    forecast = root.add_flow("forecast")
    forecast.add_parameter({"LEVEL": 2, "ENABLED": True})
    flow_only = forecast.add_task("flow_only")
    group = forecast.add_container("members")
    group.add_parameter({"LEVEL": 3, "SCALE": 1.5})
    inherited = group.add_task("inherited")
    override = group.add_task("override")
    override.add_parameter("LEVEL", 4)
    override.add_parameter(Parameter("OPTIONAL", None))

    sources = (
        (root_only, root, 1),
        (flow_only, forecast, 2),
        (inherited, group, 3),
        (override, override, 4),
    )
    result = {}
    for task, owner, expected in sources:
        level = task.find_parent_parameter("LEVEL")
        require(
            level is owner.find_user_parameter("LEVEL")
            and level.value == expected
            and task.parameters()["LEVEL"] is level,
            f"Wrong parameter resolution at {task.node_path}",
        )
        result[task.node_path] = {"source": owner.node_path, "value": expected}
    require(inherited.find_parameter("LEVEL") is None, "Local lookup inherited a value")
    merged = override.parameters()
    require(
        merged["REGION"].value == "north"
        and merged["ENABLED"].value is True
        and merged["SCALE"].value == 1.5
        and merged["OPTIONAL"].value is None
        and inherited.find_parent_parameter("OPTIONAL").value == "root-default"
        and override.find_parent_parameter("MISSING") is None,
        "Wrong merged values, null shadowing or missing lookup",
    )
    inherited.add_trigger("./override:LEVEL == 4", parse=True)
    require(
        inherited.trigger_expression.evaluate() is True,
        "Variable reference did not resolve",
    )
    override.add_parameter("LEVEL", 5)
    require(
        inherited.trigger_expression.evaluate() is False
        and inherited.find_parent_parameter("LEVEL").value == 3,
        "Local override leaked into sibling or variable reference stayed stale",
    )
    require(
        not forecast.begun and override.attempt_id is None,
        "Parameter checks created runtime",
    )
    evidence.record_checks(
        level_sources=result,
        merged={"REGION": "north", "ENABLED": True, "SCALE": 1.5, "OPTIONAL": None},
        variable_reference=[True, False],
    )
