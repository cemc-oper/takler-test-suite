import json

from takler.serialization import export_definition

from .runtime import require


def write_definition(evidence, flow, name="definition.json"):
    document = export_definition(flow)
    require(document.schema_version == 1, "Expected DefinitionDocument v1")
    path = evidence.directory / name
    path.write_text(document.model_dump_json(indent=2) + "\n")
    return path


def snapshot(evidence, service, label, scope="/"):
    output = service.client(
        "snapshot",
        "--scope",
        scope,
        "--fields",
        "summary,definition,parameters,runtime,artifacts",
    ).stdout
    view = json.loads(output)
    evidence.write_json(f"{label}.json", view)
    return view["nodes_by_path"]


def rejected(result, category, code=1):
    require(
        result.returncode == code and result.stderr.startswith(f"{category}:"),
        f"Expected {category} exit {code}, got exit {result.returncode}",
    )
    return {"category": category, "exit_code": code}
