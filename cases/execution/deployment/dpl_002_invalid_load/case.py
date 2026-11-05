import copy
import json

from takler.core import Flow

from harness.online import rejected, snapshot, write_definition
from harness.runtime import require


def run(evidence, service):
    original = Flow("stable")
    original.add_parameter("MARKER", "original")
    original.add_task("keep")
    path = write_definition(evidence, original)
    service.client("load", str(path))
    baseline = snapshot(evidence, service, "before-invalid", "/stable")
    document = json.loads(path.read_bytes())
    duplicate = copy.deepcopy(document)
    duplicate["root"]["user_parameters"] = [{"name": "MARKER", "value": "replacement"}]
    duplicate["root"]["children"] = [{"name": "unexpected", "type_id": "takler.task"}]
    invalid = [
        ("duplicate_flow", duplicate, "flow_state"),
        (
            "runtime_field",
            {
                **document,
                "root": {**document["root"], "task_id": "synthetic-invalid-field"},
            },
            "invalid_request",
        ),
        (
            "unknown_type",
            {
                **document,
                "root": {**document["root"], "type_id": "untrusted.flow"},
            },
            "invalid_request",
        ),
        ("unknown_version", {**document, "schema_version": 99}, "invalid_request"),
        ("broken_json", b'{"kind":', "invalid_request"),
    ]
    results = {}
    for label, data, category in invalid:
        candidate = evidence.directory / f"{label}.json"
        candidate.write_bytes(
            data if isinstance(data, bytes) else json.dumps(data).encode()
        )
        response = service.client("load", str(candidate), allow_failure=True)
        results[label] = rejected(response, category)
        unchanged = snapshot(evidence, service, f"after-{label}", "/stable")
        require(
            unchanged == baseline, f"{label}: failed load changed the original tree"
        )
    roots = snapshot(evidence, service, "final-root")
    require(
        set(roots) == {"/", "/stable", "/stable/keep"},
        "Failed load partially added nodes",
    )
    evidence.record_checks(
        transport=service.transport,
        rejected=results,
        original_tree_unchanged=True,
        definition_generation=baseline["/stable"]["summary"]["definition_generation"],
    )
