import json

from takler.schema import DefinitionError, parse_definition
from takler.serialization import build_definition

from takler_test_suite.harness.runtime import require


def document(children=None, **root_fields):
    return {
        "kind": "takler.definition",
        "schema_version": 1,
        "root": {
            "type_id": "takler.flow",
            "name": "forecast",
            "children": children or [],
            **root_fields,
        },
    }


def run(evidence):
    task = {"type_id": "takler.task", "name": "work"}
    marker = "synthetic-redaction-probe"
    cases = (
        (
            "wrong_version",
            parse_definition,
            {**document(), "schema_version": 2},
            "unsupported_version",
        ),
        (
            "unknown_type",
            parse_definition,
            document([{**task, "type_id": "example.unknown"}]),
            "unknown_type",
        ),
        (
            "runtime_field",
            parse_definition,
            document([{**task, "task_id": marker}]),
            "forbidden_field",
        ),
        (
            "reserved_parameter",
            parse_definition,
            document(user_parameters=[{"name": "TAKLER_PASS", "value": marker}]),
            "forbidden_field",
        ),
        (
            "duplicate_siblings",
            parse_definition,
            document([task, task]),
            "duplicate_name",
        ),
        (
            "task_with_children",
            parse_definition,
            document([{**task, "children": [{**task, "name": "nested"}]}]),
            "invalid_structure",
        ),
        (
            "duplicate_json_key",
            parse_definition,
            '{"kind":"takler.definition","kind":"takler.definition"}',
            "invalid_document",
        ),
        (
            "bad_trigger",
            build_definition,
            document([{**task, "trigger": "./other =="}]),
            "expression_syntax",
        ),
        (
            "missing_trigger_target",
            build_definition,
            document([{**task, "trigger": "./missing == complete"}]),
            "unresolved_reference",
        ),
        (
            "excess_tokens",
            build_definition,
            document(
                [{**task, "in_limits": [{"limit_name": "slots", "tokens": 2}]}],
                limits=[{"name": "slots", "limit": 1}],
            ),
            "invalid_reference",
        ),
    )
    fixtures = evidence.directory / "invalid"
    fixtures.mkdir()
    results = {}
    for label, api, source, expected in cases:
        text = source if isinstance(source, str) else json.dumps(source, indent=2)
        path = fixtures / f"{label}.json"
        path.write_text(text + "\n")
        if api is build_definition:
            # Structurally valid fixtures must reach the semantic builder.
            parse_definition(path.read_bytes())
        try:
            api(path.read_bytes())
        except DefinitionError as exc:
            require(
                exc.code == expected, f"{label}: expected {expected}, got {exc.code}"
            )
            require(
                exc.document_path.startswith("$") and marker not in str(exc),
                f"{label}: unsafe or invalid diagnostic",
            )
            results[label] = {
                "api": api.__name__,
                "code": exc.code,
                "document_path": exc.document_path,
            }
        else:
            require(False, f"{label}: invalid definition was accepted")
    evidence.record_checks(rejected=len(results), diagnostics=results)
