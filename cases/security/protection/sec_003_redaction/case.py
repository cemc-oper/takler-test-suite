import json

from takler.core import Flow

from harness.online import snapshot, write_definition
from harness.probe import Probe
from harness.runtime import require
from harness.security import assert_no_secrets, private_secret, secure


def prepare(evidence, service):
    secure(evidence, service)
    service.security["query_redacted_parameters"] = ["API_KEY"]


def run(evidence, service):
    flow = Flow("redacted")
    flow.add_parameter(
        {
            "API_KEY": private_secret(evidence, "api_key"),
            "PUBLIC_VALUE": "visible-sample",
        }
    )
    flow.add_task("work")
    other = Flow("outside")
    other.add_task("work")
    service.client(
        "load", str(write_definition(evidence, flow, "private-definition.json"))
    )
    service.client("load", str(write_definition(evidence, other, "outside.json")))
    before = snapshot(evidence, service, "before")
    public = service.client("show", "--scope", "/redacted", "--show-all")
    assert_no_secrets(evidence, public.stdout, public.stderr, json.dumps(before))
    items = {
        item["name"]: item
        for item in before["/redacted"]["groups"]["parameters"]["user"]
    }
    require(
        items["API_KEY"]["redacted"]
        and items["API_KEY"].get("value") is None
        and items["PUBLIC_VALUE"]["value"] == "visible-sample",
        "Public parameter redaction is incorrect",
    )
    with Probe(evidence, service) as probe:
        summary = probe.ok("summary", scope="/redacted")
        detail = probe.ok("detail", path="/redacted", groups=["parameters"])
        assert_no_secrets(evidence, json.dumps(detail))
        evidence.write_json("redacted-detail.json", detail)
        out_of_scope = probe.call("detail", path="/outside/work", groups=["runtime"])
        require(not out_of_scope["ok"], "Detail escaped its authorized query scope")
        invalid = probe.ok(
            "raw",
            document={
                "kind": "page_request",
                "schema_version": 1,
                "scope_path": "/redacted",
                "field_groups": ["summary"],
                "unexpected": "field",
            },
        )
        evidence.write_json("invalid-field.json", invalid)
        require(
            invalid["kind"] == "error" and invalid["code"] == "invalid_request",
            "Server accepted an unknown query field",
        )
        foreign = probe.ok(
            "raw",
            document={
                "kind": "detail_request",
                "schema_version": 1,
                "session_id": summary["session_id"],
                "query_id": summary["query_id"],
                "path": "/outside/work",
                "flow_generation": "foreign-test-generation",
                "attempt_id": None,
                "field_groups": ["runtime"],
            },
        )
        require(foreign["kind"] == "error", "Server allowed a scope escape")
        evidence.write_json(
            "query-denials.json",
            {
                "client_scope": out_of_scope,
                "unknown_field": invalid,
                "server_scope": foreign,
            },
        )
    after = snapshot(evidence, service, "after")
    require(before == after, "Invalid query changed application state")
    assert_no_secrets(
        evidence,
        (service.directory / "server.log").read_text(),
        (service.directory / "audit.jsonl").read_text(),
    )
    evidence.record_checks(
        parameter_redacted=True,
        public_parameter_visible=True,
        server_unknown_field_rejected=True,
        scope_escape_rejected=True,
        query_read_only=True,
    )
