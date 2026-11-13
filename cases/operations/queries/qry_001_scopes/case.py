from takler.core import Flow

from takler_test_suite.harness.online import snapshot, write_definition
from takler_test_suite.harness.probe import Probe
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    flow = Flow("catalog")
    small = flow.add_container("small")
    for name in ("alpha", "beta"):
        small.add_task(name)
    bulk = flow.add_container("bulk")
    for index in range(2100):
        bulk.add_task(f"item{index:04}")
    service.client("load", str(write_definition(evidence, flow)))
    service.client("suspend", "/catalog")
    service.client("begin", "catalog")
    expected = {
        "/catalog",
        "/catalog/small",
        "/catalog/small/alpha",
        "/catalog/small/beta",
        "/catalog/bulk",
    } | {f"/catalog/bulk/item{i:04}" for i in range(2100)}
    with Probe(evidence, service) as probe:
        all_nodes = probe.ok("summary", scope="/catalog")
        require(
            set(all_nodes["paths"]) == expected, "Paged summary differs from definition"
        )
        first_page = probe.ok(
            "raw",
            document={
                "kind": "page_request",
                "schema_version": 1,
                "scope_path": "/catalog",
                "field_groups": ["summary"],
            },
        )
        require(
            first_page["next_cursor"] and not first_page["complete"],
            "Summary did not exercise multiple pages",
        )
        depth_zero = probe.ok("summary", scope="/catalog", depth=0)
        require(depth_zero["paths"] == ["/catalog"], "depth=0 returned descendants")
        selected = probe.ok("summary", scope="/catalog/small", depth=1)
        require(
            set(selected["paths"])
            == {"/catalog/small", "/catalog/small/alpha", "/catalog/small/beta"},
            "Subtree scope/depth differs",
        )
        before = snapshot(evidence, service, "before-detail", scope="/catalog/small")
        detail = probe.ok(
            "detail", path="/catalog/small/alpha", groups=["parameters", "runtime"]
        )
        after = snapshot(evidence, service, "after-detail", scope="/catalog/small")
        require(before == after, "Read-only detail changed application state")
        service.client("force", "complete", "/catalog/small/alpha")
        live = probe.ok("detail", path="/catalog/small/alpha", groups=["runtime"])
        require(
            live["sample_mode"] == "live" and live["as_of"] > selected["as_of"],
            "Detail did not declare a separate live sample",
        )
        require(
            selected["nodes"]["/catalog/small/alpha"]["status"] == "queued",
            "Old summary changed in place",
        )
        current = probe.ok("summary", scope="/catalog/small", depth=1)
        require(
            current["nodes"]["/catalog/small/alpha"]["status"] == "complete",
            "Fresh summary did not reflect control mutation",
        )
        evidence.write_json(
            "query-contract.json",
            {
                "expected_paths": sorted(expected),
                "summary": all_nodes,
                "first_page": first_page,
                "depth_zero": depth_zero,
                "selected": selected,
                "detail": detail,
                "live_after_mutation": live,
                "current": current,
            },
        )
    output = service.client("show", "--scope", "/catalog", "--depth", "0")
    require(
        "alpha" not in output.stdout and "item0000" not in output.stdout,
        "CLI ignored depth",
    )
    evidence.write_json(
        "cli-depth.json", {"stdout": output.stdout, "stderr": output.stderr}
    )
    evidence.record_checks(
        expected_nodes=len(expected),
        paged_summary=True,
        subtree_and_depth=True,
        detail_read_only=True,
        summary_fixed_sample=True,
        detail_live_sample=True,
    )
