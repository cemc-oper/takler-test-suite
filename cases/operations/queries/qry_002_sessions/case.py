import time
from datetime import datetime

from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.online import write_definition
from takler_test_suite.harness.probe import Probe
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs, Probe(evidence, service) as probe:
        flow = jobs.flow("session")
        jobs.task(flow, "producer")
        jobs.task(flow, "downstream", "/session/producer == complete")
        dormant = flow.add_container("dormant")
        for index in range(2100):
            dormant.add_task(f"item{index:04}")
        jobs.load(flow, begin=False)
        service.client("suspend", "/session/dormant")
        service.client("suspend", "/session/producer")
        service.client("begin", "session")
        first = probe.ok(
            "raw",
            document={
                "kind": "page_request",
                "schema_version": 1,
                "scope_path": "/session",
                "field_groups": ["summary"],
            },
        )
        require(
            first["next_cursor"], "Need a real continuation cursor for expiry check"
        )
        initial = probe.ok("summary", scope="/session")
        unchanged = probe.ok("since")
        require(
            initial["nodes"] == unchanged["nodes"]
            and initial["revision"] == unchanged["revision"],
            "Unchanged since altered summary",
        )
        service.client("resume", "/session/producer")
        producer, downstream = "/session/producer", "/session/downstream"
        active = jobs.phase(
            "producer-active", {producer: "active", downstream: "queued"}
        )
        delta1 = probe.ok("since")
        require(
            delta1["nodes"][producer]["status"] == "active"
            and delta1["nodes"][producer]["attempt_id"]
            == active[producer]["groups"]["runtime"]["attempt_id"],
            "Sparse update lost active identity",
        )
        expected = {jobs.action(active, producer, "success"): "success"}
        active2 = jobs.phase(
            "downstream-active", {producer: "complete", downstream: "active"}
        )
        delta2 = probe.ok("since")
        require(
            delta2["nodes"][producer]["status"] == "complete"
            and delta2["nodes"][downstream]["status"] == "active",
            "Dependency transitions did not converge",
        )
        expected[jobs.action(active2, downstream, "success")] = "success"
        jobs.phase("jobs-complete", {producer: "complete", downstream: "complete"})
        converged = probe.ok("since")
        deadline = (
            datetime.fromisoformat(
                first["expires_at"].replace("Z", "+00:00")
            ).timestamp()
            + 2
        )
        polls = []
        while time.time() < deadline:
            continued = probe.ok("since")
            require(
                continued["nodes"][producer]["status"] == "complete"
                and continued["nodes"][downstream]["status"] == "complete",
                "Long session lost completed states",
            )
            polls.append(
                {
                    "at_epoch": time.time(),
                    "revision": continued["revision"],
                    "query_id": continued["query_id"],
                }
            )
            evidence.record(
                "session_lease_poll",
                revision=continued["revision"],
                original_page_expires_at=first["expires_at"],
            )
            time.sleep(min(10, max(0, deadline - time.time())))
        expired = probe.ok(
            "raw",
            document={
                "kind": "page_request",
                "schema_version": 1,
                "scope_path": "/session",
                "field_groups": ["summary"],
                "cursor": first["next_cursor"],
            },
        )
        require(
            expired["kind"] == "error" and expired["code"] == "snapshot_expired",
            "Old summary page remained valid past its retention period",
        )
        live = probe.ok("detail", path=producer, groups=["runtime"])
        require(
            live["attempt_id"] == active[producer]["groups"]["runtime"]["attempt_id"],
            "Refreshed query lease lost attempt identity",
        )
        probe.ok("reconnect")
        reconnected = probe.ok("since")
        require(
            reconnected["nodes"] == converged["nodes"],
            "Reconnect lost convergence against the same service",
        )
        service.client("server-halt")
        replacement = jobs.flow("session")
        replacement.add_task("replacement")
        service.client(
            "replace",
            "/session",
            str(write_definition(evidence, replacement, "replacement.json")),
        )
        stale = probe.call("since")
        require(
            (not stale["ok"] and stale["code"] == "reset_required")
            or (stale["ok"] and stale["value"]["stale_scopes"]),
            "Replacement was silently mixed into the old generation",
        )
        rebuilt = probe.ok("summary", scope="/session")
        require(
            set(rebuilt["paths"]) == {"/session", "/session/replacement"},
            "Fresh summary retained deleted children",
        )
        evidence.write_json(
            "session-contract.json",
            {
                "first_page": first,
                "initial": initial,
                "unchanged": unchanged,
                "active": delta1,
                "dependency": delta2,
                "converged": converged,
                "lease_polls": polls,
                "expired_page": expired,
                "live_detail": live,
                "reconnected": reconnected,
                "replacement_result": stale,
                "rebuilt": rebuilt,
                "elapsed_since_original_sample_s": time.time()
                - datetime.fromisoformat(
                    first["as_of"].replace("Z", "+00:00")
                ).timestamp(),
            },
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=1),
            unchanged_since=True,
            sparse_changes=True,
            page_expiry=True,
            scope_lease_continued=True,
            reconnect_same_service=True,
            replacement_reset_and_deletions=True,
        )
