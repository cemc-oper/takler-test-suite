import base64
import hashlib
import json

from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.online import write_definition
from takler_test_suite.harness.probe import Probe
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs, Probe(evidence, service) as probe:
        flow = jobs.flow("frozen")
        flow.add_limit("work", 1)
        jobs.task(flow, "worker").add_in_limit("work")
        bulk = flow.add_container("bulk")
        blob = "SCN07_WIDE_" + "x" * 90000
        for index in range(300):
            task = bulk.add_task(f"item{index:04}")
            task.add_parameter("WIDE", blob if index == 299 else "y" * 2048)
        jobs.load(flow, begin=False)
        service.client("suspend", "/frozen/bulk")
        service.client("begin", "frozen")
        worker = "/frozen/worker"
        active = jobs.phase("active-before-freeze", {worker: "active"})
        first = probe.ok(
            "full_start",
            scope="/frozen",
            groups=["summary", "parameters", "runtime", "artifacts"],
        )
        records = [first["node"]]
        service.client("server-halt")
        expected = {jobs.action(active, worker, "quiet"): "quiet"}
        service.client("force", "complete", worker)
        changed_paths = [f"/frozen/bulk/item{i:04}" for i in range(30)]
        service.client("suspend", *changed_paths)
        while True:
            page = probe.ok("full_next", count=50)
            records.extend(page["nodes"])
            if page["completion"]:
                completion = page["completion"]
                break
        frozen = {node["path"]: node for node in records}
        require(
            len(records) == len(frozen) == completion["total_nodes"] == 303,
            "Full snapshot omitted or duplicated nodes",
        )
        require(
            frozen[worker]["summary"]["status"] == "active",
            "Snapshot mixed in later completion",
        )
        require(
            all(not frozen[path]["summary"]["suspended"] for path in changed_paths),
            "Snapshot mixed in later suspend flags",
        )
        values = {
            p["name"]: p["value"]
            for p in frozen["/frozen/bulk/item0299"]["groups"]["parameters"]["user"]
        }
        require(
            values["WIDE"] == blob, "Wide field chunks were not reassembled exactly"
        )
        current = json.loads(
            service.client(
                "snapshot", "--scope", "/frozen", "--fields", "summary,runtime"
            ).stdout
        )
        require(
            current["nodes_by_path"][worker]["summary"]["status"] == "complete"
            and all(
                current["nodes_by_path"][p]["summary"]["suspended"]
                for p in changed_paths
            ),
            "Reference mutation did not take effect",
        )
        require(
            current["completion"]["state_version"] > completion["state_version"],
            "Snapshot did not retain an earlier version",
        )
        evidence.write_json(
            "frozen-snapshot.json", {"completion": completion, "nodes_by_path": frozen}
        )
        evidence.write_json("after-freeze.json", current)
        partial = probe.ok(
            "full_start", scope="/frozen", groups=["summary", "parameters"]
        )
        cancelled = probe.ok("full_cancel")
        require(
            cancelled["cancelled"] and "completion" not in partial,
            "Cancelled read claimed completion",
        )
        evidence.write_json(
            "cancelled-read.json",
            {"partial": partial, "cancelled": cancelled, "complete": False},
        )
        selection = {
            "kind": "full_snapshot_request",
            "schema_version": 1,
            "scope_path": "/frozen",
            "field_groups": ["summary", "parameters"],
        }
        leased = probe.ok("raw", document=selection)
        require(
            leased["next_cursor"] and not leased["complete"],
            "Continuation failure needs a real multi-page lease",
        )
        probe.ok(
            "raw",
            document={
                "kind": "cancel_full_snapshot_request",
                "schema_version": 1,
                "session_id": leased["session_id"],
                "full_snapshot_id": leased["full_snapshot_id"],
            },
        )
        failed_page = probe.ok(
            "raw",
            document={
                **selection,
                "full_snapshot_id": leased["full_snapshot_id"],
                "cursor": leased["next_cursor"],
            },
        )
        require(
            failed_page["kind"] == "error" and not failed_page.get("complete"),
            "Cancelled continuation falsely completed",
        )
        wide_page = probe.ok(
            "raw", document={**selection, "scope_path": "/frozen/bulk/item0299"}
        )
        refs = wide_page["nodes"][0][5]
        require(len(refs) == 1, "Expected one actual wide-field blob")
        chunk_request = {
            "kind": "full_snapshot_chunk_request",
            "schema_version": 1,
            "session_id": wide_page["session_id"],
            "full_snapshot_id": wide_page["full_snapshot_id"],
            "blob_id": refs[0]["blob_id"],
            "offset": 0,
        }
        first_chunk = probe.ok("raw", document=chunk_request)
        offset = len(base64.b64decode(first_chunk["data_base64"]))
        require(
            0 < offset < first_chunk["total_bytes"] and not first_chunk["complete"],
            "Wide-field negative check did not split into chunks",
        )
        probe.ok(
            "raw",
            document={
                "kind": "cancel_full_snapshot_request",
                "schema_version": 1,
                "session_id": wide_page["session_id"],
                "full_snapshot_id": wide_page["full_snapshot_id"],
            },
        )
        failed_chunk = probe.ok("raw", document={**chunk_request, "offset": offset})
        require(
            failed_chunk["kind"] == "error" and not failed_chunk.get("complete"),
            "Cancelled chunk continuation falsely completed",
        )
        evidence.write_json(
            "continuation-failures.json",
            {
                "cancelled_page": failed_page,
                "wide_page_descriptor": wide_page,
                "first_chunk": first_chunk,
                "cancelled_chunk": failed_chunk,
                "assembled_complete": False,
            },
        )
        streamed = service.client(
            "snapshot", "--scope", "/frozen", "--fields", "summary,runtime", "--stream"
        )
        lines = [json.loads(line) for line in streamed.stdout.splitlines()]
        require(
            "complete" in lines[-1]
            and len(lines) == 304
            and all("node" in row for row in lines[:-1]),
            "Native CLI stream lacks its final completion proof",
        )
        evidence.write_json("cli-stream.json", lines)
        oversized = jobs.flow("oversized")
        for index in range(4100):
            oversized.add_task(f"item{index:04}")
        service.client(
            "load", str(write_definition(evidence, oversized, "oversized.json"))
        )
        rejected = service.client(
            "show", "--scope", "/oversized", "--show-all", allow_failure=True
        )
        require(
            rejected.returncode != 0
            and "resource_exhausted" in rejected.stderr
            and not rejected.stdout.strip(),
            "Oversized detail read was presented as complete",
        )
        evidence.write_json(
            "budget-rejection.json",
            {
                "exit_code": rejected.returncode,
                "stdout": rejected.stdout,
                "stderr": rejected.stderr,
            },
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=1),
            fixed_state_version=completion["state_version"],
            later_state_version=current["completion"]["state_version"],
            full_nodes=len(frozen),
            wide_field_bytes=len(blob),
            wide_field_sha256=hashlib.sha256(blob.encode()).hexdigest(),
            cancelled_without_completion=True,
            page_continuation_failure=True,
            chunk_continuation_failure=True,
            native_stream_completion=True,
            detail_budget_rejected=True,
        )
