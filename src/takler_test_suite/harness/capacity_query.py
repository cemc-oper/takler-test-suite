"""Measure public QueryDocument RPC payloads and validated client assembly."""

import dataclasses
import getpass
import json
import time

import grpc

from takler.client.full_snapshot import FullSnapshotComplete
from takler.client.query_client import QueryClient
from takler.protocol.query_v1 import decode_query_document
from takler.server.protocol import takler_pb2, takler_pb2_grpc

from .runtime import require

FULL_GROUPS = ["summary", "definition", "parameters", "runtime", "artifacts"]
ROUTES = {
    "capabilities_request": "GetCapabilities",
    "page_request": "ReadPage",
    "detail_request": "ReadDetail",
    "chunk_request": "ReadDetailChunk",
    "since_request": "ReadSince",
    "full_snapshot_request": "ReadFullSnapshot",
    "full_snapshot_chunk_request": "ReadFullSnapshotChunk",
    "cancel_full_snapshot_request": "CancelFullSnapshot",
}


class QueryWire:
    def __init__(self, evidence, service, resources, label):
        self.resources, self.timeout = (
            resources,
            service.capacity["rpc_timeout_seconds"],
        )
        self.channel = grpc.insecure_channel(
            f"127.0.0.1:{service.port}", options=[("grpc.enable_retries", 0)]
        )
        self.stub = takler_pb2_grpc.TaklerQueryStub(self.channel)
        self.stream = (evidence.directory / f"{label}.rpc.jsonl").open("a")
        self.bytes = self.rpc_ns = self.calls = 0
        self.operation = "startup"

    def query_summary(self, document, decode):
        return self.query(document, decode)

    def query(self, document, decode=None):
        self.resources.check()
        start = time.monotonic_ns()
        row = {
            "operation": self.operation,
            "kind": document.kind,
            "start_ns": start,
            "retries": 0,
        }
        try:
            reply = getattr(self.stub, ROUTES[document.kind])(
                takler_pb2.QueryDocumentPayload(
                    json=document.model_dump_json().encode()
                ),
                metadata=[("takler-user", getpass.getuser())],
                timeout=self.timeout,
            )
            returned = time.monotonic_ns()
            row.update(reply_bytes=len(reply.json), rpc_ns=returned - start)
            self.bytes += len(reply.json)
            self.rpc_ns += returned - start
            value = (
                decode_query_document(reply.json)
                if decode is None
                else decode(reply.json)
            )
            row.update(
                result="error" if value.kind == "error" else "ok", reply_kind=value.kind
            )
            return value
        except Exception as exc:
            row.update(result="failed", error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            row["end_ns"] = time.monotonic_ns()
            self.calls += 1
            self.stream.write(json.dumps(row) + "\n")
            self.stream.flush()

    def close(self):
        self.channel.close()
        self.stream.close()


class Queries:
    def __init__(self, evidence, service, resources, label):
        self.evidence, self.resources, self.label = evidence, resources, label
        self.wire = QueryWire(evidence, service, resources, label)
        self.client = QueryClient(self.wire)
        self.sequence, self.rows = 0, []
        self.output = (evidence.directory / f"{label}.queries.jsonl").open("a")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.wire.close()
        self.output.close()

    def read(self, mode, *, scope="/", validator=None, slow_seconds=0):
        self.sequence += 1
        self.wire.operation = f"{self.label}-{self.sequence}"
        start = time.monotonic_ns()
        before_bytes, before_rpc, before_calls = (
            self.wire.bytes,
            self.wire.rpc_ns,
            self.wire.calls,
        )
        row = {
            "id": self.wire.operation,
            "mode": mode,
            "scope": scope,
            "start_ns": start,
            "phase": self.resources.phase,
        }
        try:
            if mode == "summary":
                view = self.client.read_snapshot(scope_path=scope)
                if validator:
                    for path in view.ordered_paths:
                        validator(path, None)
                row.update(
                    nodes=len(view.ordered_paths),
                    as_of=view.as_of,
                    state_version=view.revision,
                )
            else:
                iterator = self.client.iter_full_snapshot(
                    scope_path=scope, field_groups=FULL_GROUPS
                )
                count, completed = 0, None
                try:
                    for item in iterator:
                        self.resources.check()
                        if isinstance(item, FullSnapshotComplete):
                            completed = dataclasses.asdict(item)
                            break
                        count += 1
                        if validator:
                            validator(item["path"], item)
                        if slow_seconds:
                            time.sleep(slow_seconds)
                            break
                finally:
                    iterator.close()
                if slow_seconds:
                    require(completed is None, "Slow cancelled read claimed completion")
                    row.update(nodes=count, cancelled=True, complete=False)
                else:
                    require(
                        completed is not None and completed["total_nodes"] == count,
                        "Full read did not complete",
                    )
                    row.update(
                        nodes=count,
                        complete=True,
                        as_of=completed["as_of"],
                        state_version=completed["state_version"],
                        logical_bytes=completed["total_bytes"],
                        sha256=completed["sha256"],
                    )
            row["result"] = "ok"
            return row
        except Exception as exc:
            row.update(result="failed", error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            row.update(
                end_ns=time.monotonic_ns(),
                reply_bytes=self.wire.bytes - before_bytes,
                rpc_ns=self.wire.rpc_ns - before_rpc,
                rpc_calls=self.wire.calls - before_calls,
            )
            row["elapsed_s"] = (row["end_ns"] - start) / 1e9
            row["assembly_validation_s"] = max(
                0, row["elapsed_s"] - row["rpc_ns"] / 1e9
            )
            self.rows.append(row)
            self.output.write(json.dumps(row) + "\n")
            self.output.flush()
