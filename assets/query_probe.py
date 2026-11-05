"""Public Python query client, controlled via JSON lines by scenario cases."""

import argparse
import dataclasses
import json
import os
import sys

import httpx

from takler.client import TaklerServiceClient
from takler.client.credentials import current_user_name, read_first_secret
from takler.client.full_snapshot import FullSnapshotComplete
from takler.protocol.query_v1 import decode_query_document
from takler.server.protocol import takler_pb2, takler_pb2_grpc

parser = argparse.ArgumentParser()
parser.add_argument("--host")
parser.add_argument("--port")
parser.add_argument("--transport")
args = parser.parse_args()
client = TaklerServiceClient(
    args.host,
    args.port,
    transport_name=args.transport,
    retry_window=0,
    single_timeout=6,
)
client.start()
view = iterator = None


def summary(value):
    return {
        "session_id": value.session_id,
        "query_id": value.query_id,
        "snapshot_id": value.snapshot_id,
        "as_of": value.as_of,
        "expires_at": value.expires_at,
        "revision": value.revision,
        "stale_scopes": list(value.stale_scopes),
        "paths": list(value.ordered_paths),
        "nodes": {
            path: dataclasses.asdict(value.get(path)) for path in value.ordered_paths
        },
    }


def raw(document):
    metadata = [("takler-user", current_user_name() or "")]
    secret = read_first_secret(os.environ.get("TAKLER_SECRET_FILE"))
    if secret:
        metadata.append(("takler-secret", secret))
    kind = document["kind"]
    routes = {
        "capabilities_request": ("GetCapabilities", "capabilities"),
        "page_request": ("ReadPage", "pages"),
        "detail_request": ("ReadDetail", "details"),
        "chunk_request": ("ReadDetailChunk", "chunks"),
        "since_request": ("ReadSince", "since"),
        "full_snapshot_request": ("ReadFullSnapshot", "full-snapshots"),
        "full_snapshot_chunk_request": (
            "ReadFullSnapshotChunk",
            "full-snapshot-chunks",
        ),
        "cancel_full_snapshot_request": ("CancelFullSnapshot", "full-snapshot-cancel"),
    }
    rpc, route = routes[kind]
    payload = json.dumps(document).encode()
    if args.transport == "grpc":
        stub = takler_pb2_grpc.TaklerQueryStub(client.channel)
        reply = getattr(stub, rpc)(
            takler_pb2.QueryDocumentPayload(json=payload), timeout=6, metadata=metadata
        )
        content = reply.json
    else:
        if os.environ.get("TAKLER_TLS_CA_FILE"):
            raise ValueError(
                "unchecked HTTP probe is used only for plaintext query cases"
            )
        result = httpx.post(
            f"http://{args.host}:{args.port}/v1/query/{route}",
            content=payload,
            headers={**dict(metadata), "Content-Type": "application/json"},
            timeout=6,
            trust_env=False,
        )
        result.raise_for_status()
        content = result.content
    return decode_query_document(content).model_dump(mode="json")


try:
    for line in sys.stdin:
        request = json.loads(line)
        operation = request["operation"]
        try:
            if operation == "summary":
                view = client.run_query_snapshot(
                    scope_path=request.get("scope", "/"), depth=request.get("depth")
                )
                value = summary(view)
            elif operation == "since":
                view = client.run_query_since(view)
                value = summary(view)
            elif operation == "detail":
                value = client.run_query_detail(
                    view, request["path"], request["groups"]
                ).model_dump(mode="json")
            elif operation == "raw":
                value = raw(request["document"])
            elif operation == "reconnect":
                client.close_channel()
                client.start()
                value = {"reconnected": True}
            elif operation == "full_start":
                iterator = client.run_iter_full_snapshot(
                    scope_path=request.get("scope", "/"),
                    field_groups=request.get("groups"),
                )
                value = {"node": next(iterator)}
            elif operation == "full_next":
                nodes = []
                completion = None
                for _ in range(request.get("count", 50)):
                    item = next(iterator)
                    if isinstance(item, FullSnapshotComplete):
                        completion = dataclasses.asdict(item)
                        break
                    nodes.append(item)
                value = {"nodes": nodes, "completion": completion}
            elif operation == "full_cancel":
                iterator.close()
                iterator = None
                value = {"cancelled": True}
            elif operation == "close":
                value = {"closed": True}
            else:
                raise ValueError("Unknown query probe operation")
            response = {"ok": True, "value": value}
        except Exception as exc:
            response = {
                "ok": False,
                "code": getattr(exc, "code", type(exc).__name__),
                "error": str(exc),
            }
        print(json.dumps(response, ensure_ascii=False), flush=True)
        if operation == "close":
            break
finally:
    if iterator is not None:
        iterator.close()
    client.close_channel()
