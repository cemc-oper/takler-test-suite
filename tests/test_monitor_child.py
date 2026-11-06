import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys

import grpc
import pytest

from takler.server.protocol import takler_pb2, takler_pb2_grpc


@pytest.mark.parametrize(
    "status", [grpc.StatusCode.OK, grpc.StatusCode.PERMISSION_DENIED]
)
def test_real_cli_rpc_receipt(tmp_path, status):
    secret = "only-the-receiver-should-see-this"
    server = grpc.server(ThreadPoolExecutor(max_workers=1))

    class Receiver(takler_pb2_grpc.TaklerServerServicer):
        def RunCommandInit(self, request, context):
            if status != grpc.StatusCode.OK:
                context.abort(status, secret)
            return takler_pb2.ServiceResponse(message=secret)

    takler_pb2_grpc.add_TaklerServerServicer_to_server(Receiver(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    output = tmp_path / "receipts.jsonl"
    script = Path(__file__).resolve().parents[1] / "assets/monitor_child.py"
    env = {k: v for k, v in os.environ.items() if not k.startswith("TAKLER_")}
    env.pop("NO_TAKLER", None)
    env.update(
        TAKLER_NAME="/monitor/worker0",
        TAKLER_ATTEMPT_ID="9640dea6-03c6-4fb0-b56a-0ec9fb277211",
        TAKLER_PASS=secret,
        TAKLER_TIMEOUT="0",
        no_grpc_proxy="127.0.0.1,localhost",
    )
    try:
        result = subprocess.run(
            [
                sys.executable,
                script,
                output,
                Path(sys.executable).with_name("takler-client-py"),
                "init",
                "--task-id",
                "42",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            env=env,
            capture_output=True,
            timeout=8,
        )
    finally:
        server.stop(0).wait()
    assert (result.returncode == 0) == (status == grpc.StatusCode.OK)
    raw = output.read_text()
    assert secret not in raw
    receipt = json.loads(raw)
    assert receipt["returncode"] == result.returncode
    (rpc,) = receipt["rpcs"]
    assert set(rpc) == {"method", "start_ns", "end_ns", "code"}
    assert rpc["method"] == "RunCommandInit" and rpc["code"] == status.name
    assert receipt["start_ns"] <= rpc["start_ns"] < rpc["end_ns"] <= receipt["end_ns"]
