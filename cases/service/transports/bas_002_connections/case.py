import json
import socket

from harness.online import rejected
from harness.runtime import require


def run(evidence, service):
    status = json.loads(service.client("server-status").stdout)
    require(status["status"] == "running", "Expected reachable service")
    # Reserve an unused port without accepting connections.
    with socket.socket(socket.AF_INET6) as reserved:
        reserved.bind(("::", 0))
        wrong_port = reserved.getsockname()[1]
        wrong = service.client(
            "ping",
            address=("--host", "127.0.0.1", "--port", str(wrong_port)),
            allow_failure=True,
        )
        wrong_result = rejected(wrong, "ClientConnectionError", 4)
    service.stop()
    lost = service.client("ping", allow_failure=True)
    lost_result = rejected(lost, "ClientConnectionError", 4)
    evidence.record_checks(
        transport=service.transport,
        reachable=True,
        wrong_port=wrong_result,
        after_stop=lost_result,
        retry_window=0,
    )
