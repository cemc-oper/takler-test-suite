import json
import stat

from harness.online import snapshot
from harness.runtime import require


def run(evidence, service):
    config = json.loads(service.config.read_text())
    require(
        config["server"]["transport"] == service.transport, "Wrong configured transport"
    )
    require(
        config["server"]["address"]["port"] == str(service.grpc_port)
        and config["checkpoint"] == {"file": str(service.checkpoint), "interval": 600},
        "Wrong isolated service config",
    )
    status = json.loads(service.client("server-status").stdout)
    require(
        status["status"] == "running"
        and status["halt_causes"] == []
        and status["last_checkpoint_at"] is None
        and status["restore_summary"] is None,
        "Fresh service has unexpected status",
    )
    nodes = snapshot(evidence, service, "fresh")
    require(set(nodes) == {"/"}, "Fresh service is not empty")
    parameters = {
        item["name"]: item["value"]
        for item in nodes["/"]["groups"]["parameters"]["generated"]
    }
    require(
        parameters["TAKLER_HOST"] == "127.0.0.1"
        and str(parameters["TAKLER_PORT"]) == str(service.grpc_port),
        "Advertised address differs from the configured service",
    )
    service.stop()
    saved = json.loads(service.checkpoint.read_bytes())
    require(
        stat.S_IMODE(service.checkpoint.stat().st_mode) == 0o600
        and saved["format_version"] == 3
        and set(saved["runtime"]["nodes"]) == {"/"},
        "Final checkpoint differs from the empty service",
    )
    evidence.record_checks(
        transport=service.transport,
        configured_ports=service.ports,
        checkpoint_version=3,
        checkpoint_mode="0600",
        startup_status="running",
        listener_owned=True,
        stopped_cleanly=True,
    )
