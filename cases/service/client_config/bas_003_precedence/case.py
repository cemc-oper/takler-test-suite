import json

from takler.core import Flow

from harness.online import rejected, write_definition
from harness.runtime import LocalService, require


def run(evidence, service):
    peer = LocalService(evidence, name="peer")
    try:
        peer.start()
        for instance, name in ((service, "address_a"), (peer, "address_b")):
            definition = write_definition(evidence, Flow(name), f"{name}.json")
            instance.client("load", str(definition))
        original = service.env.copy()

        def target(env, address=()):
            output = service.client(
                "snapshot",
                "--scope",
                "/",
                "--fields",
                "summary",
                env=env,
                address=address,
            ).stdout
            paths = set(json.loads(output)["nodes_by_path"])
            if paths == {"/", "/address_a"}:
                return "A"
            if paths == {"/", "/address_b"}:
                return "B"
            require(False, "Client reached an unexpected tree")

        env_only = {
            **service.env,
            "TAKLER_HOST": "127.0.0.1",
            "TAKLER_PORT": str(peer.port),
        }
        env_only.pop("TAKLER_CONNECT_FILE")
        require(target(env_only) == "B", "Address environment did not select B")

        config_over_env = {
            **env_only,
            "TAKLER_HOST": "not-a-real-host.invalid",
            "TAKLER_CONNECT_FILE": str(service.config),
            "TAKLER_TRANSPORT": "http" if service.transport == "grpc" else "grpc",
        }
        require(
            target(config_over_env) == "A", "Connect file did not override environment"
        )
        require(
            target(config_over_env, peer.address) == "B",
            "CLI address did not override connect file",
        )
        missing = {
            **service.env,
            "TAKLER_CONNECT_FILE": str(evidence.directory / "absent.yaml"),
        }
        failure = service.client("ping", env=missing, address=(), allow_failure=True)
        missing_result = rejected(failure, "FileNotFoundError", 3)
        require(
            service.env == original and target(service.env) == "A",
            "Per-call settings leaked",
        )
        evidence.record_checks(
            transport=service.transport,
            environment_target="B",
            config_over_environment="A",
            cli_over_config="B",
            config_transport_over_environment=True,
            missing_config=missing_result,
            subsequent_default_target="A",
        )
    finally:
        peer.stop()
