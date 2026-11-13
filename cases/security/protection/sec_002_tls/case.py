from takler_test_suite.harness.runtime import require
from takler_test_suite.harness.security import (
    assert_no_secrets,
    denied,
    secure,
    without_config,
)


def prepare(evidence, service):
    secure(evidence, service, tls=True)


def run(evidence, service):
    require(service.client("ping").returncode == 0, "Trusted TLS connection failed")
    service.client("server-status")
    configurations = {
        "wrong-ca": {
            **service.env,
            "TAKLER_TLS_CA_FILE": str(service.directory / "security/untrusted.crt"),
        },
        "wrong-name": {
            **service.env,
            "TAKLER_TLS_SERVER_NAME": "not-localhost.invalid",
        },
        "plaintext": without_config(service),
    }
    results = {}
    for name, env in configurations.items():
        address = None
        if (
            name == "wrong-name"
            and service.client_kind == "python"
            and service.transport == "http"
        ):
            address = ("--host", "localhost", "--port", str(service.port))
            env = {**service.env}
        result = service.client("ping", env=env, address=address, allow_failure=True)
        results[name] = denied(evidence, result, name, connection=True)
        assert_no_secrets(evidence, result.stdout, result.stderr)
    require(
        service.client("ping").returncode == 0,
        "Failed TLS probe broke the valid connection",
    )
    evidence.record_checks(
        trusted_ca_and_name=True,
        failures=results,
        recovered_valid_connection=True,
        certificate_checks_enabled=True,
        name_check="URL host"
        if service.client_kind == "python" and service.transport == "http"
        else "explicit TLS server name",
    )
