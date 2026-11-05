"""Per-run credentials and certificates, kept outside public evidence."""

import getpass
import json
import secrets
import subprocess

from .runtime import require


def private_secret(evidence, label):
    path = evidence.directory / "private-secrets.json"
    values = json.loads(path.read_text()) if path.exists() else {}
    value = secrets.token_urlsafe(32)
    values[label] = value
    path.write_text(json.dumps(values) + "\n")
    path.chmod(0o600)
    return value


def secure(evidence, service, *, tls=False):
    directory = service.directory / "security"
    directory.mkdir()
    secret = directory / "operator.secret"
    secret.write_text(private_secret(evidence, "operator") + "\n")
    secret.chmod(0o600)
    wrong = directory / "wrong.secret"
    wrong.write_text(private_secret(evidence, "wrong_operator") + "\n")
    wrong.chmod(0o600)
    whitelist = directory / "whitelist"
    whitelist.write_text(getpass.getuser() + "\n")
    service.security.update(
        auth_mode="enabled",
        operator_secret_file=str(secret),
        operator_whitelist_file=str(whitelist),
        zombie_policy="fail",
        audit_file=str(service.directory / "audit.jsonl"),
    )
    service.env["TAKLER_SECRET_FILE"] = str(secret)
    if tls:

        def openssl(*arguments):
            result = subprocess.run(
                ["openssl", *arguments],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=10,
            )
            require(result.returncode == 0, "Cannot generate this run's TLS fixture")

        for name in ("ca", "untrusted"):
            openssl(
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                f"/CN=scn07-{name}",
                "-keyout",
                f"{name}.key",
                "-out",
                f"{name}.crt",
            )
        openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=scn07-cert.invalid",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
        )
        (directory / "extensions").write_text(
            "subjectAltName="
            + (
                "IP:127.0.0.1"
                if service.client_kind == "python" and service.transport == "http"
                else "DNS:localhost"
            )
            + "\nextendedKeyUsage=serverAuth\n"
        )
        openssl(
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            "extensions",
            "-out",
            "server.crt",
        )
        for path in directory.glob("*.key"):
            path.chmod(0o600)
        service.security.update(
            server_cert_file=str(directory / "server.crt"),
            server_key_file=str(directory / "server.key"),
            ca_file=str(directory / "ca.crt"),
            server_name="localhost",
        )
        service.env.update(
            TAKLER_TLS_CA_FILE=str(directory / "ca.crt"),
            TAKLER_TLS_SERVER_NAME="localhost",
        )
        if service.client_kind == "python" and service.transport == "http":
            # This released HTTP client verifies the URL host and explicitly
            # documents that a certificate-name override is unsupported.
            service.security.pop("server_name")
            service.env.pop("TAKLER_TLS_SERVER_NAME")


def without_config(service):
    return {
        key: value
        for key, value in service.env.items()
        if key
        not in {
            "TAKLER_CONNECT_FILE",
            "TAKLER_SECRET_FILE",
            "TAKLER_TLS_CA_FILE",
            "TAKLER_TLS_SERVER_NAME",
            "TAKLER_PASS",
        }
    }


def denied(evidence, result, label, *, connection=False):
    evidence.write_json(
        f"{label}.json",
        {
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    )
    expected = (1, 4) if connection else (1, 3)
    require(
        result.returncode in expected,
        f"{label}: expected exit {expected}, got {result.returncode}",
    )
    if not connection:
        require(
            any(
                word in result.stderr.lower()
                for word in (
                    "permission",
                    "authentication",
                    "unauth",
                    "denied",
                    "zombie",
                    "attempt_mismatch",
                )
            ),
            f"{label}: missing classified denial",
        )
    if connection:
        require(bool(result.stderr.strip()), f"{label}: missing connection diagnostic")
    return {"exit_code": result.returncode, "rejected": True}


def assert_no_secrets(evidence, *texts):
    path = evidence.directory / "private-secrets.json"
    values = json.loads(path.read_text()).values() if path.exists() else []
    require(
        all(value not in text for value in values for text in texts),
        "A generated secret leaked into a public response",
    )
