from takler.core import Flow

from takler_test_suite.harness.online import snapshot, write_definition
from takler_test_suite.harness.runtime import require
from takler_test_suite.harness.security import (
    assert_no_secrets,
    denied,
    secure,
    without_config,
)


def prepare(evidence, service):
    secure(evidence, service)


def run(evidence, service):
    flow = Flow("authorized")
    flow.add_task("work")
    service.client("load", str(write_definition(evidence, flow)))
    before = snapshot(evidence, service, "before")
    missing = without_config(service)
    wrong = {
        **service.env,
        "TAKLER_SECRET_FILE": str(service.directory / "security/wrong.secret"),
    }
    results = {}
    for name, env in (("missing", missing), ("wrong", wrong)):
        require(
            service.client("ping", env=env).returncode == 0,
            "Public ping required operator credentials",
        )
        results[name + "_query"] = denied(
            evidence,
            service.client(
                "snapshot", "--scope", "/authorized", env=env, allow_failure=True
            ),
            name + "-query",
        )
        results[name + "_control"] = denied(
            evidence,
            service.client("suspend", "/authorized/work", env=env, allow_failure=True),
            name + "-control",
        )
    whitelist = service.directory / "security/whitelist"
    original = whitelist.read_text()
    try:
        whitelist.write_text("__scn07_unlisted_user__\n")
        results["unlisted_user"] = denied(
            evidence,
            service.client("suspend", "/authorized/work", allow_failure=True),
            "unlisted-user",
        )
    finally:
        whitelist.write_text(original)
    after = snapshot(evidence, service, "after-denials")
    require(after == before, "Denied requests changed the tree")
    service.client("suspend", "/authorized/work")
    accepted = snapshot(evidence, service, "valid-control")
    require(
        accepted["/authorized/work"]["summary"]["suspended"],
        "Valid operator control failed",
    )
    audit = (service.directory / "audit.jsonl").read_text()
    assert_no_secrets(evidence, audit, (service.directory / "server.log").read_text())
    evidence.record_checks(
        public_ping=True,
        valid_operator_query=True,
        valid_operator_control=True,
        denials=results,
        denied_tree_unchanged=True,
    )
