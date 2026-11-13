from types import SimpleNamespace

import pytest

from takler_test_suite.harness.control import batch
from takler_test_suite.harness.runtime import RunError


def test_batch_checks_order_effect_and_partial_failure():
    saved = {}
    evidence = SimpleNamespace(
        write_json=lambda name, value: saved.update({name: value})
    )
    response = SimpleNamespace(
        returncode=1,
        stdout="[0] /f/a success effect=none: success\n[1] /f/b flow_state effect=none: flow_state\n[2] /f/c success effect=applied: success\n",
        stderr="batch_failed: processed=3 succeeded=2 failed=1\n",
    )
    service = SimpleNamespace(client=lambda *args, **kwargs: response)
    expected = [
        ("/f/a", "success", "none"),
        ("/f/b", "flow_state", "none"),
        ("/f/c", "success", "applied"),
    ]
    assert batch(evidence, service, "mixed", "force", expected=expected) == expected
    assert saved["mixed.json"]["exit_code"] == 1
    with pytest.raises(RunError, match="per-item outcomes"):
        batch(
            evidence, service, "reordered", "force", expected=list(reversed(expected))
        )
    response.stdout = response.stdout.replace("[2]", "[3]")
    with pytest.raises(RunError, match="per-item outcomes"):
        batch(evidence, service, "missing-index", "force", expected=expected)
