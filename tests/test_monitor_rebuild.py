from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from takler_test_suite.harness.monitoring.rebuild import ScopeReader, definition_paths
from takler_test_suite.harness.runtime import RunError
from takler.exceptions import QueryResponseError


def reader(scope="/cycle00", marker="/cycle00/prepare"):
    service = SimpleNamespace(port=0, capacity={"rpc_timeout_seconds": 1})
    actor = ScopeReader(
        service, "unused", scope, marker, frozenset({scope, scope + "/prepare"})
    )
    actor.wire.open = Mock()
    actor.primary = Mock()
    return actor


def view(actor, *, revision=3, suspended=False, snapshot="old"):
    nodes = {p: SimpleNamespace(suspended=suspended) for p in actor.paths}
    return SimpleNamespace(
        scope_path=actor.scope,
        nodes=nodes,
        get=nodes.get,
        stale_scopes=(),
        is_stale=lambda s: False,
        session_id="session",
        query_id="query",
        snapshot_id=snapshot,
        revision=revision,
        changed_paths=(),
        removed_paths=(),
    )


def test_scope_validation_distinguishes_matching_and_unaffected_subtree():
    root = {"name": "cycle00", "children": [{"name": "prepare", "children": []}]}
    actor = reader()
    assert set(definition_paths(root)) == actor.paths
    actor.primary.current = view(actor)
    changed = view(actor, revision=4, suspended=True)
    changed.changed_paths = (actor.marker,)
    actor.primary.read_since.return_value = changed
    assert actor.step("changed", True)["marker_suspended"]
    changed.nodes["/cycle06"] = changed.nodes[actor.marker]
    with pytest.raises(RunError, match="input definition"):
        actor.step("changed", True)
    other = reader(scope="/cycle06")
    other.primary.current = other.primary.read_since.return_value = view(other)
    assert other.step("changed", True)["marker_suspended"] is None
    other.primary.current.changed_paths = (other.scope,)
    with pytest.raises(RunError, match="unrelated"):
        other.step("changed", True)


def test_reset_requires_history_expiry_and_fresh_in_scope_snapshot():
    actor = reader()
    old = actor.primary.current = view(actor)
    old.is_stale = lambda s: s == actor.scope
    actor.primary.read_since.side_effect = QueryResponseError(
        "reset_required", "history_expired", actor.scope
    )
    result = actor.step("expired-since", False)
    assert result["reset_reason"] == "history_expired"
    actor.primary.read_since.side_effect = QueryResponseError(
        "reset_required", "session_changed", actor.scope
    )
    with pytest.raises(RunError, match="Unexpected reset"):
        actor.step("expired-since", False)
    fresh = view(actor, revision=5, snapshot="fresh")
    actor.primary.read_snapshot.return_value = fresh
    assert actor.step("rebuild", False)["snapshot_id"] == "fresh"
    fresh.snapshot_id = "old"
    with pytest.raises(RunError, match="fresh snapshot"):
        actor.step("rebuild", False)
