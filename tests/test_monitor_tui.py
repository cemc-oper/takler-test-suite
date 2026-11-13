"""The TUI evidence must identify the selected path and the visible state."""

import pytest

from harness.monitoring.tui import TuiWindow, _screen_observation
from harness.runtime import RunError


@pytest.mark.parametrize(
    "path,state,suspended,seen",
    [
        ("/monitor/marker", "suspend (queued)", True, True),
        ("/monitor/marker", "queued", False, True),
        ("/monitor/marker", "queued", True, False),
        ("/monitor/marker", "suspend (queued)", False, False),
        ("/monitor/marker-extra", "queued", False, False),
    ],
)
def test_read_visible_info_rows(path, state, suspended, seen):
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg">
        <text y="1">Path:&#160;</text><text y="1">{path}</text>
        <text y="2">State:&#160;</text><text y="2">■</text><text y="2">■ {state}</text>
    </svg>"""
    assert _screen_observation(svg, "/monitor/marker", "queued", suspended) is seen


@pytest.mark.parametrize("crash", [False, True])
def test_cleanup_reaps_and_reports_child(tmp_path, crash):
    class Evidence:
        report = {}
        events = []

        def record(self, event, **values):
            self.events.append((event, values))

    evidence = Evidence()
    # The prepared child waits for the wave; cleanup needs no server or credentials.
    window = TuiWindow(
        1, tmp_path / "unused", "/monitor", "/monitor/marker", evidence=evidence
    )
    try:
        if crash:
            window.process.kill()
            window.process.wait(timeout=5)
            with pytest.raises(RunError, match="did not shut down cleanly"):
                window.close()
        else:
            window.close()
        outcome = evidence.report["monitor_tui_cleanup"]
        assert evidence.events == [("monitor_tui_cleanup", outcome)]
        assert outcome["reaped"] and outcome["ipc_closed"]
        assert outcome["quit_sent"] is not crash
        assert (outcome["returncode"] == 0) is not crash
        assert window.close() == outcome
    finally:
        if window.process.poll() is None:
            window.close()
