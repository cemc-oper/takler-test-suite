"""Monitor waves synchronized with externally observed periodic checkpoint writes."""

import time

from .runtime import require, wait_for


def _observe(workload, description, predicate):
    def sample():
        workload.check()
        return predicate()

    return wait_for(
        description,
        sample,
        timeout=2 * workload.cfg["checkpoint_seconds"] + 5,
        interval=0.01,
    )


def _next_create(workload, after_ns):
    def next_start():
        start = workload.resources.latest_checkpoint_start
        return start if start and start["start_lower_ns"] >= after_ns else None

    return _observe(workload, "new periodic checkpoint temp CREATE", next_start)


def _publication(workload, start):
    def matching():
        return next(
            (
                event
                for event in workload.resources.windows
                if event.get("temp_name") == start["temp_name"]
                and event["start_upper_ns"] == start["start_upper_ns"]
            ),
            None,
        )

    return _observe(workload, "publication of the observed checkpoint temp", matching)


def _inside(checkpoint, begin, end):
    return (
        checkpoint is not None
        and checkpoint["matched_temp_rename"]
        and begin is not None
        and end is not None
        and checkpoint["start_upper_ns"] <= begin <= end <= checkpoint["end_lower_ns"]
    )


def checkpoint_cold_open(workload, group):
    """Open fresh readers once; a phase miss can still finish the rest of the scenario."""
    require(not group.rows, "Checkpoint cold open requires fresh readers")
    planned = len(group.windows)
    attempt = {"planned": planned, "armed_ns": time.monotonic_ns(), "qualified": False}
    filename = "checkpoint-cold-open.json"
    workload.evidence.write_json(filename, attempt)
    checkpoint = None
    try:
        start = _next_create(workload, attempt["armed_ns"])
        attempt["observed_create"] = start
        workload.evidence.write_json(filename, attempt)
        opened = group.wave("cold-open", "open", False)
        checkpoint = _publication(workload, start)
        attempt["checkpoint"] = checkpoint
        require(
            checkpoint["matched_temp_rename"] and checkpoint["bytes"] > 0,
            "Primary rename does not match the observed checkpoint temp",
        )
    except Exception as exc:
        attempt["reason"] = str(exc)
        raise
    finally:
        by_index = {row["window"]: row for row in group.rows}
        windows = []
        for index in range(planned):
            row = by_index.get(index, {})
            begin, ready = row.get("planned_ns"), row.get("summary_ready_ns")
            pages = [rpc for rpc in row.get("rpcs", []) if rpc["method"] == "ReadPage"]
            elapsed = ready - begin if ready is not None else None
            windows.append(
                {
                    "window": index,
                    "result": row.get("result", "not_sent"),
                    "planned_ns": begin,
                    "summary_ready_ns": ready,
                    "summary_elapsed_ns": elapsed,
                    "pages": len(pages),
                    "all_pages_inside": bool(pages)
                    and all(
                        _inside(checkpoint, rpc["start_ns"], rpc["end_ns"])
                        for rpc in pages
                    ),
                    "summary_inside": _inside(checkpoint, begin, ready),
                    "logical_inside": _inside(checkpoint, begin, row.get("end_ns")),
                }
            )
        attempt["windows"] = windows
        attempt["sent"] = sum(row.get("first_attempt", False) for row in group.rows)
        attempt["ok"] = sum(w["result"] == "ok" for w in windows)
        attempt["summary_within_5s"] = sum(
            w["result"] == "ok"
            and w["summary_elapsed_ns"] is not None
            and w["summary_elapsed_ns"] <= 5_000_000_000
            for w in windows
        )
        attempt["qualified"] = "reason" not in attempt and all(
            w["result"] == "ok" and w["summary_inside"] and w["all_pages_inside"]
            for w in windows
        )
        if not attempt["qualified"] and "reason" not in attempt:
            attempt["reason"] = (
                "Cold summaries or ReadPage RPCs outside the checkpoint interval"
            )
        workload.evidence.write_json(filename, attempt)
    return opened, attempt["qualified"]


def classify_wave(rows, planned, checkpoint, suspended):
    by_window = {row["window"]: row for row in rows}
    windows = []
    for index in range(planned):
        row = by_window.get(index, {})
        reads = [
            {
                "start_ns": rpc["start_ns"],
                "end_ns": rpc["end_ns"],
                "inside": _inside(checkpoint, rpc["start_ns"], rpc["end_ns"]),
            }
            for rpc in row.get("rpcs", [])
            if rpc["method"] == "ReadSince"
        ]
        windows.append(
            {
                "window": index,
                "result": row.get("result", "not_sent"),
                "start_ns": row.get("start_ns"),
                "end_ns": row.get("end_ns"),
                "logical_inside": _inside(
                    checkpoint, row.get("start_ns"), row.get("end_ns")
                ),
                "previous_revision": row.get("previous_revision"),
                "revision": row.get("revision"),
                "new_marker": row.get("marker_changed") is True
                and row.get("suspended") is suspended
                and row["revision"] > row["previous_revision"],
                "read_since": reads,
                "all_reads_inside": bool(reads) and all(r["inside"] for r in reads),
            }
        )
    return {
        "planned": planned,
        "sent": sum(w["start_ns"] is not None for w in windows),
        "ok": sum(w["result"] == "ok" for w in windows),
        "new_markers": sum(w["new_marker"] for w in windows),
        "logical_inside": sum(w["logical_inside"] for w in windows),
        "all_reads_inside": sum(w["all_reads_inside"] for w in windows),
        "windows": windows,
    }


def checkpoint_refreshes(workload, group, requested):
    evidence = workload.evidence
    planned = len(group.windows)
    run = {
        "requested": requested,
        "maximum_attempts": requested + 2,
        "qualified": 0,
        "attempts": [],
    }

    # catch-up left all readers at suspended=True. Every dispatched wave sees a change.
    for index in range(run["maximum_attempts"]):
        suspended = bool(index % 2)
        phase = f"checkpoint-sync-{index + 1}"
        attempt = {
            "phase": phase,
            "suspended": suspended,
            "qualified": False,
            "planned": planned,
        }
        run["attempts"].append(attempt)
        evidence.write_json("checkpoint-sync.json", run)
        first_row = len(group.rows)
        checkpoint = None
        try:
            workload.report_meter(1 + index % 99)
            ack = workload.set_marker(suspended)
            attempt["receipt_ns"] = ack

            start = _next_create(workload, ack)
            attempt["observed_create"] = start
            group.wave(phase, "refresh", suspended, receipt_ns=ack)
            checkpoint = _publication(workload, start)
            attempt["checkpoint"] = checkpoint
            require(
                checkpoint["matched_temp_rename"] and checkpoint["bytes"] > 0,
                "Primary rename does not match the observed checkpoint temp",
            )
        except Exception as exc:
            attempt["reason"] = str(exc)
            raise
        finally:
            attempt.update(
                classify_wave(group.rows[first_row:], planned, checkpoint, suspended)
            )
            if "reason" not in attempt:
                if attempt["ok"] != planned or attempt["new_markers"] != planned:
                    attempt["reason"] = "A reader missed the new marker"
                elif attempt["all_reads_inside"] != planned:
                    attempt["reason"] = (
                        "ReadSince missing or outside the conservative checkpoint interval"
                    )
                else:
                    attempt["qualified"] = True
                    run["qualified"] += 1
            evidence.write_json("checkpoint-sync.json", run)
        require(attempt["new_markers"] == planned, attempt.get("reason"))
        if run["qualified"] == requested:
            return {
                "requested": requested,
                "qualified": requested,
                "attempts": index + 1,
            }
    require(False, f"Only {run['qualified']}/{requested} checkpoint waves qualified")
