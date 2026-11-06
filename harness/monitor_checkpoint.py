"""Synchronized refreshes inside externally observed periodic checkpoint writes."""

from .runtime import require, wait_for


def classify_wave(rows, planned, checkpoint, suspended):
    def inside(begin, end):
        return (
            checkpoint is not None
            and checkpoint["matched_temp_rename"]
            and begin is not None
            and end is not None
            and checkpoint["start_upper_ns"]
            <= begin
            <= end
            <= checkpoint["end_lower_ns"]
        )

    by_window = {row["window"]: row for row in rows}
    windows = []
    for index in range(planned):
        row = by_window.get(index, {})
        reads = [
            {
                "start_ns": rpc["start_ns"],
                "end_ns": rpc["end_ns"],
                "inside": inside(rpc["start_ns"], rpc["end_ns"]),
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
                "logical_inside": inside(row.get("start_ns"), row.get("end_ns")),
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
    resources = workload.resources
    planned = len(group.windows)
    run = {
        "requested": requested,
        "maximum_attempts": requested + 2,
        "qualified": 0,
        "attempts": [],
    }

    def observe(description, predicate):
        def sample():
            workload.check()
            return predicate()

        return wait_for(
            description,
            sample,
            timeout=2 * workload.cfg["checkpoint_seconds"] + 5,
            interval=0.01,
        )

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

            def next_start():
                start = resources.latest_checkpoint_start
                return start if start and start["start_lower_ns"] >= ack else None

            start = observe("new periodic checkpoint temp CREATE", next_start)
            attempt["observed_create"] = start
            group.wave(phase, "refresh", suspended, receipt_ns=ack)
            checkpoint = observe(
                "publication of the observed checkpoint temp",
                lambda: next(
                    (
                        w
                        for w in resources.windows
                        if w.get("temp_name") == start["temp_name"]
                        and w["start_upper_ns"] == start["start_upper_ns"]
                    ),
                    None,
                ),
            )
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
