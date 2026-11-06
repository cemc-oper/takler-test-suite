"""Hold a real summary cursor while the other authenticated windows refresh."""

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from .runtime import require, wait_for


def slow_reader_refresh(workload, group):
    peers = list(range(len(group.windows) - 1))
    slow = group.windows[-1]
    previous = slow.primary.current
    old_marker = previous.get(slow.marker).suspended
    held, release = Event(), Event()
    report = {
        "window": len(peers),
        "peers": peers,
        "previous_revision": previous.revision,
        "pinned_marker": old_marker,
        "result": "failed",
    }
    trace = slow.wire.trace
    timeout = workload.cfg["rpc_timeout_seconds"]

    def hold_continuation(request):
        if not request.get("cursor") or held.is_set():
            return
        require(
            request.get("scope_path") == slow.scope
            and len(trace.calls) == 1
            and trace.calls[0]["method"] == "ReadPage"
            and trace.calls[0]["code"] == "OK",
            "Expected the first successful subtree page before holding its cursor",
        )
        report["held_ns"] = time.monotonic_ns()
        held.set()
        require(
            release.wait(workload.cfg["stop_timeout_seconds"]),
            "Slow reader gate timed out",
        )

    trace.calls = []
    trace.before_page = hold_continuation
    try:
        report["start_ns"] = time.monotonic_ns()
        with ThreadPoolExecutor(max_workers=1) as executor:
            reading = executor.submit(slow.primary.read_snapshot, scope_path=slow.scope)

            def waiting():
                workload.check()
                if reading.done():
                    reading.result()
                    require(
                        False, "Slow reader finished without requesting a second page"
                    )
                return held.is_set()

            try:
                wait_for(
                    "slow reader's first continuation",
                    waiting,
                    timeout=timeout,
                    interval=0.01,
                )
                workload.report_meter(1)
                ack = workload.set_marker(not old_marker)
                report["receipt_ns"] = ack
                rows = group.wave(
                    "slow-peers",
                    "refresh",
                    not old_marker,
                    indices=peers,
                    receipt_ns=ack,
                )
                require(
                    all(row["marker_changed"] and not row["retries"] for row in rows),
                    "A peer missed the acknowledged change while the slow reader was held",
                )
            finally:
                report["release_ns"] = time.monotonic_ns()
                release.set()
            pinned = reading.result()
            report["view_ready_ns"] = time.monotonic_ns()
        report["snapshot_rpc"] = trace.evidence()
        pages = trace.calls
        require(
            len(pages) >= 2
            and all(
                p["method"] == "ReadPage"
                and p["code"] == "OK"
                and not p.get("query_error")
                for p in pages
            )
            and len({p["request_hash"] for p in pages}) == len(pages)
            and pages[0]["end_ns"] <= report["held_ns"] <= ack
            and all(
                ack <= row["start_ns"] <= row["end_ns"] <= report["release_ns"]
                for row in rows
            )
            and report["release_ns"] <= pages[1]["start_ns"],
            "Page continuation or a peer's first read escaped the slow reader interval",
        )
        require(
            pinned.get(slow.marker).suspended is old_marker
            and pinned.ordered_paths == previous.ordered_paths
            and pinned.revision < min(row["revision"] for row in rows),
            "Held snapshot did not preserve its complete pre-write subtree",
        )
        report.update(
            pinned_nodes=len(pinned.ordered_paths),
            pinned_revision=pinned.revision,
            pinned_snapshot_id=pinned.snapshot_id,
            pinned_query_id=pinned.query_id,
        )
        catchup = group.wave("slow-catch-up", "refresh", not old_marker, receipt_ns=ack)
        caught = catchup[-1]
        latest_peer_revision = max(row["revision"] for row in rows)
        require(
            caught["previous_revision"] == pinned.revision
            and caught["snapshot_id"] == pinned.snapshot_id
            and caught["query_id"] == pinned.query_id
            and caught["marker_changed"]
            and all(
                not row["retries"] and row["revision"] >= latest_peer_revision
                for row in catchup
            ),
            "Readers did not converge incrementally after the continuation was released",
        )
        report.update(caught_up_revision=caught["revision"], result="passed")
        return {
            "peers": len(peers),
            "pages": len(pages),
            "caught_up_revision": caught["revision"],
        }
    except Exception as exc:
        report["reason"] = str(exc)
        raise
    finally:
        trace.before_page = None
        if "snapshot_rpc" not in report:
            report["snapshot_rpc"] = trace.evidence()
        workload.evidence.write_json("slow-reader.json", report)
