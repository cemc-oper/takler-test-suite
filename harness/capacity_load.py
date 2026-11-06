"""Real bounded job waves, concurrent public readers, and endurance analysis."""

import json
import statistics
import threading
import time

from takler.client import TaklerServiceClient
from takler.client.grpc_transport import GrpcTransport
from takler.core import Flow

from .capacity import Resources, wait_until
from .capacity_query import Queries
from .jobs import Jobs
from .online import write_definition
from .runtime import require


class Commands:
    def __init__(self, service):
        self.client = TaklerServiceClient(
            transport=GrpcTransport(
                host="127.0.0.1",
                port=service.port,
                single_timeout=service.capacity["load_timeout_seconds"],
                retry_window=0,
            )
        )

    def __enter__(self):
        self.client.start()
        return self

    def __exit__(self, *exc):
        self.client.close_channel()

    def call(self, name, *args):
        result = getattr(self.client, "run_command_" + name)(*args)
        require(result.flag == 0, f"Capacity command failed: {name}")
        return result

    def status(self):
        return self.client.run_request_server_status().model_dump(mode="json")


def analysis(evidence, resources, rows):
    def stats(values):
        values = sorted(values)
        return (
            {
                "count": len(values),
                "p50_s": statistics.median(values),
                "p95_s": values[min(len(values) - 1, int(len(values) * 0.95))],
                "max_s": max(values),
            }
            if values
            else {"count": 0}
        )

    result = {}
    for mode in ("summary", "full", "slow"):
        selected = [r for r in rows if r["mode"] == mode]
        overlap = [
            r
            for r in selected
            if any(
                r["start_ns"] <= w["end_upper_ns"]
                and r["end_ns"] >= w["start_lower_ns"]
                for w in resources.windows
            )
        ]
        result[mode] = {
            "all_attempts": len(selected),
            "failures": sum(r["result"] != "ok" for r in selected),
            "end_to_end": stats([r["elapsed_s"] for r in selected]),
            "possible_checkpoint_io_overlap": stats([r["elapsed_s"] for r in overlap]),
            "wire_reply_bytes": sum(r["reply_bytes"] for r in selected),
            "rpc_calls": sum(r["rpc_calls"] for r in selected),
        }
    evidence.write_json(
        "query-statistics.json",
        {
            "measurements": result,
            "overlap_contract": "temp-create/primary-rename observation envelope, including inotify sampling uncertainty; not exact internal capture time",
        },
    )
    return result


def mixed(evidence, service, *, endurance=False):
    cfg = service.capacity
    duration = (
        cfg["endurance"]["duration_seconds"]
        if endurance
        else cfg["mixed"]["duration_seconds"]
    )
    count = cfg["mixed"]["concurrency"]
    done, errors, query_rows = threading.Event(), [], []
    with (
        Resources(evidence, service) as resources,
        Commands(service) as commands,
        Jobs(evidence, service, scope="/load") as jobs,
    ):
        backdrop = Flow("backdrop")
        for index in range(cfg["mixed"]["backdrop_tasks"]):
            backdrop.add_task(f"item{index:05}").add_parameter(
                "PAYLOAD", "x" * cfg["large"]["parameter_bytes"]
            )
        commands.call(
            "load", str(write_definition(evidence, backdrop, "backdrop.json"))
        )
        expected_nodes = cfg["mixed"]["backdrop_tasks"] + count + 3

        def reader(index):
            try:
                connection = 0
                while not done.is_set():
                    with Queries(
                        evidence,
                        service,
                        resources,
                        f"reader{index}-connection{connection}",
                    ) as query:
                        for _ in range(30):
                            if done.is_set():
                                break
                            start = time.monotonic()
                            row = query.read("summary" if index == 0 else "full")
                            query_rows.append(row)
                            require(
                                row["nodes"] == expected_nodes,
                                "Concurrent reader omitted tree nodes",
                            )
                            limit = cfg["large"][
                                "summary_deadline_seconds"
                                if index == 0
                                else "full_deadline_seconds"
                            ]
                            require(
                                row["elapsed_s"] <= limit,
                                "Concurrent query exceeded frozen deadline",
                            )
                            done.wait(
                                max(
                                    0,
                                    cfg["mixed"]["query_seconds"]
                                    - (time.monotonic() - start),
                                )
                            )
                    connection += 1
            except Exception as exc:
                errors.append(f"reader{index}: {type(exc).__name__}: {exc}")
                done.set()

        expected, rounds, threads = {}, [], []
        start = time.monotonic()
        start_ns = time.monotonic_ns()
        deadline = start + duration
        try:
            index = 0
            with (evidence.directory / "rounds.jsonl").open("a") as output:
                while time.monotonic() < deadline:
                    resources.check()
                    require(not errors, str(errors))
                    resources.phase = (
                        "steady"
                        if endurance
                        and time.monotonic() - start
                        >= cfg["endurance"]["warmup_seconds"]
                        else "warmup"
                        if endurance
                        else "mixed"
                    )
                    flow = jobs.flow("load")
                    flow.add_limit("work", count)
                    paths = []
                    for number in range(count):
                        task = jobs.task(
                            flow,
                            f"worker{number}",
                            body=f'total=0; for ((i=0;i<1000;i++)); do total=$((total+i)); done\nprintf "round={index} total=%s\\n" "$total" > "result-{index}-worker{number}-$TAKLER_ATTEMPT_ID"',
                        )
                        task.add_meter("step", min_value=0, max_value=100)
                        task.add_in_limit("work")
                        paths.append(f"/load/worker{number}")
                    target = write_definition(evidence, flow, f"round-{index:04}.json")
                    if index:
                        commands.call("replace", "/load", str(target))
                    else:
                        commands.call("load", str(target))
                        commands.call("begin", "load")
                        threads = [
                            threading.Thread(target=reader, args=(n,), daemon=True)
                            for n in range(2)
                        ]
                        for thread in threads:
                            thread.start()
                    active = jobs.phase(
                        f"round-{index:04}-active", {p: "active" for p in paths}
                    )
                    jobs.usage(active, "/load", paths)
                    for path in paths:
                        runtime = active[path]["groups"]["runtime"]
                        file = (
                            jobs.data
                            / f"result-{index}-{path.rsplit('/', 1)[1]}-{runtime['attempt_id']}"
                        )
                        require(
                            file.read_text() == f"round={index} total=499500\n",
                            "Real job business result differs",
                        )
                        for value in range(1, cfg["mixed"]["meters_per_job"] + 1):
                            jobs.action(
                                active,
                                path,
                                f"meter:{100 * value // cfg['mixed']['meters_per_job']}",
                            )
                    reported = jobs.phase(
                        f"round-{index:04}-reported", {p: "active" for p in paths}
                    )
                    require(
                        all(
                            reported[p]["groups"]["runtime"]["meter_values"]
                            == {"step": 100}
                            for p in paths
                        ),
                        "Actual child meter reports were lost",
                    )
                    for path in paths:
                        expected[jobs.action(active, path, "success")] = "success"
                    finished = jobs.phase(
                        f"round-{index:04}-complete", {p: "complete" for p in paths}
                    )
                    jobs.usage(finished, "/load", [])
                    status = commands.status()
                    require(
                        status["status"] == "running" and not status["halt_causes"],
                        "Checkpoint/processing fault halted the load",
                    )
                    row = {
                        "round": index,
                        "elapsed_s": time.monotonic() - start,
                        "at_ns": time.monotonic_ns(),
                        "jobs": count,
                        "attempts": [
                            active[p]["groups"]["runtime"]["attempt_id"] for p in paths
                        ],
                        "last_checkpoint_at": status["last_checkpoint_at"],
                    }
                    rounds.append(row)
                    output.write(json.dumps(row) + "\n")
                    output.flush()
                    if (index + 1) % cfg["mixed"]["slow_every_rounds"] == 0:
                        with Queries(
                            evidence, service, resources, f"slow-round{index}"
                        ) as query:
                            query_rows.append(
                                query.read(
                                    "slow", slow_seconds=cfg["mixed"]["slow_seconds"]
                                )
                            )
                    index += 1
                    wait_until(
                        min(deadline, start + index * cfg["mixed"]["round_seconds"]),
                        resources,
                    )
            elapsed = time.monotonic() - start
        finally:
            done.set()
            for thread in threads:
                thread.join(timeout=cfg["rpc_timeout_seconds"] + 10)
            require(all(not t.is_alive() for t in threads), "Query reader did not stop")
        require(not errors, str(errors))
        require(
            elapsed >= duration
            and len(rounds)
            >= max(1, int(duration / (2 * cfg["mixed"]["round_seconds"]))),
            "Workload did not meet duration/progress gate",
        )
        require(
            rounds[-1]["last_checkpoint_at"] is not None,
            "No periodic checkpoint published during load",
        )
        checks = jobs.finish(expected, capacity=count)
        require(
            len({a for row in rounds for a in row["attempts"]}) == len(expected),
            "A generation reused an attempt",
        )
        resources.phase = "settle"
        if endurance:
            wait_until(time.monotonic() + cfg["endurance"]["settle_seconds"], resources)
            with Queries(evidence, service, resources, "after-settle") as query:
                query_rows.append(query.read("summary"))
            wait_until(time.monotonic() + 2 * cfg["sample_seconds"], resources)
        measured = analysis(evidence, resources, query_rows)
        require(
            measured["summary"]["end_to_end"]["count"] > 0
            and measured["full"]["end_to_end"]["count"] > 0,
            "Missing concurrent client measurements",
        )
        require(resources.windows, "No external checkpoint publication was observed")
        trend = {}
        if endurance:
            stable = [s for s in resources.samples if s["phase"] == "steady"]
            # Use medians of first/last ten-minute windows to reduce job/CP spikes.
            window = min(
                600e9, max(1, duration - cfg["endurance"]["warmup_seconds"]) * 1e9 / 3
            )
            first = [s for s in stable if s["mono_ns"] <= stable[0]["mono_ns"] + window]
            last = [s for s in stable if s["mono_ns"] >= stable[-1]["mono_ns"] - window]
            hours = (
                statistics.median(s["mono_ns"] for s in last)
                - statistics.median(s["mono_ns"] for s in first)
            ) / 3.6e12
            require(hours > 0, "Not enough steady samples for trend")
            for field in ("rss_bytes", "fds", "threads"):
                before, after = [
                    statistics.median(s[field] for s in group)
                    for group in (first, last)
                ]
                trend[field] = {
                    "first_median": before,
                    "last_median": after,
                    "growth": after - before,
                    "per_hour": (after - before) / hours,
                }
            require(
                trend["rss_bytes"]["per_hour"]
                <= cfg["endurance"]["max_rss_slope_bytes_per_hour"],
                "RSS growth exceeded frozen steady trend gate",
            )
            require(
                trend["fds"]["growth"] <= cfg["endurance"]["max_fd_growth"]
                and trend["threads"]["growth"] <= cfg["endurance"]["max_thread_growth"],
                "FD/thread growth exceeded frozen gate",
            )
            evidence.write_json(
                "endurance-trend.json",
                {
                    "steady_window_hours": hours,
                    "trend": trend,
                    "sample_counts": [len(first), len(last)],
                    "requested_seconds": duration,
                    "actual_workload_seconds": elapsed,
                    "start_ns": start_ns,
                    "settle_seconds": cfg["endurance"]["settle_seconds"],
                },
            )
        evidence.record_checks(
            **checks,
            capacity_profile=cfg["name"],
            duration_seconds=elapsed,
            requested_seconds=duration,
            rounds=len(rounds),
            concurrent_readers=2,
            synthetic_child_reports=0,
            real_meter_reports=len(expected) * cfg["mixed"]["meters_per_job"],
            cancelled_slow_reads=measured["slow"]["all_attempts"],
            periodic_checkpoint_publications=len(resources.windows),
            trend=trend,
        )
