from pathlib import Path

from takler.core import RepeatDate

from harness.jobs import Jobs
from harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        source = Path(__file__).resolve().parents[4] / "assets/controlled.takler"
        script = evidence.directory / "repeat.takler"
        script.write_text(
            source.read_text().replace(
                "{{ BODY }}",
                "printf '%s\\n' '{{ RUN_DATE }}' > \"$TAKLER_ATTEMPT_ID.date\"",
            )
        )
        flow = jobs.flow("repeated")
        daily = flow.add_container("daily")
        daily.add_repeat(RepeatDate("RUN_DATE", 20260101, 20260103))
        jobs.task(daily, "work", script=script)
        path, owner = "/repeated/daily/work", "/repeated/daily"
        jobs.load(flow)
        expected, attempts, values = {}, [], []
        for value in (20260101, 20260102, 20260103):
            nodes = jobs.phase(f"round-{value}-active", {path: "active"})
            runtime = nodes[path]["groups"]["runtime"]
            current = nodes[owner]["groups"]["runtime"]["repeat_value"]
            require(current == value, "Unexpected current RepeatDate value")
            attempt = runtime["attempt_id"]
            require(attempt not in attempts, "RepeatDate reused an attempt")
            require(
                (jobs.data / f"{attempt}.date").read_text() == f"{value}\n",
                "Generated repeat variable differs from runtime",
            )
            attempts.append(attempt)
            values.append(current)
            expected[jobs.action(nodes, path, "success")] = "success"
        final = jobs.phase(
            "final", {path: "complete", owner: "complete", "/repeated": "complete"}
        )
        require(
            final[owner]["groups"]["runtime"]["repeat_value"] == 20260103,
            "RepeatDate terminal value changed",
        )
        jobs.hold(
            "repeat-exhausted-no-fourth-job", {path: "complete", owner: "complete"}
        )
        evidence.record_checks(
            **jobs.finish(expected, capacity=1),
            repeat_kind="RepeatDate",
            values=values,
            attempts=attempts,
            try_numbers=[1, 1, 1],
            no_extra_round=True,
        )
