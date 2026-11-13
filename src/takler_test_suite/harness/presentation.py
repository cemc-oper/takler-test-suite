"""The QRY-004 four-state fixture, shared by CLI and operator exercises."""


def start_showcase(jobs, *, script=None, detached=False):
    flow = jobs.flow("showcase")
    flow.add_limit("work", 3)
    if detached:
        flow.add_parameter(
            "TAKLER_SHELL_JOB_CMD",
            "setsid {{ TAKLER_JOB | shquote }} > {{ TAKLER_JOBOUT | shquote }} 2>&1 < /dev/null &",
        )
    for name in ("running", "failed", "done"):
        jobs.task(flow, name, script=script).add_in_limit("work")
    jobs.task(
        flow,
        "blocked",
        "/showcase/running == complete and /showcase/failed == aborted",
        script=script,
    ).add_in_limit("work")
    jobs.load(flow)
    active = jobs.phase(
        "three-active",
        {
            "/showcase/running": "active",
            "/showcase/failed": "active",
            "/showcase/done": "active",
            "/showcase/blocked": "queued",
        },
    )
    expected = {
        jobs.action(active, "/showcase/done", "success"): "success",
        jobs.action(active, "/showcase/failed", "failure"): "failure",
    }
    mixed = jobs.phase(
        "four-states",
        {
            "/showcase/running": "active",
            "/showcase/failed": "aborted",
            "/showcase/done": "complete",
            "/showcase/blocked": "queued",
        },
    )
    return mixed, expected


def finish_showcase(jobs, mixed, expected):
    expected[jobs.action(mixed, "/showcase/running", "success")] = "success"
    nodes = jobs.phase("blocked-released", {"/showcase/blocked": "active"})
    expected[jobs.action(nodes, "/showcase/blocked", "success")] = "success"
    jobs.phase("showcase-final", {"/showcase/blocked": "complete"})
    return expected
