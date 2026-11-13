from takler_test_suite.harness.jobs import Jobs
from takler_test_suite.harness.presentation import start_showcase, finish_showcase
from takler_test_suite.harness.runtime import require


def run(evidence, service):
    with Jobs(evidence, service) as jobs:
        mixed, expected = start_showcase(jobs)
        output = service.client("show", "--scope", "/showcase", "--show-all")
        require(
            all(
                word in output.stdout
                for word in (
                    "queued",
                    "active",
                    "aborted",
                    "complete",
                    "running",
                    "failed",
                    "done",
                    "blocked",
                )
            ),
            "CLI did not show the four reference states",
        )
        evidence.write_json(
            "cli-presentation.json",
            {"stdout": output.stdout, "stderr": output.stderr, "reference": mixed},
        )
        expected = finish_showcase(jobs, mixed, expected)
        evidence.record_checks(
            **jobs.finish(expected, capacity=3),
            cli_four_states=True,
            tui="separate_interactive_check_required",
        )
