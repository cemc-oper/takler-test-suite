import os
import shlex

from takler.core import Flow, NodeStatus
from takler.exceptions import JobSubmissionError
from takler.tasks.shell import ShellScriptTask, check_job_creation

from takler_test_suite.harness.runtime import require


def run(evidence):
    home = evidence.directory / "job output with spaces"
    templates = evidence.directory / "source templates"
    first = evidence.directory / "includes first"
    second = evidence.directory / "includes second"
    for path in (templates, first, second):
        path.mkdir()
    for directory, text in ((templates, "local"), (first, "first"), (second, "second")):
        (directory / "shared.takler").write_text(f"# shared={text}\n")
    (first / "ordered.takler").write_text("# ordered=first\n")
    (second / "ordered.takler").write_text("# ordered=second\n")
    (second / "tail.takler").write_text("# tail=second\n")
    sentinel = evidence.directory / "executed.marker"
    source = templates / "preview.takler"
    source.write_text(
        "#!/bin/bash\n"
        '{% include "shared.takler" %}\n'
        '{% include "ordered.takler" %}\n'
        '{% include "tail.takler" %}\n'
        "# value={{ VALUE }} node={{ TAKLER_NAME }} try={{ TAKLER_TRY_NO }}\n"
        "# fixed-sample={{ PREVIEW_ATTEMPT }} missing=<{{ UNDEFINED }}>\n"
        'touch "{{ SENTINEL }}"\n'
    )

    flow = Flow("preview")
    flow.add_parameter(
        {
            "TAKLER_HOME": str(home),
            "TAKLER_INCLUDE": os.pathsep.join(map(str, (first, second))),
            "TAKLER_SHELL_JOB_CMD": (
                "{{ TAKLER_JOB | shquote }} 1> {{ TAKLER_JOBOUT | shquote }} 2>&1"
            ),
            "VALUE": "regional sample",
            "PREVIEW_ATTEMPT": "offline-fixture-001",
            "SENTINEL": str(sentinel),
        }
    )
    task = flow.add_task(ShellScriptTask("work", script_path=source))
    command = task.create_job_script()
    job = home / "preview/work.job0"
    output = home / "preview/work.0"
    require(
        command == f"{shlex.quote(str(job))} 1> {shlex.quote(str(output))} 2>&1",
        "Preview command does not quote both paths",
    )
    require(
        task.find_parameter("TAKLER_JOB").value == job
        and task.find_parameter("TAKLER_JOBOUT").value == output,
        "Wrong preview job or output path",
    )
    require(
        job.read_text().splitlines()
        == [
            "#!/bin/bash",
            "# shared=local",
            "# ordered=first",
            "# tail=second",
            "# value=regional sample node=/preview/work try=0",
            "# fixed-sample=offline-fixture-001 missing=<>",
            f'touch "{sentinel}"',
        ],
        "Rendered content or include precedence differs from expectation",
    )
    require(
        not flow.begun
        and task.state.node_status == NodeStatus.unknown
        and task.attempt_id is None
        and task.try_no == 0,
        "Preview allocated runtime identity",
    )

    (templates / "bad_include.takler").write_text(
        '{% include "missing-part.takler" %}\n'
    )
    (templates / "bad_syntax.takler").write_text("{{ unfinished\n")
    failures = {}
    for name, message in (
        ("missing_script", "job template source is not a regular file"),
        ("bad_include", "missing-part.takler"),
        ("bad_syntax", "unexpected end of template"),
    ):
        invalid = Flow(name)
        invalid.add_parameter("TAKLER_HOME", str(home))
        invalid.add_task(
            ShellScriptTask("work", script_path=templates / f"{name}.takler")
        )
        try:
            check_job_creation(invalid)
        except JobSubmissionError as exc:
            require(message in str(exc), f"{name}: unexpected render error: {exc}")
            failures[name] = message
        else:
            require(False, f"{name}: invalid template was accepted")

    require(
        not output.exists() and not sentinel.exists(),
        "Preview script appears to have executed",
    )
    require(list(home.rglob("*.job*")) == [job], "Unexpected job preview artifacts")
    evidence.record_checks(
        rendered_job=str(job.relative_to(evidence.directory)),
        quoted_command=True,
        include_sources=["script_directory", "first_include", "second_include"],
        fixed_sample="offline-fixture-001",
        undefined_rendered_empty=True,
        attempt_id=None,
        try_no=0,
        script_executed=False,
        rejected=failures,
    )
