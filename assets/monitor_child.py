"""Time the real child CLI and its unary gRPC attempts without recording secrets."""

import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryFile
import time


def traced_cli(trace_fd, client, arguments):
    # This separate interpreter executes the installed console script. CAP-004
    # runs a local plaintext server, so instrument only that channel factory.
    import grpc

    calls = []

    class Trace(grpc.UnaryUnaryClientInterceptor):
        def intercept_unary_unary(self, continuation, details, request):
            start = time.monotonic_ns()
            code = "UNKNOWN"
            try:
                reply = continuation(details, request)
                code = reply.code().name
                return reply
            finally:
                calls.append(
                    {
                        "method": details.method.rsplit("/", 1)[-1],
                        "start_ns": start,
                        "end_ns": time.monotonic_ns(),
                        "code": code,
                    }
                )

    channel = grpc.insecure_channel

    def traced_channel(*args, **kwargs):
        return grpc.intercept_channel(channel(*args, **kwargs), Trace())

    grpc.insecure_channel = traced_channel
    sys.argv = [client, *arguments]
    try:
        runpy.run_path(client, run_name="__main__")
    finally:
        with os.fdopen(trace_fd, "w") as output:
            json.dump(calls, output)


def main():
    if sys.argv[1] == "--trace-cli":
        return traced_cli(int(sys.argv[2]), sys.argv[3], sys.argv[4:])

    destination, client, *arguments = sys.argv[1:]
    calls = []
    with TemporaryFile() as trace:
        command = [client, *arguments]
        trace_fds = ()
        if Path(client).name == "takler-client-py":
            trace_fds = (trace.fileno(),)
            command = [
                sys.executable,
                "-B",
                __file__,
                "--trace-cli",
                str(trace.fileno()),
                *command,
            ]
        start = time.monotonic_ns()
        result = subprocess.run(command, pass_fds=trace_fds)
        end = time.monotonic_ns()
        trace.seek(0)
        if payload := trace.read():
            calls = json.loads(payload)
    row = {
        "command": arguments[0],
        "path": os.environ["TAKLER_NAME"],
        "attempt": os.environ["TAKLER_ATTEMPT_ID"],
        "start_ns": start,
        "end_ns": end,
        "returncode": result.returncode,
        "rpcs": calls,
    }
    if arguments[0] == "meter" and "--meter-value" in arguments:
        row["meter_value"] = int(arguments[arguments.index("--meter-value") + 1])
    with open(destination, "ab", buffering=0) as output:
        output.write((json.dumps(row) + "\n").encode())
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
