"""Time the real child CLI from process launch to exit without recording secrets."""

import json
import os
import subprocess
import sys
import time


def main():
    destination, client, *arguments = sys.argv[1:]
    start = time.monotonic_ns()
    result = subprocess.run([client, *arguments])
    end = time.monotonic_ns()
    row = {
        "command": arguments[0],
        "path": os.environ["TAKLER_NAME"],
        "attempt": os.environ["TAKLER_ATTEMPT_ID"],
        "start_ns": start,
        "end_ns": end,
        "returncode": result.returncode,
    }
    if arguments[0] == "meter" and "--meter-value" in arguments:
        row["meter_value"] = int(arguments[arguments.index("--meter-value") + 1])
    with open(destination, "ab", buffering=0) as output:
        output.write((json.dumps(row) + "\n").encode())
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
