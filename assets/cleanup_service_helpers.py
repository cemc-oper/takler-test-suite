"""Run the service helper cleanup with the system Python."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from takler_test_suite.harness.cleanup_service_helpers import helper_role as helper_role
from takler_test_suite.harness.cleanup_service_helpers import main

if __name__ == "__main__":
    main()
