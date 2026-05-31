"""Start coverage in subprocesses (aswan's spawned scheduler workers).

Python auto-imports a top-level ``sitecustomize`` at startup; since the project
root is on the workers' ``PYTHONPATH`` (set by the test fixture / the docker
run), this fires in every worker. It is a no-op unless ``COVERAGE_PROCESS_START``
points at the coverage config, so it costs nothing in a normal test run.
"""

import os

if os.environ.get("COVERAGE_PROCESS_START"):
    try:
        import coverage

        coverage.process_startup()
    except ImportError:
        pass
