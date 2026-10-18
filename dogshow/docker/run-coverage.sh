#!/usr/bin/env bash
# Build the env and run the suite under coverage (incl. aswan's spawned workers),
# reporting the combined coverage of every ecosystem package.
#
# Runs as the host user (dropped to by entrypoint.sh), so coverage/, REPORT.md
# and .pytest_cache land host-owned — no root-owned mess left behind.
set -euo pipefail

uv sync

# report.py runs pytest in its own process group under DOGSHOW_SUITE_TIMEOUT and
# kills the group on expiry; this outer `timeout` is the last-ditch backstop on
# report.py itself, so the container is guaranteed to exit (compose's
# --abort-on-container-exit then reaps everything — no background hang).
exec timeout --kill-after=30 1200 uv run python report.py
