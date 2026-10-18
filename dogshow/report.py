"""Generate ``REPORT.md`` — the single source of truth for the suite's measured
numbers: total test runtime and per-library coverage across the whole ecosystem.

Regenerated on every run, so the README can point here instead of copying
figures that would go stale. Run it directly::

    make report            # == uv run python report.py

It is also the body of the dockerized cross-package run (``docker/run-coverage.sh``),
which adds the real rsync-over-ssh backend before calling it.

Hang safety: the suite spawns subprocess workers, a relay and a Flask site. The
in-process ``pytest-timeout`` timers (per-test ``--timeout`` and
``--session-timeout``) catch the common stalls, but they are Python-level and
cannot fire if a native call deadlocks while holding the GIL. So the pytest run
is launched in its own process group under a wall-clock cap (``SUITE_TIMEOUT``,
overridable via ``DOGSHOW_SUITE_TIMEOUT``); on expiry the whole group is SIGKILLed and the report is written
with a loud TIMEOUT banner. The runner can never wedge indefinitely.
"""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree

HERE = Path(__file__).resolve().parent
# the spine first, then the siblings it drives
PACKAGES = ["datazimmer", "shackleton", "sqlmermaid", "aswan", "atqo", "dirark"]

COV_DIR = HERE / "coverage"
HTML_DIR = COV_DIR / "html"
COV_JSON = COV_DIR / "coverage.json"
JUNIT = COV_DIR / "junit.xml"
REPORT = HERE / "REPORT.md"

# wall-clock cap on the pytest subprocess; the external backstop to the in-process
# pytest-timeout timers. Generous enough never to false-fire on a slow CI box.
SUITE_TIMEOUT = int(os.environ.get("DOGSHOW_SUITE_TIMEOUT", "600"))


def _run_pytest(env: dict) -> tuple[int, bool]:
    """Run the suite under coverage in its own process group, hard-bounded.

    ``start_new_session`` makes the coverage/pytest process a group leader, so a
    single ``killpg`` reaps everything the suite spawned — workers, the relay and
    the site — leaving nothing running in the background. Returns
    ``(returncode, timed_out)``.
    """
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "coverage",
            "run",
            "-m",
            "pytest",
            "-q",
            f"--junitxml={JUNIT}",
        ],
        cwd=HERE,
        env=env,
        start_new_session=True,
    )
    try:
        return proc.wait(timeout=SUITE_TIMEOUT), False
    except subprocess.TimeoutExpired:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                break
            try:
                proc.wait(timeout=15)
                break
            except subprocess.TimeoutExpired:
                continue
        return 124, True


def run_suite() -> tuple[float, int, bool]:
    """Run the suite under coverage; write coverage/{html,coverage.json,junit.xml}.

    Returns ``(wall_clock_seconds, returncode, timed_out)``. ``COVERAGE_PROCESS_START``
    + the project root on ``PYTHONPATH`` let ``sitecustomize.py`` start coverage
    inside aswan's spawned workers, so worker code is measured too.
    """
    env = dict(os.environ)
    env["COVERAGE_PROCESS_START"] = str(HERE / "pyproject.toml")
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(HERE), env.get("PYTHONPATH", "")])
    )
    COV_DIR.mkdir(exist_ok=True)

    def cov(*args: str) -> int:
        return subprocess.run(
            [sys.executable, "-m", "coverage", *args], cwd=HERE, env=env
        ).returncode

    cov("erase")
    start = time.perf_counter()
    rc, timed_out = _run_pytest(env)
    wall = time.perf_counter() - start
    # whatever coverage data the (possibly killed) run flushed, combine + render
    cov("combine")
    cov("json", "-o", str(COV_JSON))
    cov("html", "-d", str(HTML_DIR))
    return wall, rc, timed_out


def _package_roots() -> dict[str, Path]:
    """Map each package to its on-disk source root (a dir, or a single module)."""
    roots: dict[str, Path] = {}
    for name in PACKAGES:
        spec = importlib.util.find_spec(name)
        locs = spec.submodule_search_locations if spec else None
        roots[name] = Path(locs[0] if locs else spec.origin).resolve()
    return roots


def _owner(path: Path, roots: dict[str, Path]) -> str | None:
    for name, root in roots.items():
        if path == root or root in path.parents:
            return name
    return None


def _coverage_rows() -> tuple[list[tuple[str, int, int, float]], dict]:
    if not COV_JSON.exists():
        return [], {}
    data = json.loads(COV_JSON.read_text())
    roots = _package_roots()
    agg = {n: [0, 0, 0, 0] for n in PACKAGES}  # stmts, cov_lines, branches, cov_br
    for fpath, info in data["files"].items():
        owner = _owner(Path(fpath).resolve(), roots)
        if owner is None:
            continue
        s = info["summary"]
        a = agg[owner]
        a[0] += s["num_statements"]
        a[1] += s["covered_lines"]
        a[2] += s.get("num_branches", 0)
        a[3] += s.get("covered_branches", 0)
    rows = []
    for n in PACKAGES:
        stmts, cl, br, cb = agg[n]
        denom, cov = stmts + br, cl + cb
        rows.append((n, stmts, br, 100 * cov / denom if denom else 0.0))
    return rows, data["totals"]


def _junit_summary() -> dict[str, int]:
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    if not JUNIT.exists():
        return totals
    for suite in ElementTree.parse(JUNIT).getroot().iter("testsuite"):
        for key in totals:
            totals[key] += int(suite.get(key, 0))
    return totals


def _slowest_tests(n: int = 5) -> list[tuple[float, str]]:
    """The n slowest test cases — a standing watch for any test creeping toward a
    hang. Parsed from the junit per-testcase timings."""
    if not JUNIT.exists():
        return []
    cases = [
        (float(tc.get("time", 0.0)), f"{tc.get('classname', '')}::{tc.get('name', '')}")
        for tc in ElementTree.parse(JUNIT).getroot().iter("testcase")
    ]
    cases.sort(reverse=True)
    return cases[:n]


def _fmt_duration(seconds: float) -> str:
    minutes, secs = divmod(int(round(seconds)), 60)
    return f"{minutes} m {secs:02d} s" if minutes else f"{seconds:.1f} s"


def write_report(wall: float, timed_out: bool) -> None:
    rows, totals = _coverage_rows()
    t = _junit_summary()
    passed = t["tests"] - t["failures"] - t["errors"] - t["skipped"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "# Dogshow run report",
        "",
        "_Generated by `make report` (`report.py`) — do not edit. Regenerated on "
        "every run; the README points here so timing and coverage figures never "
        "go stale in prose._",
        "",
    ]
    if timed_out:
        lines += [
            f"> ⛔ **TIMEOUT** — the suite exceeded `DOGSHOW_SUITE_TIMEOUT="
            f"{SUITE_TIMEOUT}s` and its process group was killed. Figures below "
            "are partial. Investigate the slowest tests before trusting them.",
            "",
        ]
    lines += [
        f"- **Generated:** {now}",
        f"- **Result:** {passed} passed · {t['skipped']} skipped · "
        f"{t['failures'] + t['errors']} failed",
        f"- **Total test runtime:** {_fmt_duration(wall)} (wall clock)",
        "- **Browse:** [`coverage/html/index.html`](coverage/html/index.html) · "
        "ER diagram [`docs/schema.md`](docs/schema.md)",
        "",
        "## Cross-package coverage",
        "",
        "One suite, six packages. Collection runs in spawned subprocess workers, "
        "so these include worker code (`sitecustomize.py` starts coverage in each). "
        "The dogshow's own `src/` is the test driver, not under measure.",
        "",
        "| Package | Statements | Branches | Coverage |",
        "| --- | ---: | ---: | ---: |",
    ]
    lines += [f"| {n} | {stmts} | {br} | {pct:.1f}% |" for n, stmts, br, pct in rows]
    if totals:
        lines.append(
            f"| **combined** | {totals['num_statements']} | "
            f"{totals.get('num_branches', 0)} | {totals['percent_covered_display']}% |"
        )

    slowest = _slowest_tests()
    if slowest:
        lines += [
            "",
            "## Slowest tests",
            "",
            "_Watched because a hang shows up here first — collection and the "
            "real end-to-end run dominate; nothing should creep toward the "
            "`--timeout` ceiling._",
            "",
            "| Test | Seconds |",
            "| --- | ---: |",
        ]
        lines += [f"| `{name}` | {secs:.1f} |" for secs, name in slowest]
    lines.append("")
    REPORT.write_text("\n".join(lines))


def main() -> int:
    wall, rc, timed_out = run_suite()
    write_report(wall, timed_out)
    print(f"wrote {REPORT.name} and coverage/html/index.html")
    return 124 if timed_out else rc


if __name__ == "__main__":
    raise SystemExit(main())
