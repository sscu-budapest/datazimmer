"""Drive the dogshow exactly as a user would — in one bounded command.

``make showcase`` runs this. It boots the federation site and a relay worker,
then runs the *real* ``dz`` CLI (``python -m datazimmer …``) through the full
``run → validate → draw → sql → publish → pull`` loop against the live site, and
tears the site down on the way out. No second shell, and — unlike the test
suite — no pytest: this is the hand-driven path, kept honest.

Hang safety (the suite's third rule applies here too): the site and relay are
spawned with die-with-parent and torn down in a ``finally``; every ``dz`` command
runs in its own process group with a per-command wall-clock cap whose expiry
SIGKILLs the whole group; and the Makefile wraps the lot in a final ``timeout``.
No path leaves anything running. Every artifact stays inside ``dogshow/`` (the
depot is redirected here, not to ``$HOME``) and is gitignored — ``make clean``
wipes it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

from aswan.constants import DEPOT_ROOT_ENV_VAR
from datazimmer.remote import CACHE_ENV_VAR

HERE = Path(__file__).resolve().parent
SITE_URL = "http://localhost:5005"
CMD_TIMEOUT = 90  # generous per-command cap; a full collection is ~5 s


def _dz(*args: str) -> None:
    """Run one ``dz`` command in its own process group, hard-bounded: the group is
    SIGKILLed on any exit that leaves it running (timeout, error, SIGTERM)."""
    print(f"\n$ dz {' '.join(args)}", flush=True)
    proc = subprocess.Popen(
        [sys.executable, "-m", "datazimmer", *args], cwd=HERE, start_new_session=True
    )
    try:
        rc = proc.wait(timeout=CMD_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise SystemExit(f"dz {args[0]} exceeded {CMD_TIMEOUT}s — killed its group")
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    if rc != 0:
        raise SystemExit(f"dz {args[0]} failed ({rc})")


def main() -> int:
    # an outer `timeout` sends SIGTERM: unwind through the `finally`s so the site
    # and any in-flight `dz` group are torn down rather than orphaned
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(128 + signal.SIGTERM))
    # keep every artifact inside dogshow/ (all gitignored) — never touch $HOME,
    # and let spawned aswan workers import src.collection to unpickle handlers.
    os.environ["DOGSHOW_SITE_URL"] = SITE_URL
    os.environ[DEPOT_ROOT_ENV_VAR] = str(HERE / "_depot")
    os.environ[CACHE_ENV_VAR] = str(HERE / "_cache")  # keep pull in-tree
    os.environ["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(HERE), os.environ.get("PYTHONPATH", "")])
    )
    sys.path.insert(0, str(HERE))
    from tests.site.federation import federation_app_creator, federation_port
    from tests.site.runner import AppRunner, relay_worker

    site = AppRunner(federation_app_creator, federation_port).start()
    try:
        with relay_worker():  # the results feed arrives via the relay during run
            _dz("run")  # collect + parse + aggregate
        _dz("run")  # idempotent: every step skips on the second pass
        _dz("validate")  # index uniqueness + FK integrity
        _dz("draw", "--out", "erd.md")  # Mermaid ER diagram (gitignored copy)
        _dz("sql", "--constr", "sqlite:///dogshow.db")  # materialize the database
        _dz("publish")  # -> _remote/dogshow (local backend)
        _dz("pull", "_remote/dogshow")  # round-trip the published dataset
    finally:
        site.stop()
    print(
        "\nshowcase ok — published to _remote/dogshow, db at dogshow.db, "
        "erd at erd.md. `make clean` to wipe.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
