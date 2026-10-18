"""Spawn the federation site and the relay worker as side processes.

Lifted from aswan's ``conftest`` pattern: a spawn-context process (forking a
multi-threaded test process is deadlock-prone under 3.12+), a port guard so a
stale server can't silently poison the next run, and SIGKILL-on-parent-death so
an interrupted run leaks nothing.
"""

import ctypes
import multiprocessing as mp
import os
import signal
import socket
import sys
import time
from contextlib import contextmanager

import requests

_SPAWN = mp.get_context("spawn")


def _die_with_parent():  # pragma: no cover - runs in a spawned subprocess
    if sys.platform != "linux":
        return
    try:
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(1, signal.SIGKILL)
    except Exception:
        pass


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _run_app(app_creator, port_no):  # pragma: no cover - spawned subprocess
    _die_with_parent()
    sys.stdout = open(os.devnull, "w")
    sys.stderr = open(os.devnull, "w")
    app_creator().run(port=port_no)


class AppRunner:
    def __init__(self, app_creator, port_no):
        self.app_creator = app_creator
        self._port_no = port_no
        self.address = f"http://localhost:{port_no}"
        self._process: mp.process.BaseProcess | None = None

    def start(self) -> "AppRunner":
        if _port_in_use(self._port_no):
            raise RuntimeError(
                f"port {self._port_no} already in use — free the stale server first"
            )
        self._process = _SPAWN.Process(
            target=_run_app, args=(self.app_creator, self._port_no)
        )
        self._process.start()
        for i in range(20):
            try:
                requests.get(self.address, timeout=15)
                assert self._process.is_alive()
                break
            except requests.exceptions.ConnectionError:
                if i > 15:
                    raise
            time.sleep(0.2)
        return self

    def stop(self):
        if self._process is not None:
            self._process.kill()
            self._process.join()
            self._process.close()


def start_relay_worker() -> mp.process.BaseProcess:
    """Spawn the pull-based relay worker; returns the process to be stopped.

    The worker is bounded (``_die_with_parent`` + a finite poll loop), but it
    only *self*-terminates after a long idle stretch. Collection blocks until the
    relayed results are in, so callers should ``stop_relay_worker`` once
    ``run()`` returns rather than wait that idle out. The ``serve`` launcher
    re-spawns a fresh one in a loop to stay available across manual ``dz run``s.
    """
    proc = _SPAWN.Process(target=_relay_worker)
    proc.start()
    return proc


def stop_relay_worker(proc: mp.process.BaseProcess, timeout: float = 5.0) -> None:
    """Stop the relay worker now that collection is done.

    ``run()`` returns only after the relayed results have been collected, so the
    worker has finished its job; terminating it skips its ~25 s idle wind-down —
    the single biggest per-collection cost in the suite. SIGKILL is the backstop
    if it ignores the terminate, so this never blocks for long.
    """
    proc.terminate()
    proc.join(timeout)
    if proc.is_alive():
        proc.kill()
        proc.join()


@contextmanager
def relay_worker():
    """A relay worker scoped to a collection: started before, stopped promptly
    after. ``with relay_worker(): collector.run()`` is the whole pattern — the
    worker can never be left running, and the ~25 s idle wind-down is skipped."""
    proc = start_relay_worker()
    try:
        yield proc
    finally:
        stop_relay_worker(proc)


def _relay_worker():  # pragma: no cover - spawned subprocess
    """Mimic an external fetcher: poll the relay port for the pending url, GET
    it from the federation site, POST the JSON body back as the content."""
    _die_with_parent()  # same guard as the site: never outlive a hard-killed parent
    from aswan.constants import (
        RELAY_CONTENT_K,
        RELAY_PORT,
        RELAY_REG_K,
        RELAY_URL_K,
        RELAY_URL_ROUTE,
    )

    base = f"http://localhost:{RELAY_PORT}"
    posted: set[str] = set()
    idle = 0
    for _ in range(300):
        if posted and idle > 25:
            return
        time.sleep(0.1)
        try:
            url = requests.get(f"{base}/{RELAY_URL_ROUTE}", timeout=1).text
        except Exception:
            continue
        if (not url) or (url in posted):
            idle += 1
            continue
        idle = 0
        try:
            content = requests.get(url, timeout=2).json()
        except Exception:
            continue
        requests.post(
            base,
            json={RELAY_CONTENT_K: content, RELAY_URL_K: url, RELAY_REG_K: []},
        )
        posted.add(url)
