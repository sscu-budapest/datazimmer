"""Bridge a datazimmer pipeline to an aswan collection project.

Subclass ``DzAswan`` to describe a collection: name the depot, list the
handlers + seed URLs, then read parsed events back into pipeline steps::

    class HunElection(DzAswan):
        name = "hun-election"
        starters = {ListHandler: ["https://valtor.valasztas.hu/..."]}

    collector = HunElection()

    @pipeline.step(outputs=[raw_table])
    def collect():
        collector.run()                       # pull, scrape, save, push
        rows = [e.content for e in collector.events(VoteHandler)]
        raw_table.write(pl.DataFrame(rows))

A ``cadence`` (``"30m"``, ``"1h"``, ``"1d"``, ``"2w"``) schedules a collector:
``dz collect`` runs every collector of the modules listed under
``[tool.datazimmer] collectors`` whose cadence has passed since its last
attempt, recorded in ``ATTEMPTS_FILE`` under the state dir. A failed run counts
as an attempt, so a broken collector is retried once per cadence. A collector
without a cadence runs only when named; a subclass without a ``name`` is a base,
not a collector.

Uses the aswan depot API (``pull(remote)`` / ``push(remote)`` /
``get_handler_events``); aswan is imported lazily so it stays an optional
extra.
"""

import json
import logging
import os
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import ClassVar

from .config import Config

logger = logging.getLogger(__name__)

ATTEMPTS_FILE = "collect_attempts.json"
CADENCE_UNITS = {
    "m": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
}


class CollectError(Exception):
    pass


class DzAswan:
    name: str = ""
    cadence: str = ""
    starters: ClassVar[dict] = {}

    def __init__(self) -> None:
        from aswan import Project

        self.project = Project(self.name)
        self.depot = self.project.depot
        self.depot.setup()
        self._registered = False

    def run(self, remote: str | None = None) -> None:
        """Collect; with a remote (argument or aswan's remote env var) set, pull
        the depot first and push it after."""
        from aswan.constants import REMOTE_ENV_VAR

        remote = remote or os.environ.get(REMOTE_ENV_VAR)
        self.prepare_run()
        self._register()
        if remote:
            logger.info("pulling depot %s", self.name)
            self.depot.pull(remote)
            self.depot.current.purge()
        self.project.run(urls_to_overwrite=self.starters)
        self.depot.save_current()
        if remote:
            logger.info("pushing depot %s", self.name)
            self.depot.push(remote)
        self.depot.current.purge()

    def events(self, handler, only_successful: bool = True, only_latest: bool = True):
        self._register()
        return self.depot.get_handler_events(
            handler, only_successful=only_successful, only_latest=only_latest
        )

    def prepare_run(self) -> None:
        """Hook called before collection (e.g. tune ``self.project``)."""

    def _register(self) -> None:
        if self._registered:
            return
        self.project.register_module(import_module(type(self).__module__))
        self._registered = True


@dataclass(frozen=True)
class CollectorState:
    collector: type[DzAswan]
    last_attempt: float | None
    due: bool

    def __str__(self) -> str:
        last = "never"
        if self.last_attempt is not None:
            last = datetime.fromtimestamp(self.last_attempt, UTC).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
        due = " due" if self.due else ""
        return f"{self.collector.name} cadence {self.collector.cadence or '-'}, last attempt {last}{due}"


def parse_cadence(cadence: str) -> timedelta:
    count, unit = cadence[:-1], cadence[-1:]
    if not count.isdigit() or int(count) == 0 or unit not in CADENCE_UNITS:
        raise CollectError(
            f"cadence {cadence!r}: a positive whole number, then one of {', '.join(CADENCE_UNITS)}"
        )
    return int(count) * CADENCE_UNITS[unit]


def find_collectors(config: Config) -> dict[str, type[DzAswan]]:
    """The ``DzAswan`` subclasses defined in the configured modules, by depot name."""
    modules = set(config.collectors)
    for module in modules:
        config.import_module(module)
    found: dict[str, type[DzAswan]] = {}
    for cls in _subclasses(DzAswan):
        if cls.__module__ not in modules or not cls.name:
            continue
        if (other := found.setdefault(cls.name, cls)) is not cls:
            raise CollectError(
                f"depot {cls.name!r} has two collectors: {other} and {cls}"
            )
        if cls.cadence:
            parse_cadence(cls.cadence)
    return found


def collector_states(config: Config, now: float | None = None) -> list[CollectorState]:
    now = time.time() if now is None else now
    attempts = _read_attempts(config)
    return [
        CollectorState(cls, attempts.get(name), _is_due(cls, attempts.get(name), now))
        for name, cls in sorted(find_collectors(config).items())
    ]


def collect(
    config: Config, names: Sequence[str] = (), *, force: bool = False
) -> tuple[list[str], list[str]]:
    """Run the named collectors, else the due ones (with ``force``, every one
    with a cadence); a failure is logged and the rest still run.

    Returns the names run and the names that failed."""
    states = {s.collector.name: s for s in collector_states(config)}
    if unknown := sorted(set(names) - states.keys()):
        raise CollectError(f"no collector for {unknown} in {config.collectors}")
    if names:
        todo = [states[n] for n in names]
    else:
        todo = [s for s in states.values() if s.due or (force and s.collector.cadence)]
    failed = []
    for state in todo:
        name = state.collector.name
        _write_attempt(config, name, time.time())
        try:
            state.collector().run()
        except Exception:
            logger.exception("collector %s failed", name)
            failed.append(name)
    return [s.collector.name for s in todo], failed


def _is_due(cls: type[DzAswan], last_attempt: float | None, now: float) -> bool:
    if not cls.cadence:
        return False
    if last_attempt is None:
        return True
    return now - last_attempt >= parse_cadence(cls.cadence).total_seconds()


def _subclasses(cls: type[DzAswan]) -> Iterator[type[DzAswan]]:
    for sub in cls.__subclasses__():
        yield sub
        yield from _subclasses(sub)


def _read_attempts(config: Config) -> dict[str, float]:
    path = config.state_dir / ATTEMPTS_FILE
    return json.loads(path.read_text()) if path.exists() else {}


def _write_attempt(config: Config, name: str, at: float) -> None:
    attempts = _read_attempts(config) | {name: at}
    config.state_dir.mkdir(parents=True, exist_ok=True)
    (config.state_dir / ATTEMPTS_FILE).write_text(json.dumps(attempts, indent=2))
