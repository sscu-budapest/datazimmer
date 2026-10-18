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

Uses the aswan depot API (``pull(remote)`` / ``push(remote)`` /
``get_handler_events``); aswan is imported lazily so it stays an optional
extra.
"""

import logging
import os
from importlib import import_module
from typing import ClassVar

logger = logging.getLogger(__name__)


class DzAswan:
    name: str = ""
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
