"""The aswan path: handlers, the scheduler, the depot, push/pull.

Exercises aswan (request/soup, request/json, relay, link cascade), atqo (the
subprocess scheduler), and dirark (the content-addressed object store + rsync
dedup on push/pull).
"""

import aswan
import pytest

from src.collection import (
    BreedPageHandler,
    KennelHandler,
    ResultFeedHandler,
    ShowScheduleHandler,
    get_collector,
)
from tests.site.runner import start_relay_worker


def _collect():
    worker = start_relay_worker()
    try:
        get_collector().run()
    finally:
        worker.join(timeout=30)


def test_collects_every_handler(project):
    _collect()
    col = get_collector()

    breeds = [e.content for e in col.events(BreedPageHandler)]
    assert {b["breed_id"] for b in breeds} == {"lab", "pug", "collie", "dane"}
    assert sum(len(b["dogs"]) for b in breeds) == 5  # cascaded from the index

    kennels = next(iter(col.events(KennelHandler))).content
    assert {k["kennel_id"] for k in kennels} == {"k1", "k2"}

    shows = next(iter(col.events(ShowScheduleHandler))).content
    assert {s["show_id"] for s in shows} == {"s1", "s2"}

    # the relay path: results delivered by the external worker
    results = [r for e in col.events(ResultFeedHandler) for r in e.content]
    assert len(results) == 5

    assert len(col.depot.get_all_run_ids()) == 1  # one committed run


def test_push_pull_roundtrip(project, tmp_path):
    _collect()
    remote_dir = tmp_path / "remote"
    remote_dir.mkdir()  # rsync only creates the leaf, not the base
    remote = str(remote_dir)
    get_collector().depot.push(remote)

    # a fresh depot pulls the run + object-store ark back (dirark dedup + rsync)
    fresh = aswan.AswanDepot("dog-federation", tmp_path / "pulled")
    fresh.pull(remote)
    fresh.setup()
    pulled = [e.content for e in fresh.get_handler_events(BreedPageHandler)]
    assert {b["breed_id"] for b in pulled} == {"lab", "pug", "collie", "dane"}


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
