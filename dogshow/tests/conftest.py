"""Fixtures: the federation site (session) + an isolated project per test.

``project`` redirects everything a run touches into ``tmp_path`` — the aswan
depot (``ASWAN_DEPOT_ROOT``), the dz state/data/_remote dirs (via ``chdir``) —
and exports ``DOGSHOW`` on ``PYTHONPATH`` so spawned aswan workers can import
``src.collection`` to unpickle the handlers. ``built`` additionally runs the
whole pipeline (collection included) with the relay worker alive.
"""

import os
import shutil
from pathlib import Path

import pytest

from tests.site.federation import federation_app_creator, federation_port
from tests.site.runner import AppRunner, start_relay_worker

DOGSHOW = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def federation():
    runner = AppRunner(federation_app_creator, federation_port).start()
    yield runner
    runner.stop()


@pytest.fixture
def project(tmp_path, monkeypatch, federation):
    monkeypatch.setenv("ASWAN_DEPOT_ROOT", str(tmp_path / "depots"))
    # spawned aswan workers import src.collection to unpickle handlers, and pick
    # up the root-level sitecustomize that starts subprocess coverage; append so
    # the dockerized coverage run's own PYTHONPATH survives.
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join(filter(None, [str(DOGSHOW), existing]))
    )
    shutil.copy(DOGSHOW / "pyproject.toml", tmp_path / "pyproject.toml")
    monkeypatch.chdir(tmp_path)
    import src

    return src


@pytest.fixture
def built(project):
    """``project`` with the full pipeline run once (collection included)."""
    worker = start_relay_worker()
    try:
        project.pipeline.run()
    finally:
        worker.join(timeout=30)
    return project
