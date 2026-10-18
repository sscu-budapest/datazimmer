"""Fixtures: the federation site (session) + an isolated project per test.

``project`` redirects everything a run touches into ``tmp_path`` — the aswan
depot (``ASWAN_DEPOT_ROOT``), the dz state/data/_remote dirs (via ``chdir``) —
and exports ``DOGSHOW`` on ``PYTHONPATH`` so spawned aswan workers can import
``src.collection`` to unpickle the handlers.

``built`` clones a session-scoped template that ran the whole pipeline once, so
the aswan collection is paid a single time rather than per test. Every
step's fingerprint is a pure function of step source (produced-table inputs
propagate their producer's fingerprint, not a data digest), so cloning the
built ``data/`` + ``.datazimmer/`` makes a subsequent ``pipeline.run()`` skip
every step deterministically — the depot is never touched. Tests that need a
real collection from scratch (``test_collection``, ``test_e2e``) use ``project``.
"""

import os
import shutil
from pathlib import Path

import pytest
from aswan.constants import DEPOT_ROOT_ENV_VAR
from datazimmer.remote import CACHE_ENV_VAR

from tests.site.federation import federation_app_creator, federation_port
from tests.site.runner import AppRunner, relay_worker

DOGSHOW = Path(__file__).resolve().parents[1]


def _isolate(tmp_path, monkeypatch):
    """Point a clean dz project at ``tmp_path``: depot, pull cache, cwd, imports."""
    monkeypatch.setenv(DEPOT_ROOT_ENV_VAR, str(tmp_path / "depots"))
    # keep `dz pull` hermetic — otherwise it localizes into ~/.cache/datazimmer
    monkeypatch.setenv(CACHE_ENV_VAR, str(tmp_path / "cache"))
    # spawned aswan workers import src.collection to unpickle handlers, and pick
    # up the root-level sitecustomize that starts subprocess coverage; append so
    # the dockerized coverage run's own PYTHONPATH survives.
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv(
        "PYTHONPATH", os.pathsep.join(filter(None, [str(DOGSHOW), existing]))
    )
    shutil.copy(DOGSHOW / "pyproject.toml", tmp_path / "pyproject.toml")
    monkeypatch.chdir(tmp_path)


@pytest.fixture(scope="session")
def federation():
    runner = AppRunner(federation_app_creator, federation_port).start()
    yield runner
    runner.stop()


@pytest.fixture
def project(tmp_path, monkeypatch, federation):
    _isolate(tmp_path, monkeypatch)
    import src

    return src


@pytest.fixture(scope="session")
def _collected_template(tmp_path_factory, federation):
    """Run the full pipeline once (real collection) into a session template."""
    template = tmp_path_factory.mktemp("template")
    with pytest.MonkeyPatch.context() as mp:
        _isolate(template, mp)
        import src

        with relay_worker():
            src.pipeline.run()
    return template


@pytest.fixture
def built(tmp_path, monkeypatch, _collected_template):
    """A project pre-populated by cloning the session template's built outputs.

    A ``pipeline.run()`` here skips every step (all up to date) unless the test
    changes an input — no collection, so these tests are near-instant.
    """
    _isolate(tmp_path, monkeypatch)
    shutil.copytree(_collected_template / "data", tmp_path / "data")
    shutil.copytree(_collected_template / ".datazimmer", tmp_path / ".datazimmer")
    import src

    return src
