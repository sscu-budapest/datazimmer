"""``dz collect``: discovery from the listed modules, cadence, attempts."""

import pytest

from datazimmer.aswan_integration import CollectError, parse_cadence
from datazimmer.cli import main

PYPROJECT = """
[tool.datazimmer]
name = "probe"
version = "0.1.0"
package = "probe_src"
collectors = ["probe_src.collectors"]
"""

COLLECTORS = """
from pathlib import Path

from datazimmer import DzAswan

LOG = Path("collected.log")


class _Probe(DzAswan):
    def __init__(self):
        pass

    def run(self, remote=None):
        with LOG.open("a") as f:
            f.write(self.name + "\\n")


class Hourly(_Probe):
    name = "hourly"
    cadence = "1h"


class Broken(_Probe):
    name = "broken"
    cadence = "1d"

    def run(self, remote=None):
        raise RuntimeError("site down")


class Stopped(_Probe):
    name = "stopped"
"""


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    pkg = tmp_path / "probe_src"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "collectors.py").write_text(COLLECTORS)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    return tmp_path


def _collected(root) -> list[str]:
    log = root / "collected.log"
    return log.read_text().split() if log.exists() else []


def test_parse_cadence():
    assert parse_cadence("90m").total_seconds() == 90 * 60
    assert parse_cadence("2w").days == 14
    for bad in ["", "h", "0d", "1.5h", "1y", "-1h"]:
        with pytest.raises(CollectError):
            parse_cadence(bad)


def test_collect_runs_due_once_per_cadence(project):
    assert main(["collect"]) == 1  # broken fails, hourly still runs
    assert _collected(project) == ["hourly"]
    assert main(["collect"]) == 0  # neither is due again, broken included
    assert _collected(project) == ["hourly"]


def test_collect_named_and_forced(project, capsys):
    assert main(["collect", "stopped"]) == 0
    assert main(["collect", "--status"]) == 0
    status = capsys.readouterr().out
    assert "hourly cadence 1h, last attempt never due" in status
    assert "stopped cadence -, last attempt 20" in status
    assert main(["collect", "--force"]) == 1
    assert _collected(project) == ["stopped", "hourly"]
    with pytest.raises(CollectError, match="no collector"):
        main(["collect", "nope"])
