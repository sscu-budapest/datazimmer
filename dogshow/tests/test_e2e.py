"""The whole loop through the ``dz`` CLI — collect → validate → draw → sql →
publish → pull — as the integration smoke test."""

from pathlib import Path

from datazimmer.cli import main

from tests.site.runner import relay_worker


def test_cli_full_loop(project, tmp_path):
    with relay_worker():
        assert main(["run"]) == 0  # collect + parse + aggregate

    assert main(["run"]) == 0  # idempotent: everything skips
    assert main(["validate"]) == 0
    assert main(["draw", "--out", "erd.md"]) == 0
    assert Path("erd.md").exists()
    assert main(["sql", "--constr", f"sqlite:///{tmp_path / 'dog.db'}"]) == 0
    assert main(["schema", "export", "--out", "schemas"]) == 0
    assert (Path("schemas") / "dogs.json").exists()

    assert main(["publish"]) == 0  # -> _remote (local backend)
    assert main(["pull", "_remote/dogshow"]) == 0
