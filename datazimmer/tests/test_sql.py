from pathlib import Path

from datazimmer.typer_commands import draw


def test_draw(in_template):
    draw(v=False)
    assert Path("erd.md").exists()
