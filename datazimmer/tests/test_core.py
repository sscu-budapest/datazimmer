"""Focused unit tests for config, pipeline, validate and the CLI."""

from typing import Annotated

import polars as pl
import pytest

from datazimmer import (
    Config,
    Entity,
    Index,
    Pipeline,
    Table,
    ValidationError,
    validate,
)
from datazimmer.cli import main
from datazimmer.config import ConfigError
from datazimmer.pipeline import PipelineError

PYPROJECT = """
[project]
name = "demo"
version = "0.1.0"

[tool.datazimmer]
name = "demo"
version = "0.2.0"
package = "pkg"

[tool.datazimmer.remote]
kind = "s3"
url = "s3://bucket/data"

[tool.datazimmer.dependencies.elections]
source = "s3://bucket/data/electiondata"
version = "0.1.0"
"""


def test_config_load(tmp_path):
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    config = Config.load(tmp_path)
    assert (config.name, config.version, config.package) == ("demo", "0.2.0", "pkg")
    assert config.remote.kind == "s3"
    dep = config.get_dependency("elections")
    assert dep.source.endswith("electiondata") and dep.version == "0.1.0"


def test_config_missing_table(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    with pytest.raises(ConfigError):
        Config.load(tmp_path)


class Thing(Entity):
    tid: Annotated[str, Index]
    n: int


def test_pipeline_force_and_skip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    table = Table(Thing, "data/thing")
    pipe = Pipeline()
    calls = []

    @pipe.step(outputs=[table])
    def build():
        calls.append(1)
        table.write(pl.DataFrame({"tid": ["a"], "n": [1]}))

    assert pipe.run() == ["build"]
    assert pipe.run() == []  # skipped
    assert pipe.run(force=True) == ["build"]  # forced
    assert len(calls) == 2


def test_pipeline_rejects_duplicate_step_names():
    pipe = Pipeline()
    pipe.step()(lambda: None)
    with pytest.raises(PipelineError, match="duplicate"):
        pipe.step()(lambda: None)


def test_pipeline_records_each_step_on_success(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    a, b = Table(Thing, "data/a"), Table(Thing, "data/b")
    pipe = Pipeline()
    fail = [True]

    @pipe.step(outputs=[a])
    def build_a():
        a.write(pl.DataFrame({"tid": ["x"], "n": [1]}))

    @pipe.step(inputs=[a], outputs=[b])
    def build_b():
        if fail[0]:
            raise RuntimeError("boom")
        b.write(a.collect())

    with pytest.raises(RuntimeError):
        pipe.run()
    fail[0] = False
    assert pipe.run() == ["build_b"]  # build_a's success survived the failure


def test_pipeline_detects_cycle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    a, b = Table(Thing, "data/a"), Table(Thing, "data/b")
    pipe = Pipeline()

    @pipe.step(inputs=[b], outputs=[a])
    def sa():
        pass

    @pipe.step(inputs=[a], outputs=[b])
    def sb():
        pass

    with pytest.raises(PipelineError, match="cycle"):
        pipe.run()


def test_pipeline_step_ordering_dependency(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    table = Table(Thing, "data/thing")
    pipe = Pipeline()
    order = []

    @pipe.step(outputs=[])  # produces no table, only a side effect
    def collect():
        order.append("collect")

    @pipe.step(inputs=[collect], outputs=[table])  # ordering dep on a step
    def build():
        order.append("build")
        table.write(pl.DataFrame({"tid": ["a"], "n": [1]}))

    assert pipe.run() == ["collect", "build"]  # collect ordered first
    assert order == ["collect", "build"]
    assert pipe.run() == []  # both skip when unchanged


def test_validate_duplicate_index(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    table = Table(Thing, "data/thing")
    table._repo.replace_all(pl.DataFrame({"tid": ["a", "a"], "n": [1, 2]}))
    with pytest.raises(ValidationError, match="not unique"):
        validate([table])


def test_validate_custom_check(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    table = Table(Thing, "data/thing")
    table.write(pl.DataFrame({"tid": ["a"], "n": [-5]}))

    def no_negatives(tables):
        if tables["Thing"].collect()["n"].min() < 0:
            return "Thing.n has negatives"
        return None

    with pytest.raises(ValidationError, match="negatives"):
        validate([table], checks=[no_negatives])


def test_tables_need_unique_names(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SchemaError, match="two tables"):
        validate([Table(Thing, "a/thing"), Table(Thing, "b/thing")])


def test_cli_init_and_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["init", "proj"]) == 0
    assert (tmp_path / "proj" / "pyproject.toml").exists()
    assert "proj" in (tmp_path / "proj" / "pyproject.toml").read_text()

    monkeypatch.chdir(tmp_path / "proj")
    assert main(["run"]) == 0  # template pipeline builds records
    assert main(["validate"]) == 0
