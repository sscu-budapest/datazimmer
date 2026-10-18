"""Reuse of data and metadata — the cross-project payoff.

* selective re-run   — unchanged steps reuse their fingerprinted outputs rather
                       than recompute; only the step whose output is gone re-runs.
* metadata reuse     — ``schema_codegen`` regenerates the entity classes from the
                       published manifest alone, and the published data reads +
                       validates through those regenerated types.
* dependency-driven  — a downstream project consumes dogshow by URL via a
                       ``[tool.datazimmer.dependencies]`` entry and reuses it for
                       its own analytics.
* multi-project chain — that consumer *re-publishes* a derived dataset, typed by
                       its own entity, which a third project consumes by URL with
                       no knowledge of dogshow — data and metadata reuse compose.
"""

from typing import Annotated

import polars as pl
from datazimmer import (
    Config,
    Entity,
    Index,
    Table,
    fetch_manifest,
    load_dependency,
    publish,
    pull,
    schema_codegen,
    validate,
)


def _write_project(directory, body: str):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text("[tool.datazimmer]\n" + body)


def test_selective_rerun_reuses_unchanged_outputs(built):
    # drop one aggregate's output: its fingerprint is unchanged, so only that
    # step re-runs — collect / parse / the other aggregate reuse their outputs.
    built.breed_wins.purge()
    assert not built.breed_wins.is_populated()

    assert built.pipeline.run() == ["aggregate_breed_wins"]
    assert built.breed_wins.n_rows() == 2  # lab + pug, recomputed from reuse


def test_metadata_reuse_roundtrip(built, tmp_path):
    publish(Config.load(), built.pipeline.output_tables)  # -> _remote/dogshow

    # rebuild the schema purely from the published manifest — no src.meta in
    # sight — then read the published data back through the regenerated types.
    manifest = fetch_manifest("_remote/dogshow")
    ns: dict = {}
    code = schema_codegen(manifest)
    exec(compile(code, "<codegen>", "exec"), ns)  # noqa: S102
    dest = pull("_remote/dogshow", dest=tmp_path / "consumed")

    consumer = {
        name: Table(ns[tm.schema["name"]], dest / name)
        for name, tm in manifest.tables.items()
    }
    # the regenerated Dog yields the same flat columns (incl. both FK columns)
    assert set(consumer["dogs"].column_names) == set(built.dogs.column_names)
    assert consumer["dogs"].collect().height == 5
    # and uniqueness + FK integrity hold when checked through the regen entities
    validate(list(consumer.values()))


def test_dependency_driven_consumption(built, tmp_path):
    publish(Config.load(), built.pipeline.output_tables)

    # a downstream project declares dogshow as a dependency and consumes by URL
    remote = (tmp_path / "_remote" / "dogshow").resolve()
    consumer_dir = tmp_path / "kennel-report"
    _write_project(
        consumer_dir,
        'name = "kennel-report"\n'
        'version = "0.1.0"\n'
        "[tool.datazimmer.dependencies.dogshow]\n"
        f'source = "{remote}"\n'
        'version = "0.1.0"\n',
    )
    cfg = Config.load(consumer_dir)
    assert cfg.get_dependency("dogshow").source == str(remote)

    breeds = load_dependency(cfg, "dogshow", "breeds").collect()
    assert sorted(breeds["name"].to_list()) == [
        "Border Collie",
        "Great Dane",
        "Labrador",
        "Pug",
    ]
    # reuse the consumed tables for the consumer's own analytics
    results = load_dependency(cfg, "dogshow", "results")
    dogs = load_dependency(cfg, "dogshow", "dogs")
    points_by_group = (
        results.join(dogs, left_on="dog__dog_id", right_on="dog_id")
        .join(breeds.lazy(), left_on="breed__breed_id", right_on="breed_id")
        .group_by("group")
        .agg(pl.col("points").sum().alias("points"))
        .collect()
    )
    assert points_by_group["points"].sum() == 35.0  # every result's points


def test_reuse_chain_across_projects(built, tmp_path):
    # project 1 — dogshow publishes to its local remote
    publish(Config.load(), built.pipeline.output_tables)
    dogshow_src = (tmp_path / "_remote" / "dogshow").resolve()

    # project 2 — consumes dogshow by URL, derives a per-group standings table
    # typed by its OWN entity, and re-publishes it to its own remote
    p2, p2_remote = tmp_path / "kennel-report", (tmp_path / "p2-remote").resolve()
    _write_project(
        p2,
        'name = "kennel-report"\nversion = "0.1.0"\n'
        f'[tool.datazimmer.remote]\nkind = "local"\nurl = "{p2_remote}"\n'
        "[tool.datazimmer.dependencies.dogshow]\n"
        f'source = "{dogshow_src}"\nversion = "0.1.0"\n',
    )
    cfg2 = Config.load(p2)
    results = load_dependency(cfg2, "dogshow", "results")
    dogs = load_dependency(cfg2, "dogshow", "dogs")
    breeds = load_dependency(cfg2, "dogshow", "breeds")
    standings = (
        results.join(dogs, left_on="dog__dog_id", right_on="dog_id")
        .join(breeds, left_on="breed__breed_id", right_on="breed_id")
        .group_by("group")
        .agg(pl.col("points").sum().alias("points"))
        .collect()
    )

    class GroupStanding(Entity):
        group: Annotated[str, Index]
        points: float

    standings_tbl = Table(GroupStanding, p2 / "data" / "group_standings")
    standings_tbl.write(standings)
    publish(cfg2, [standings_tbl])
    kennel_report_src = (p2_remote / "kennel-report").resolve()

    # project 3 — knows nothing of dogshow; consumes the re-published, derived
    # dataset by URL and reuses it
    p3 = tmp_path / "audit"
    _write_project(
        p3,
        'name = "audit"\nversion = "0.1.0"\n'
        "[tool.datazimmer.dependencies.standings]\n"
        f'source = "{kennel_report_src}"\nversion = "0.1.0"\n',
    )
    cfg3 = Config.load(p3)
    consumed = load_dependency(cfg3, "standings", "group_standings").collect()
    assert consumed["points"].sum() == 35.0
    assert set(consumed["group"]) == {"sporting", "toy", "herding", "working"}

    # metadata reuse composes too: the re-published entity regenerates from its
    # own manifest, index intact
    code = schema_codegen(fetch_manifest(kennel_report_src))
    assert "group: Annotated[str, Index]" in code
