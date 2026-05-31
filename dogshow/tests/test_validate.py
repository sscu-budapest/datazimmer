"""Validation: index uniqueness, FK integrity (incl. nullable FK), custom checks."""

import polars as pl
import pytest

from datazimmer import ValidationError, validate


def test_passes_clean(built):
    # d3 has a null kennel FK — a clean pass proves null FKs aren't flagged
    validate(built.ALL_TABLES)


def test_detects_orphan_fk(built):
    bad = built.results.collect().with_columns(pl.lit("ghost").alias("dog__dog_id"))
    built.results._repo.replace_all(bad)
    with pytest.raises(ValidationError, match="orphan"):
        validate(built.ALL_TABLES)


def test_detects_duplicate_index(built):
    dup = pl.concat([built.breeds.collect(), built.breeds.collect().head(1)])
    built.breeds._repo.replace_all(dup)
    with pytest.raises(ValidationError, match="not unique"):
        validate(built.ALL_TABLES)


def test_custom_check(built):
    def positive_points(tables):
        if tables["Result"].collect()["points"].min() <= 0:
            return "Result.points must be positive"
        return None

    validate(built.ALL_TABLES, checks=[positive_points])  # passes

    built.results._repo.replace_all(
        built.results.collect().with_columns(pl.lit(-1.0).alias("points"))
    )
    with pytest.raises(ValidationError, match="positive"):
        validate(built.ALL_TABLES, checks=[positive_points])
