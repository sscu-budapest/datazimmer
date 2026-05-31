from datetime import datetime
from pathlib import Path
from typing import Annotated

import polars as pl
import pytest

from datazimmer.schema import Entity, Index
from datazimmer.table import SchemaError, Table


class Election(Entity):
    eid: Annotated[str, Index]
    start_date: datetime
    is_individual: bool


class GeographicalUnit(Entity):
    gid: Annotated[str, Index]
    name: str | None


class ElectionPrecinct(Entity):
    pid: Annotated[str, Index]
    name: str
    geo_parent: GeographicalUnit
    election: Election


SAMPLE_ELECTIONS = pl.DataFrame(
    {
        "eid": ["e1", "e2", "e3"],
        "start_date": [
            datetime(2022, 1, 1),
            datetime(2022, 4, 3),
            datetime(2022, 10, 1),
        ],
        "is_individual": [True, False, True],
    }
)


@pytest.fixture
def election_table(tmp_path: Path) -> Table:
    return Table(Election, tmp_path / "elections")


@pytest.fixture
def precinct_table(tmp_path: Path) -> Table:
    return Table(ElectionPrecinct, tmp_path / "precincts")


def test_write_and_read(election_table: Table):
    election_table.write(SAMPLE_ELECTIONS)
    result = election_table.collect()
    assert result.shape == (3, 3)
    assert set(result.columns) == {"eid", "start_date", "is_individual"}


def test_append(election_table: Table):
    election_table.write(SAMPLE_ELECTIONS)
    extra = pl.DataFrame(
        {
            "eid": ["e4"],
            "start_date": [datetime(2023, 1, 1)],
            "is_individual": [False],
        }
    )
    election_table.append(extra)
    assert election_table.collect().shape[0] == 4


def test_replace(election_table: Table):
    election_table.write(SAMPLE_ELECTIONS)
    updated = pl.DataFrame(
        {
            "eid": ["e1"],
            "start_date": [datetime(2025, 1, 1)],
            "is_individual": [False],
        }
    )
    election_table.replace(updated)
    result = election_table.collect()
    assert result.shape[0] == 3
    row = result.filter(pl.col("eid") == "e1")
    assert row["is_individual"][0] is False


def test_purge(election_table: Table):
    election_table.write(SAMPLE_ELECTIONS)
    election_table.purge()
    assert election_table.collect().shape[0] == 0


def test_read_lazy(election_table: Table):
    election_table.write(SAMPLE_ELECTIONS)
    lf = election_table.read()
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().shape == (3, 3)


def test_missing_columns(election_table: Table):
    bad = pl.DataFrame({"eid": ["e1"], "start_date": [datetime(2022, 1, 1)]})
    with pytest.raises(SchemaError, match="missing columns"):
        election_table.write(bad)


def test_extra_columns(election_table: Table):
    bad = SAMPLE_ELECTIONS.with_columns(pl.lit("x").alias("bonus"))
    with pytest.raises(SchemaError, match="unexpected columns"):
        election_table.write(bad)


def test_type_coercion(election_table: Table):
    df = pl.DataFrame(
        {
            "eid": ["e1"],
            "start_date": [datetime(2022, 1, 1)],
            "is_individual": [1],  # int, should coerce to bool
        }
    )
    election_table.write(df)
    result = election_table.collect()
    assert result.schema["is_individual"] == pl.Boolean


def test_fk_expanded_columns(precinct_table: Table):
    df = pl.DataFrame(
        {
            "pid": ["p1"],
            "name": ["District 5"],
            "geo_parent__gid": ["g1"],
            "election__eid": ["e1"],
        }
    )
    precinct_table.write(df)
    result = precinct_table.collect()
    assert "geo_parent__gid" in result.columns
    assert "election__eid" in result.columns


def test_column_names(election_table: Table):
    assert election_table.column_names == ["eid", "start_date", "is_individual"]


def test_polars_schema(election_table: Table):
    schema = election_table.polars_schema
    assert schema["eid"] == pl.Utf8()
    assert schema["is_individual"] == pl.Boolean()
