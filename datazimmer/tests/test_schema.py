from datetime import date, datetime
from typing import Annotated

import pytest

from datazimmer.schema import (
    ColumnInfo,
    Entity,
    Index,
    SchemaError,
    get_columns,
    get_foreign_keys,
    get_index_columns,
    get_property_columns,
    to_json_schema,
)


class Election(Entity):
    """An election event."""

    eid: Annotated[str, Index]
    is_individual: bool
    start_date: datetime


class GeographicalUnit(Entity):
    gid: Annotated[str, Index]
    name: str | None
    level_info: str | None


class NominatingOrganization(Entity):
    nid: Annotated[str, Index]


class ElectionPrecinct(Entity):
    pid: Annotated[str, Index]
    name: str
    eligible_voters: float | None
    external_votes: bool
    geo_parent: GeographicalUnit
    election: Election


class DistrictHierarchy(Entity):
    """No index — just two FK references."""

    parent: GeographicalUnit
    child: GeographicalUnit


def test_primitive_columns():
    cols = get_columns(Election)
    assert len(cols) == 3
    assert cols[0] == ColumnInfo("eid", str, is_index=True)
    assert cols[1] == ColumnInfo("is_individual", bool)
    assert cols[2] == ColumnInfo("start_date", datetime)


def test_nullable():
    cols = get_columns(GeographicalUnit)
    by_name = {c.name: c for c in cols}
    assert by_name["gid"].nullable is False
    assert by_name["name"].nullable is True
    assert by_name["level_info"].nullable is True


def test_index_columns():
    idx = get_index_columns(Election)
    assert len(idx) == 1
    assert idx[0].name == "eid"
    assert idx[0].is_index is True


def test_property_columns():
    props = get_property_columns(Election)
    assert [c.name for c in props] == ["is_individual", "start_date"]


def test_foreign_key_expansion():
    cols = get_columns(ElectionPrecinct)
    by_name = {c.name: c for c in cols}
    assert "geo_parent__gid" in by_name
    assert by_name["geo_parent__gid"].dtype is str
    assert by_name["geo_parent__gid"].fk_entity is GeographicalUnit
    assert "election__eid" in by_name
    assert by_name["election__eid"].fk_entity is Election


def test_fk_only_entity():
    cols = get_columns(DistrictHierarchy)
    names = [c.name for c in cols]
    assert names == ["parent__gid", "child__gid"]
    assert all(c.fk_entity is GeographicalUnit for c in cols)


class Region(Entity):
    rid: Annotated[str, Index]
    parent: "Region | None"  # self-reference


class Candidacy(Entity):
    election: Annotated[Election, Index]  # composite key made of FKs
    precinct: Annotated[ElectionPrecinct, Index]
    votes: int


class Recount(Entity):
    candidacy: Candidacy
    votes: int


def test_self_reference():
    assert [c.name for c in get_columns(Region)] == ["rid", "parent__rid"]


def test_fk_index_and_nested_fk_key():
    assert [c.name for c in get_index_columns(Candidacy)] == [
        "election__eid",
        "precinct__pid",
    ]
    assert [c.name for c in get_columns(Recount)] == [
        "candidacy__election__eid",
        "candidacy__precinct__pid",
        "votes",
    ]
    (fk,) = get_foreign_keys(Recount)
    assert fk.target_columns == ("election__eid", "precinct__pid")


def test_unsupported_type_fails_loudly():
    class Event(Entity):
        eid: Annotated[str, Index]
        on: date

    with pytest.raises(SchemaError, match="Event.on"):
        get_columns(Event)


def test_fk_to_unindexed_entity_fails_loudly():
    class Link(Entity):
        hierarchy: DistrictHierarchy

    with pytest.raises(SchemaError, match="no Index"):
        get_columns(Link)


def test_json_schema():
    schema = to_json_schema(Election)
    assert schema["title"] == "Election"
    assert schema["description"] == "An election event."
    assert "eid" in schema["properties"]
    assert schema["properties"]["eid"] == {"type": "string"}
    assert schema["x-index"] == ["eid"]
    assert "eid" in schema["required"]


def test_json_schema_nullable():
    schema = to_json_schema(GeographicalUnit)
    assert schema["properties"]["name"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}]
    }
    assert "name" not in schema["required"]


def test_json_schema_fk():
    schema = to_json_schema(ElectionPrecinct)
    prop = schema["properties"]["geo_parent__gid"]
    assert "$comment" in prop
    assert "GeographicalUnit" in prop["$comment"]
