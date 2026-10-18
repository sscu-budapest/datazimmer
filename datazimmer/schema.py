"""Entity schema definitions using plain classes + type hints::

    class Election(Entity):
        eid: Annotated[str, Index]
        start_date: datetime
        is_individual: bool

    class ElectionPrecinct(Entity):
        pid: Annotated[str, Index]
        name: str
        eligible_voters: float | None
        geo_parent: GeographicalUnit
        election: Election

An entity-typed field is a foreign key: it flattens to the target's index
columns, prefixed with the field name. Only index fields are expanded when
resolving a target, so self-references and other FK cycles through non-index
fields terminate.
"""

import types
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Annotated, Union, get_args, get_origin, get_type_hints


class Index:
    """Marker for index (primary key) fields. Use via ``Annotated[str, Index]``."""


class Entity:
    """Base class for datazimmer entity schemas.

    Subclass and annotate fields to define a schema.
    Entity classes are never instantiated — they are pure schema descriptors.
    """


class SchemaError(Exception):
    pass


PREFIX_SEP = "__"
_PRIMITIVE_TYPES = frozenset({int, float, str, bool, bytes, datetime})
_JSON_TYPES: dict[str, dict] = {
    "int": {"type": "integer"},
    "float": {"type": "number"},
    "str": {"type": "string"},
    "bool": {"type": "boolean"},
    "bytes": {"type": "string", "contentEncoding": "base64"},
    "datetime": {"type": "string", "format": "date-time"},
}


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    """A flat table column derived from an entity schema."""

    name: str
    dtype: type
    nullable: bool = False
    is_index: bool = False
    fk_entity: type | None = None


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """A foreign-key relation: local columns referencing a target entity's index."""

    columns: tuple[str, ...]
    target_entity: str
    target_columns: tuple[str, ...]
    nullable: bool = False


def _unwrap_optional(hint) -> tuple[type, bool]:
    origin = get_origin(hint)
    if origin is Union or origin is types.UnionType:
        args = get_args(hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0], True
    return hint, False


def _unwrap_annotated(hint) -> tuple[type, tuple]:
    if get_origin(hint) is Annotated:
        args = get_args(hint)
        return args[0], args[1:]
    return hint, ()


def _resolve_hint(hint) -> tuple[type, bool, bool]:
    """Returns (base_type, nullable, is_index)."""
    hint, nullable = _unwrap_optional(hint)
    base, markers = _unwrap_annotated(hint)
    # Handle Annotated[Optional[X], ...] edge case
    inner, inner_nullable = _unwrap_optional(base)
    if inner_nullable:
        base, nullable = inner, True
    is_index = Index in markers or any(isinstance(m, Index) for m in markers)
    return base, nullable, is_index


def _is_entity(t) -> bool:
    return isinstance(t, type) and issubclass(t, Entity) and t is not Entity


def _fields(entity_cls: type[Entity]) -> list[tuple[str, type, bool, bool]]:
    """``(name, base_type, nullable, is_index)`` for each public field."""
    fields = []
    for name, hint in get_type_hints(entity_cls, include_extras=True).items():
        if name.startswith("_"):
            continue
        base, nullable, is_index = _resolve_hint(hint)
        if not (_is_entity(base) or base in _PRIMITIVE_TYPES):
            raise SchemaError(f"{entity_cls.__name__}.{name}: unsupported type {hint}")
        fields.append((name, base, nullable, is_index))
    return fields


def _target_index(
    entity_cls: type[Entity], name: str, target: type
) -> list[ColumnInfo]:
    index = get_index_columns(target)
    if not index:
        raise SchemaError(
            f"{entity_cls.__name__}.{name}: {target.__name__} has no Index to reference"
        )
    return index


def _columns(entity_cls: type[Entity], index_only: bool) -> list[ColumnInfo]:
    columns: list[ColumnInfo] = []
    for name, base, nullable, is_index in _fields(entity_cls):
        if index_only and not is_index:
            continue
        if _is_entity(base):
            columns += [
                ColumnInfo(
                    f"{name}{PREFIX_SEP}{c.name}", c.dtype, nullable, is_index, base
                )
                for c in _target_index(entity_cls, name, base)
            ]
        else:
            columns.append(ColumnInfo(name, base, nullable, is_index))
    return columns


def get_columns(entity_cls: type[Entity]) -> list[ColumnInfo]:
    """Get flat column list for an entity, expanding FK references."""
    return _columns(entity_cls, index_only=False)


def get_index_columns(entity_cls: type[Entity]) -> list[ColumnInfo]:
    return _columns(entity_cls, index_only=True)


def get_property_columns(entity_cls: type[Entity]) -> list[ColumnInfo]:
    return [c for c in get_columns(entity_cls) if not c.is_index]


def get_foreign_keys(entity_cls: type[Entity]) -> list[ForeignKey]:
    """Foreign-key relations declared by entity-typed fields."""
    fks: list[ForeignKey] = []
    for name, base, nullable, _ in _fields(entity_cls):
        if not _is_entity(base):
            continue
        target = tuple(c.name for c in _target_index(entity_cls, name, base))
        fks.append(
            ForeignKey(
                columns=tuple(f"{name}{PREFIX_SEP}{c}" for c in target),
                target_entity=base.__name__,
                target_columns=target,
                nullable=nullable,
            )
        )
    return fks


def entity_metadata(entity_cls: type[Entity]) -> dict:
    """Full schema descriptor for an entity.

    Carries the column list, the index (uniqueness key), the sort order, and
    foreign keys — everything needed to materialize a SQL schema, render an ER
    diagram, or regenerate the entity from a published manifest.
    """
    columns = get_columns(entity_cls)
    index = [c.name for c in columns if c.is_index]
    return {
        "name": entity_cls.__name__,
        "doc": entity_cls.__doc__ or "",
        "columns": [
            {
                "name": c.name,
                "dtype": c.dtype.__name__,
                "nullable": c.nullable,
                "is_index": c.is_index,
                "fk_entity": c.fk_entity.__name__ if c.fk_entity else None,
            }
            for c in columns
        ],
        "index": index,
        "sorted_by": index,
        "foreign_keys": [asdict(fk) for fk in get_foreign_keys(entity_cls)],
    }


def get_polars_schema(entity_cls: type[Entity]) -> dict:
    """Return ``{column_name: polars_dtype}`` mapping."""
    import polars as pl

    type_map: dict[type, pl.DataType] = {
        int: pl.Int64(),
        float: pl.Float64(),
        str: pl.Utf8(),
        bool: pl.Boolean(),
        bytes: pl.Binary(),
        datetime: pl.Datetime(),
    }
    return {c.name: type_map[c.dtype] for c in get_columns(entity_cls)}


def to_json_schema(entity_cls: type[Entity]) -> dict:
    """Export entity definition as JSON Schema."""
    meta = entity_metadata(entity_cls)
    properties: dict[str, dict] = {}
    for col in meta["columns"]:
        prop = dict(_JSON_TYPES[col["dtype"]])
        if col["nullable"]:
            prop = {"anyOf": [prop, {"type": "null"}]}
        if col["fk_entity"]:
            prop["$comment"] = f"FK -> {col['fk_entity']}"
        properties[col["name"]] = prop

    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": meta["name"],
        "description": meta["doc"],
        "type": "object",
        "properties": properties,
        "required": [c["name"] for c in meta["columns"] if not c["nullable"]],
        "x-index": meta["index"],
        "x-unique": meta["index"],
        "x-sorted-by": meta["sorted_by"],
        "x-foreign-keys": meta["foreign_keys"],
    }
