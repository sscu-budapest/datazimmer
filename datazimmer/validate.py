"""Data-quality checks over a set of tables.

Verifies the guarantees the schema metadata promises — index uniqueness and
foreign-key integrity — plus any custom checks. The built-in checks are lazy
queries collected together (``pl.collect_all``), so they stream rather than
load whole tables; no database round-trip is required.

    validate([elections, precincts], checks=[no_future_dates])
"""

from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

from .schema import get_foreign_keys, get_index_columns
from .table import Table, tables_by_name

Check = Callable[[dict[str, Table]], "str | None"]


class ValidationError(Exception):
    pass


@dataclass(frozen=True)
class _Probe:
    """A lazy single-count query; a nonzero count is reported as an error."""

    count: pl.LazyFrame
    subject: str
    what: str


def validate(tables: list[Table], checks: list[Check] | tuple[Check, ...] = ()) -> None:
    named = tables_by_name(tables)
    by_entity = {t.entity.__name__: t for t in named.values()}
    probes = [
        probe
        for table in named.values()
        for probe in (*_index_probes(table), *_fk_probes(table, by_entity))
    ]
    counts = pl.collect_all([p.count for p in probes])
    errors = [
        f"{p.subject}: {n} {p.what}" for p, c in zip(probes, counts) if (n := c.item())
    ]
    errors += [msg for check in checks if (msg := check(by_entity))]
    if errors:
        raise ValidationError("; ".join(errors))


def _index_probes(table: Table) -> list[_Probe]:
    index = [c.name for c in get_index_columns(table.entity)]
    if not index:
        return []
    dups = table.read().select(pl.len() - pl.struct(index).n_unique())
    return [_Probe(dups, f"{table.name}: index {index} not unique", "duplicate rows")]


def _fk_probes(table: Table, by_entity: dict[str, Table]) -> list[_Probe]:
    probes = []
    for fk in get_foreign_keys(table.entity):
        target = by_entity.get(fk.target_entity)
        if target is None:
            continue
        cols, target_cols = list(fk.columns), list(fk.target_columns)
        local = table.read().select(cols)
        if fk.nullable:
            local = local.drop_nulls()
        orphans = (
            local.rename(dict(zip(cols, target_cols)))
            .join(target.read().select(target_cols), on=target_cols, how="anti")
            .select(pl.len())
        )
        subject = f"{table.name}.{cols} -> {fk.target_entity}"
        probes.append(_Probe(orphans, subject, "orphan rows"))
    return probes
