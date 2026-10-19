"""Materialize entity-typed tables into a relational database + ER diagrams.

Builds a SQLAlchemy schema from the entity metadata (primary keys from the
index, foreign keys from entity-typed fields), loads the parquet data, and
renders a Mermaid ER diagram via sqlmermaid. This is what makes the index /
uniqueness / foreign-key information carried in the schema metadata pay off.
"""

from datetime import datetime
from pathlib import Path

import sqlalchemy as sa

from .schema import get_columns, get_foreign_keys
from .table import Table, tables_by_name

DEFAULT_DB = "sqlite:///data.db"
DEFAULT_ERD = "erd.md"

_SA_TYPES = {
    int: sa.BigInteger,
    float: sa.Float,
    str: sa.String,
    bool: sa.Boolean,
    bytes: sa.LargeBinary,
    datetime: sa.DateTime,
}


def build_metadata(tables: list[Table]) -> sa.MetaData:
    meta = sa.MetaData()
    named = tables_by_name(tables)
    table_name_by_entity = {t.entity.__name__: name for name, t in named.items()}
    for table in named.values():
        columns = [
            sa.Column(
                c.name,
                _SA_TYPES[c.dtype](),
                primary_key=c.is_index,
                nullable=c.nullable,
            )
            for c in get_columns(table.entity)
        ]
        constraints = []
        for fk in get_foreign_keys(table.entity):
            target = table_name_by_entity.get(fk.target_entity)
            if target is None:
                continue
            constraints.append(
                sa.ForeignKeyConstraint(
                    list(fk.columns),
                    [f"{target}.{tc}" for tc in fk.target_columns],
                )
            )
        sa.Table(table.name, meta, *columns, *constraints)
    return meta


def to_sql(
    tables: list[Table],
    constr: str = DEFAULT_DB,
    *,
    batch_size: int = 10_000,
) -> sa.Engine:
    """Create the schema and load every table's rows, parents before children."""
    engine = sa.create_engine(constr)
    meta = build_metadata(tables)
    # idempotent: a fresh materialization of the current data, so re-running
    # `dz sql` against an existing db replaces rather than appends (which would
    # otherwise fail on the primary-key/uniqueness constraints).
    meta.drop_all(engine)
    meta.create_all(engine)
    named = tables_by_name(tables)
    with engine.begin() as conn:
        for sa_table in meta.sorted_tables:
            rows = named[sa_table.name].collect().to_dicts()
            for start in range(0, len(rows), batch_size):
                conn.execute(sa_table.insert(), rows[start : start + batch_size])
    return engine


def draw(tables: list[Table], out: Path | str = DEFAULT_ERD) -> Path:
    """Write a Mermaid ER diagram derived from the entity schema."""
    from sqlmermaid import get_mermaid

    mermaid = get_mermaid(build_metadata(tables))
    out = Path(out)
    out.write_text(f"```mermaid\n{mermaid}\n```\n")
    return out
