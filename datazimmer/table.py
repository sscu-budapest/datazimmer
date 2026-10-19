"""Typed table storage backed by shackleton's TableRepo. Explicit paths, no env
context magic.

    elections = Table(Election, "data/elections")
    elections.write(df)
    lf = elections.read()
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import polars as pl
from shackleton import DfBatchWriter, HashPartitioner, RecordWriter, TableRepo

from .schema import (
    Entity,
    SchemaError,
    get_columns,
    get_index_columns,
    get_polars_schema,
)

T = TypeVar("T")


@dataclass
class Table:
    """Typed table backed by shackleton's TableRepo.

    Validates schema on every write. Reads return LazyFrames.
    """

    entity: type[Entity]
    path: Path
    partition_cols: list[str] | HashPartitioner = field(default_factory=list)
    max_records: int = 0
    compression: str | None = None

    def __post_init__(self):
        self.path = Path(self.path)
        self._columns = get_columns(self.entity)
        self._index_cols = get_index_columns(self.entity)
        id_col = self._index_cols[0].name if len(self._index_cols) == 1 else None
        self._repo = TableRepo(
            root_path=self.path,
            id_col=id_col,
            partition_cols=self.partition_cols,
            max_records=self.max_records,
            compression=self.compression,
        )

    @property
    def polars_schema(self) -> dict[str, pl.DataType]:
        return get_polars_schema(self.entity)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self._columns]

    # --- read ---

    def read(self) -> pl.LazyFrame:
        """All rows; an unpopulated table reads as an empty, typed frame."""
        if not self.is_populated():
            return pl.LazyFrame(schema=self.polars_schema)
        return self._repo.get_full_lf()

    def collect(self) -> pl.DataFrame:
        return self.read().collect()

    def read_partition(self, partitions: dict[str, Any]) -> pl.LazyFrame:
        return self._repo.get_partition_lf(partitions)

    # --- write ---

    def write(self, df: pl.DataFrame) -> None:
        self._repo.replace_all(self._prepare(df))

    def append(self, df: pl.DataFrame) -> None:
        self._repo.extend(self._prepare(df))

    def replace(self, df: pl.DataFrame) -> None:
        self._repo.replace_records(self._prepare(df))

    def purge(self) -> None:
        self._repo.purge()

    @property
    def name(self) -> str:
        return self.path.name

    def is_populated(self) -> bool:
        return self._repo.n_files > 0

    def n_rows(self) -> int:
        return int(self.read().select(pl.len()).collect().item())

    # --- batch writers (schema-validated on every flush) ---

    def get_record_writer(
        self, batch_size: int = RecordWriter.batch_size
    ) -> RecordWriter:
        return RecordWriter(self._repo, batch_size, self._validated_extend)

    def get_df_writer(
        self, batch_size: int = DfBatchWriter.batch_size
    ) -> DfBatchWriter:
        return DfBatchWriter(self._repo, batch_size, self._validated_extend)

    # --- partitions ---

    def map_partitions(
        self, fn: Callable[[pl.DataFrame], T], *, workers: int | None = None
    ) -> list[T]:
        return self._repo.map_partitions(fn, workers=workers)

    def compact(self, **kwargs: int) -> None:
        self._repo.compact(**kwargs)

    # --- internals ---

    def _prepare(self, df: pl.DataFrame) -> pl.DataFrame:
        self._validate_columns(df)
        return self._coerce(df)

    def _validated_extend(self, repo: TableRepo, df: pl.DataFrame) -> None:
        """Flush callback for the batch writers: validate + coerce, then extend."""
        repo.extend(self._prepare(df))

    def _validate_columns(self, df: pl.DataFrame) -> None:
        expected = {c.name for c in self._columns}
        actual = set(df.columns)
        missing = expected - actual
        extra = actual - expected
        errors = []
        if missing:
            errors.append(f"missing columns: {sorted(missing)}")
        if extra:
            errors.append(f"unexpected columns: {sorted(extra)}")
        if errors:
            raise SchemaError(f"{self.entity.__name__}: {'; '.join(errors)}")

    def _coerce(self, df: pl.DataFrame) -> pl.DataFrame:
        schema = self.polars_schema
        casts = {}
        for col_name, expected_dtype in schema.items():
            actual_dtype = df.schema[col_name]
            if not _dtype_compatible(actual_dtype, expected_dtype):
                casts[col_name] = expected_dtype
        if casts:
            df = df.cast(casts)
        return df


def _dtype_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
    return actual == expected or actual.base_type() == expected.base_type()


def tables_by_name(tables: list[Table]) -> dict[str, Table]:
    """Tables keyed by ``Table.name``, which doubles as the SQL and manifest name."""
    out: dict[str, Table] = {}
    for table in tables:
        if (prev := out.setdefault(table.name, table)).path != table.path:
            raise SchemaError(
                f"two tables named {table.name!r}: {prev.path}, {table.path}"
            )
    return out
