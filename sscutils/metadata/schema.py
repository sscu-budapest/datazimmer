from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union


class DType(Enum):
    float = float
    int = int
    str = str
    bytes = bytes
    bool = bool
    datetime = datetime


@dataclass
class Column:
    name: str
    dtype: DType
    description: Optional[str] = None


@dataclass
class ForeignKey:
    name: str
    dtype: DType
    subject: str
    description: Optional[str] = None


@dataclass
class ColumnGroupInstance:
    group_name: str
    instance_name: str
    description: Optional[str]


ANY_COL_TYPE = Union[Column, ColumnGroupInstance, ForeignKey]


@dataclass
class ColumnGroup:
    name: str
    columns: List[ANY_COL_TYPE]
    description: Optional[str] = None


@dataclass
class Table:
    name: str
    columns: List[ANY_COL_TYPE]
    subject_of_records: str
    index_cols: Optional[List[ANY_COL_TYPE]] = None
    partitioning_cols: Optional[List[ANY_COL_TYPE]] = None
    partition_max_rows: Optional[int] = None
    description: Optional[str] = None
