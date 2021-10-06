from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union


class DType(Enum):
    # TODO: categorical
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
    table: str  # an ID, that needs to link to a table
    prefix: str
    description: Optional[str] = None


@dataclass
class ColumnGroupInstance:
    group_name: str  # an ID, that needs to link to a group
    prefix: str
    description: Optional[str] = None


ANY_COL_ROOT_TYPE = Union[Column, ColumnGroupInstance, ForeignKey]


@dataclass
class ColumnGroup:
    name: str
    columns: List[ANY_COL_ROOT_TYPE]
    description: Optional[str] = None


@dataclass
class Table:
    """TODO: document restrictions here

    or find new location for them

    e. g.
    - no column group instances with same prefix in same table
    - column group needs to be found from instance
    - column name cant contain __ (and other)

    """

    name: str
    columns: List[ANY_COL_ROOT_TYPE]
    subject_of_records: str
    index_cols: Optional[List[ANY_COL_ROOT_TYPE]] = None
    partitioning_cols: Optional[List[ANY_COL_ROOT_TYPE]] = None
    partition_max_rows: Optional[int] = None
    description: Optional[str] = None
