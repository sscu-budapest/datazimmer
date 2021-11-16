from dataclasses import dataclass
from typing import List

import sqlalchemy as sa

from ...primitive_types import PrimitiveType, get_np_type, get_sa_type


@dataclass
class Column:
    name: str
    dtype: PrimitiveType


def to_sql_col(col: Column):
    return sa.Column(col.name, get_sa_type(col.dtype))


def to_dt_map(cols: List[Column]):
    return {c.name: get_np_type(c.dtype) for c in cols}
