from datetime import datetime
from enum import Enum
from typing import Type

import sqlalchemy as sa


class PrimitiveType(Enum):
    # TODO: categorical
    float = float
    int = int
    str = str
    bytes = bytes
    bool = bool
    datetime = datetime


def get_sa_type(dtype: Type):
    return sa_type_map[dtype]


def get_np_type(dtype: Type):
    return {datetime: "datetime64", str: object}.get(dtype, dtype)


sa_type_map = {
    int: sa.Integer,
    float: sa.Float,
    str: sa.String,
    bytes: sa.LargeBinary,
    bool: sa.Boolean,
    datetime: sa.DateTime,
}
