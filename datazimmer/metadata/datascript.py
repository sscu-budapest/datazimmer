from datetime import datetime
from enum import Enum
from typing import Type, TypeVar, Union

import sqlalchemy as sa
from colassigner import ColAssigner, get_att_value
from colassigner.type_hinting import get_return_hint

from ..utils import get_creation_module_name

T = TypeVar("T")

SA_TYPE_MAP = {
    int: sa.Integer,
    float: sa.Float,
    str: sa.String,
    bytes: sa.LargeBinary,
    bool: sa.Boolean,
    datetime: sa.DateTime,
}


class PrimitiveType(Enum):
    # TODO: categorical
    float = float
    int = int
    str = str
    bytes = bytes
    bool = bool
    datetime = datetime


class SourceUrl(str):
    def __init__(self, _) -> None:
        super().__init__()  # i don't know why this does not need arg
        self.__module__ = get_creation_module_name()


class AbstractEntity(ColAssigner):
    pass


class CompositeTypeBase(ColAssigner):
    pass


class Nullable(type):
    base: Type

    def __new__(cls, dtype):
        return super().__new__(cls, dtype.__name__, (), {"base": dtype})


class _IMeta(type):
    def __and__(cls, other: T) -> T:
        return type(other.__name__, (other, IndexIndicator), {})


class Index(metaclass=_IMeta):
    pass


class IndexIndicator:
    pass


def get_feature_dict(cls: Union[CompositeTypeBase, AbstractEntity]):
    feat_dict = {}
    for attid in dir(cls):
        if attid.startswith("_"):
            continue
        att_val = get_att_value(cls, attid)
        feat_dict[attid] = get_return_hint(att_val) or att_val
    return feat_dict


def get_sa_type(dtype: Type):
    return SA_TYPE_MAP[dtype]


def get_np_type(dtype: Type, nullable: bool):
    if nullable and (dtype == str):
        dtype = object
    return {datetime: "datetime64[ns]"}.get(dtype, dtype)
