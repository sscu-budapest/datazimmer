from typing import Type, TypeVar, Union

from colassigner import ColAssigner, get_att_value
from colassigner.type_hinting import get_return_hint

T = TypeVar("T")


class SourceUrl(str):
    pass


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
