from functools import reduce
from typing import Type, TypeVar, Union

from colassigner import ColAccessor, ColAssigner
from colassigner.type_hinting import get_return_hint

T = TypeVar("T")


class SourceUrl(str):
    pass


class AbstractEntity(ColAssigner):
    pass


class CompositeTypeBase(ColAccessor):
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

    base_items = reduce(or_, [c.__dict__ for c in [*cls.__bases__, cls]]).items()
    out = {k: _get_feat_type(v) for k, v in base_items if not k.startswith("_")}
    return out


def _get_feat_type(attval):
    return get_return_hint(attval) or attval


def or_(d1, d2):
    return {**d1, **d2}
