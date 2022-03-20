from functools import reduce
from typing import Type, Union

from colassigner import ColAccessor, ColAssigner
from colassigner.type_hinting import get_return_hint


class BaseEntity:
    pass


class IndexBase(ColAccessor):
    pass


class CompositeTypeBase(ColAccessor):
    pass


class TableFeaturesBase(ColAssigner):
    pass


class Nullable(type):

    base: Type

    def __new__(cls, dtype):
        return super().__new__(cls, dtype.__name__, (), {"base": dtype})


def get_feature_dict(cls: Union[CompositeTypeBase, TableFeaturesBase]):

    base_items = reduce(or_, [c.__dict__ for c in [*cls.__bases__, cls]]).items()
    out = {k: _get_feat_type(v) for k, v in base_items if not k.startswith("_")}
    return out


def _get_feat_type(attval):
    return get_return_hint(attval) or attval


def or_(d1, d2):
    return {**d1, **d2}
