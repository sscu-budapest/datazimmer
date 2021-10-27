from typing import Union

from colassigner import ColAccessor

from .type_hinting import get_return_col_type


class BaseEntity:
    pass


class IndexBase(ColAccessor):
    pass


class CompositeTypeBase(ColAccessor):
    pass


class TableFeaturesBase(ColAccessor):
    pass


def get_feature_dict(cls: Union[CompositeTypeBase, TableFeaturesBase]):
    return {
        k: _get_feat_type(v)
        for k, v in cls.__dict__.items()
        if not k.startswith("_")
    }


def _get_feat_type(attval):
    try:
        return get_return_col_type(attval)
    except (AssertionError, AttributeError, KeyError):
        return attval
