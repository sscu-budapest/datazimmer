from typing import Union

from colassigner import ColAccessor


class BaseEntity:
    pass


class IndexBase(ColAccessor):
    pass


class CompositeTypeBase(ColAccessor):
    pass


class TableFeaturesBase(ColAccessor):
    pass


def get_feature_dict(cls: Union[CompositeTypeBase, TableFeaturesBase]):
    return {k: v for k, v in cls.__dict__.items() if not k.startswith("_")}
