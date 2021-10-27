from typing import Iterable, Tuple

from ..metaprogramming import snake_to_camel
from .namespaced_id import NamespacedId
from .schema import CompositeFeature, ForeignKey, PrimitiveFeature


def feature_to_items(feat, fk_to_ind=False) -> Tuple[str, NamespacedId]:
    if isinstance(feat, PrimitiveFeature):
        return feat.name, NamespacedId.from_conf_obj_id(feat.dtype)
    elif isinstance(feat, CompositeFeature):
        return feat.prefix, NamespacedId.from_conf_obj_id(feat.dtype)
    elif isinstance(feat, ForeignKey):
        val = NamespacedId.from_conf_obj_id(feat.table)
        if fk_to_ind:
            val = table_id_to_index_cls_name(val)
        return feat.prefix, val
    else:
        raise TypeError(f"{type(feat)} {feat} is not a feature type")


def feature_list_items(
    feature_list, fk_to_ind=False
) -> Iterable[Tuple[str, NamespacedId]]:
    for feat in feature_list:
        yield feature_to_items(feat, fk_to_ind)


def table_id_to_index_cls_name(table_id: NamespacedId):
    return _add_suffix(table_id, "Index")


def table_id_to_feature_cls_name(table_id: NamespacedId):
    # scrutable cls knows this suffix...
    return _add_suffix(table_id, "Features")


def _add_suffix(full_id: NamespacedId, suffix: str):
    full_id.obj_id = f"{snake_to_camel(full_id.obj_id)}{suffix}"
    return full_id
