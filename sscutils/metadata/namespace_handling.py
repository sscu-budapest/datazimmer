from typing import List

from .conf_obj_converters import feature_list_items
from .namespaced_id import NamespacedId
from .schema import (
    FEATURE_TYPE,
    CompositeFeature,
    ForeignKey,
    NamespaceMetadata,
)


def get_used_ns_prefixes(ns_meta: NamespaceMetadata) -> List[str]:
    full_feature_list = []
    full_id_list = []
    for table in ns_meta.tables:
        full_feature_list += table.features + (table.index or [])
        _append_if_ns(table.subject_of_records, full_id_list)

    for ct in ns_meta.composite_types:
        full_feature_list += ct.features

    for ec in ns_meta.entity_classes:
        for ecp in ec.parents or []:
            _append_if_ns(ecp, full_id_list)

    return list(
        set(
            _ns_prefixes_of_feature_list(full_feature_list)
            + _ns_prefixes_of_id_list(full_id_list)
        )
    )


def map_ns_prefixes(ns_meta: NamespaceMetadata, rename_dict: dict):
    """changes ns_meta inplace

    Parameters
    ----------
    ns_meta : NamespaceMetadata
        metadata to change
    rename_dict : dict
        old_prefix: new_prefix
    """

    for table in ns_meta.tables:
        _change_feat_list_prefix(table.index, rename_dict)
        _change_feat_list_prefix(table.features, rename_dict)
        table.subject_of_records = _change_prefix(
            table.subject_of_records, rename_dict
        )

    for ct in ns_meta.composite_types:
        _change_feat_list_prefix(ct.features, rename_dict)

    for ec in ns_meta.entity_classes:
        if ec.parents is None:
            continue
        ec.parents = [
            _change_prefix(parent, rename_dict) for parent in ec.parents
        ]


def filter_for_local_ns(elems):
    return [e for e in elems if NamespacedId.from_conf_obj_id(e.name).is_local]


def _ns_prefixes_of_feature_list(feature_list=List[FEATURE_TYPE]) -> List[str]:
    return [
        full_id.ns_prefix for _, full_id in feature_list_items(feature_list)
    ]


def _ns_prefixes_of_id_list(id_list: List[str]):
    out = []
    for id_str in id_list:
        _append_if_ns(id_str, out)
    return list(set(out))


def _change_prefix(id_, pref_dict):
    full_id = NamespacedId.from_conf_obj_id(id_)
    old_prefix = full_id.ns_prefix
    full_id.ns_prefix = pref_dict.get(old_prefix, old_prefix)
    return full_id.conf_obj_id


def _change_feat_list_prefix(feat_list, pref_dict):
    for feat in feat_list or []:
        if isinstance(feat, ForeignKey):
            feat.table = _change_prefix(feat.table, pref_dict)
        elif isinstance(feat, CompositeFeature):
            feat.dtype = _change_prefix(feat.dtype, pref_dict)


def _append_if_ns(id_, ns_list):
    ns_prefix = NamespacedId.from_conf_obj_id(id_).ns_prefix
    if ns_prefix:
        ns_list.append(ns_prefix)
