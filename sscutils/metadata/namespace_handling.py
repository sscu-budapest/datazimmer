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
    ns_ids = []
    for table in ns_meta.tables:
        full_feature_list += table.features + (table.index or [])
        ns_ids.append(_pref_of_id(table.subject_of_records))

    for ct in ns_meta.composite_types:
        full_feature_list += ct.features

    for ec in ns_meta.entity_classes:
        for ecp in ec.parents or []:
            ns_ids.append(_pref_of_id(ecp))

    return list(
        set(
            filter(
                None, _ns_prefixes_of_feature_list(full_feature_list) + ns_ids
            )
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
    for ns in ns_meta.imported_namespaces:
        ns.prefix = rename_dict.get(ns.prefix, ns.prefix)

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


def _pref_of_id(id_):
    return NamespacedId.from_conf_obj_id(id_).ns_prefix
