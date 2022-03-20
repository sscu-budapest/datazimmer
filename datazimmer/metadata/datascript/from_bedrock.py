from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import Dict, Iterable, List

from ...metaprogramming import get_class_def
from ...naming import PACKAGE_SHORTHAND
from ...primitive_types import PrimitiveType
from ...utils import format_code
from ..bedrock.atoms import CompositeType, EntityClass, Table, get_index_cls_name
from ..bedrock.complete_id import CompleteId
from ..bedrock.feature_types import ANY_FEATURE_TYPE, ForeignKey, PrimitiveFeature
from ..bedrock.namespace_metadata import NamespaceMetadata
from .bases import BaseEntity, CompositeTypeBase, IndexBase, Nullable, TableFeaturesBase
from .scrutable import ScruTable


@dataclass
class LocalInScriptObject:
    creation_str: str
    dependencies: List[str]


class ScriptWriter:
    def __init__(self, metadata: NamespaceMetadata, path: Path) -> None:

        self.script_path = path
        self.zimmer_imports = set()
        self.local_objects: Dict[str, LocalInScriptObject] = {}
        self._table_assigns = []
        self._add_entity_classes(metadata.entity_classes)
        self._add_comp_types(metadata.composite_types)
        self._add_tables(metadata.tables)
        self._write_to_file(_get_import_strings(metadata))

    def _add_entity_classes(self, ec_list: List[EntityClass]):
        for entity_class in ec_list:
            deps = []
            if len(entity_class.parents) == 0:
                parent_names = [f"{PACKAGE_SHORTHAND}.{BaseEntity.__name__}"]
            else:
                parent_names = []
                for parent in entity_class.parents:
                    if parent.is_local:
                        deps.append(parent.obj_id)
                    parent_names.append(parent.datascript_obj_accessor)
            self._add_cls_def(entity_class.name, parent_names, deps)

    def _add_comp_types(self, ct_list: List[CompositeType]):
        for comp_type in ct_list:
            self._add_feat_list_as_cls(
                comp_type.features, comp_type.name, CompositeTypeBase
            )

    def _add_tables(self, table_list: List[Table]):

        for table in table_list:
            feats_cls_name = self._add_table_features(table)
            index_cls_name = self._add_table_index_cls(table)
            self._add_table(table, feats_cls_name, index_cls_name)

    def _write_to_file(self, import_strings):
        import_lines = [
            "from datetime import datetime  # noqa: F401",
            f"import datazimmer as {PACKAGE_SHORTHAND}",
            *import_strings,
        ]

        out_statements = (
            import_lines + self._get_ordered_lo_defs() + self._table_assigns
        )
        out_str = format_code("\n".join(out_statements))
        self.script_path.write_text(out_str)

    def _add_table_features(self, table: Table):
        # not very pretty
        cls_name = table.feature_cls_id
        self._add_feat_list_as_cls(table.features, cls_name, TableFeaturesBase)
        return cls_name

    def _add_table_index_cls(self, table: Table):
        if not table.index:
            return
        cls_name = table.index_cls_id
        self._add_feat_list_as_cls(table.index, cls_name, IndexBase)
        return cls_name

    def _add_table(self, table: Table, feats_cls_name: str, index_cls_name: str):
        ec_obj = table.subject_of_records.datascript_obj_accessor
        scrutable_kwargs = {
            "features": feats_cls_name,
            "subject_of_records": ec_obj,
        }
        if index_cls_name:
            scrutable_kwargs["index"] = index_cls_name
        if table.partitioning_cols:
            scrutable_kwargs["partitioning_cols"] = table.partitioning_cols
        if table.partition_max_rows:
            scrutable_kwargs["max_partition_size"] = table.partition_max_rows

        scrutable_kwargs_str = ", ".join(
            [f"{k}={v}" for k, v in scrutable_kwargs.items()]
        )
        tab_cls = f"{PACKAGE_SHORTHAND}.{ScruTable.__name__}"
        table_ass = f"{table.name}_table = {tab_cls}({scrutable_kwargs_str})"
        self._table_assigns.append(table_ass)

    def _get_ordered_lo_defs(self):
        defined = set()
        key_queue = Queue()
        out = []

        for k in self.local_objects.keys():
            key_queue.put(k)

        while not key_queue.empty():
            # TODO: prevent accidental inf loop here
            # dependency can be detected
            # when statement is not, e.g. in dogsuccess
            lo_key = key_queue.get()
            lo = self.local_objects[lo_key]
            if all([d in defined for d in lo.dependencies]):
                out.append(lo.creation_str)
                defined.add(lo_key)
            else:
                key_queue.put(lo_key)

        return out

    def _add_feat_list_as_cls(self, feat_list, cls_name, parent_cls):
        deps, att_dic = self._deps_and_att_dic_from_feats(feat_list)
        cls_acc = f"{PACKAGE_SHORTHAND}.{parent_cls.__name__}"
        self._add_cls_def(cls_name, [cls_acc], deps, att_dic)

    def _deps_and_att_dic_from_feats(self, feats: Iterable[ANY_FEATURE_TYPE]):
        deps = []
        att_dic = {}
        for feat in feats:
            full_id = feat.val_id
            if isinstance(feat, ForeignKey):
                ind_cls = get_index_cls_name(full_id.obj_id)
                full_id = CompleteId(full_id.artifact, full_id.namespace, ind_cls)
            if full_id.namespace is None and not (
                full_id.obj_id in PrimitiveType.__members__
            ):
                deps.append(full_id.datascript_obj_accessor)
            ds_obj_accessor = full_id.datascript_obj_accessor
            if isinstance(feat, PrimitiveFeature) and feat.nullable:
                null_cls = f"{PACKAGE_SHORTHAND}.{Nullable.__name__}"
                ds_obj_accessor = f"{null_cls}({ds_obj_accessor})"
            att_dic[feat.prime_id] = ds_obj_accessor

        return deps, att_dic

    def _add_cls_def(self, cls_name, parent_names, deps, att_dict=None):
        self.local_objects[cls_name] = LocalInScriptObject(
            get_class_def(cls_name, parent_names, att_dict), dependencies=deps
        )


def _get_import_strings(ns_meta: NamespaceMetadata):
    feature_list: List[ANY_FEATURE_TYPE] = []
    init_list = []
    for table in ns_meta.tables:
        feature_list += table.features_w_ind
        init_list.append(table.subject_of_records.base.import_str)

    for ct in ns_meta.composite_types:
        feature_list += ct.features

    for ec in ns_meta.entity_classes:
        for ecp in ec.parents:
            init_list.append(ecp.base.import_str)

    full_list = [feat.val_id.base.import_str for feat in feature_list] + init_list
    return list(set(filter(None, full_list)))
