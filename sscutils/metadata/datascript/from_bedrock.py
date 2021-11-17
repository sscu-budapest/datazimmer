from dataclasses import dataclass
from queue import Queue
from typing import Dict, Iterable, List

from ...metaprogramming import get_class_def
from ...naming import IMPORTED_NAMESPACES_SCRIPTS_PATH
from ...primitive_types import PrimitiveType
from ...utils import format_code
from ..bedrock.atoms import (
    CompositeType,
    EntityClass,
    Table,
    get_index_cls_name,
)
from ..bedrock.feature_types import (
    ANY_FEATURE_TYPE,
    ForeignKey,
    PrimitiveFeature,
)
from ..bedrock.namespace_metadata import NamespaceMetadata
from .bases import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    Nullable,
    TableFeaturesBase,
)
from .scrutable import TableFactory


@dataclass
class LocalInScriptObject:
    creation_str: str
    dependencies: List[str]


class ScriptWriter:
    def __init__(
        self,
        metadata: NamespaceMetadata,
        import_tables: bool,
    ) -> None:

        self._prefix = metadata.local_name
        self.script_path = (
            IMPORTED_NAMESPACES_SCRIPTS_PATH / f"{self._prefix}.py"
        )
        self._import_tables = import_tables
        self._table_factory = "table_factory"

        self.ssc_imports = set()
        self.local_objects: Dict[str, LocalInScriptObject] = {}
        self._table_assigns = []
        self._add_entity_classes(metadata.entity_classes)
        self._add_comp_types(metadata.composite_types)
        self._add_tables(metadata.tables)
        self._write_to_file(metadata.used_prefixes)

    def _add_entity_classes(self, ec_list: List[EntityClass]):
        for entity_class in ec_list:
            deps = []
            if len(entity_class.parents) == 0:
                self.ssc_imports.add(BaseEntity)
                parent_names = [BaseEntity.__name__]
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

        if self._import_tables:
            self.ssc_imports.add(TableFactory)
            self._table_assigns.append(
                f"{self._table_factory} = {TableFactory.__name__}"
                f'("{self._prefix}")'
            )
        for table in table_list:

            feats_cls_name = self._add_table_features(table)
            index_cls_name = self._add_table_index_cls(table)

            if self._import_tables:
                self._add_table(table, feats_cls_name, index_cls_name)

    def _write_to_file(self, imported_prefixes):
        import_names = ", ".join([c.__name__ for c in self.ssc_imports])
        import_lines = [
            "from datetime import datetime  # noqa: F401",
            f"from sscutils import {import_names}",
            *[f"from . import {p}" for p in imported_prefixes],
        ]

        out_statements = (
            import_lines + self._get_ordered_lo_defs() + self._table_assigns
        )
        out_str = format_code("\n".join(out_statements))
        self.script_path.write_text(out_str)

    def _add_table_features(self, table: Table):
        # not very pretty...
        cls_name = table.feature_cls_id
        self._add_feat_list_as_cls(table.features, cls_name, TableFeaturesBase)
        return cls_name

    def _add_table_index_cls(self, table: Table):
        if not table.index:
            return
        cls_name = table.index_cls_id
        self._add_feat_list_as_cls(table.index, cls_name, IndexBase)
        return cls_name

    def _add_table(
        self, table: Table, feats_cls_name: str, index_cls_name: str
    ):
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
        table_name = f"{table.name}_table"
        self._table_assigns.append(
            f"{table_name} = {self._table_factory}"
            f".create({scrutable_kwargs_str})"
        )

    def _get_ordered_lo_defs(self):
        defined = set()
        key_queue = Queue()
        out = []

        for k in self.local_objects.keys():
            key_queue.put(k)

        while not key_queue.empty():
            # prevent accidental inf loop here
            lo_key = key_queue.get()
            lo = self.local_objects[lo_key]
            if all([d in defined for d in lo.dependencies]):
                out.append(lo.creation_str)
                defined.add(lo_key)
            else:
                key_queue.put(lo_key)

        return out

    def _add_feat_list_as_cls(self, feat_list, cls_name, parent_cls):
        self.ssc_imports.add(parent_cls)
        deps, att_dic = self._deps_and_att_dic_from_feats(feat_list)
        self._add_cls_def(cls_name, [parent_cls.__name__], deps, att_dic)

    def _deps_and_att_dic_from_feats(self, feats: Iterable[ANY_FEATURE_TYPE]):
        deps = []
        att_dic = {}
        for feat in feats:
            full_id = feat.val_id
            if isinstance(feat, ForeignKey):
                full_id.obj_id = get_index_cls_name(full_id.obj_id)
            if full_id.ns_prefix is None and not (
                full_id.obj_id in PrimitiveType.__members__
            ):
                deps.append(full_id.datascript_obj_accessor)
            ds_obj_accessor = full_id.datascript_obj_accessor
            if isinstance(feat, PrimitiveFeature) and feat.nullable:
                ds_obj_accessor = f"{Nullable.__name__}({ds_obj_accessor})"
                self.ssc_imports.add(Nullable)
            att_dic[feat.prime_id] = ds_obj_accessor
        return deps, att_dic

    def _add_cls_def(self, cls_name, parent_names, deps, att_dict=None):
        self.local_objects[cls_name] = LocalInScriptObject(
            get_class_def(cls_name, parent_names, att_dict), dependencies=deps
        )
