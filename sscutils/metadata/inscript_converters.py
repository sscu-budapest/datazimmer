from dataclasses import dataclass
from importlib import import_module
from queue import Queue
from typing import Dict, Iterable, List, Tuple, Type

from ..config_loading import ProjectConfig
from ..exceptions import ProjectSetupException
from ..metaprogramming import get_class_def, get_simplified_mro
from ..naming import IMPORTED_NAMESPACES_MODULE_NAME, SRC_PATH
from ..scrutable_class import ScruTable, TableFactory
from ..utils import (
    PRIMITIVE_MODULES,
    format_code,
    get_cls_defined_in_module,
    get_instances_from_module,
)
from .bases import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    TableFeaturesBase,
    get_feature_dict,
)
from .conf_obj_converters import (
    feature_list_items,
    table_id_to_feature_cls_name,
    table_id_to_index_cls_name,
)
from .io import (
    dump_imported_namespaces,
    load_imported_namespaces,
    load_metadata_from_imported_ns,
)
from .namespace_handling import filter_for_local_ns, map_ns_prefixes
from .namespaced_id import NamespacedId, namespace_metadata_abs_module
from .schema import (
    CompositeFeature,
    CompositeType,
    EntityClass,
    ForeignKey,
    ImportedNamespace,
    NamespaceMetadata,
    PrimitiveFeature,
    Table,
)


@dataclass
class LocalInScriptObject:
    creation_str: str
    dependencies: List[str]


class ScriptWriter:
    def __init__(
        self,
        metadata: NamespaceMetadata,
        prefix,
        import_tables: bool,
    ) -> None:

        prefix_rename_map = _resolve_namespace_import_tree(
            metadata.imported_namespaces
        )

        map_ns_prefixes(metadata, prefix_rename_map)

        self.script_path = (
            SRC_PATH / IMPORTED_NAMESPACES_MODULE_NAME / f"{prefix}.py"
        )
        self._import_tables = import_tables
        self._prefix = prefix

        self.ssc_imports = set()
        self.local_objects: Dict[str, LocalInScriptObject] = {}
        self._add_entity_classes(metadata.entity_classes)
        self._add_comp_types(metadata.composite_types)
        self._table_assigns = self._add_tables(metadata.tables)
        self._write_to_file(metadata.imported_namespaces)

    def _add_entity_classes(self, ec_list: List[EntityClass]):
        for entity_class in ec_list:
            deps = []
            if entity_class.parents is None:
                self.ssc_imports.add(BaseEntity)
                parent_names = [BaseEntity.__name__]
            else:
                parent_names = []
                for parent in entity_class.parents:
                    full_id = NamespacedId.from_conf_obj_id(parent)
                    if full_id.ns_prefix is None:
                        deps.append(parent)
                    parent_names.append(full_id.py_obj_accessor)
            self._add_cls_def(entity_class.name, parent_names, deps)

    def _add_comp_types(self, ct_list: List[CompositeType]):
        for comp_type in ct_list:
            self._add_feat_list_as_cls(
                comp_type.features, comp_type.name, CompositeTypeBase
            )

    def _add_tables(self, table_list: List[Table]):
        table_assigns = []
        factory_pyobj = "table_factory"
        if self._import_tables:
            self.ssc_imports.add(TableFactory)
            table_assigns.append(
                f'{factory_pyobj} = {TableFactory.__name__}("{self._prefix}")'
            )
        for table in table_list:

            feats_cls_name = self._add_table_features(table)
            index_cls_name = self._add_table_index_cls(table)

            if not self._import_tables:
                continue

            scrutable_kwargs = {
                "features": feats_cls_name,
                "subject_of_records": NamespacedId.from_conf_obj_id(
                    table.subject_of_records
                ).py_obj_accessor,
            }
            if index_cls_name:
                scrutable_kwargs["index"] = index_cls_name
            if table.partitioning_features:
                scrutable_kwargs[
                    "partitioning_cols"
                ] = table.partitioning_features
            if table.partition_max_rows:
                scrutable_kwargs[
                    "max_partition_size"
                ] = table.partition_max_rows

            scrutable_kwargs_str = ", ".join(
                [f"{k}={v}" for k, v in scrutable_kwargs.items()]
            )
            table_name = f"{table.name}_table"
            table_assigns.append(
                f"{table_name} = {factory_pyobj}"
                f".create({scrutable_kwargs_str})"
            )
        return table_assigns

    def _write_to_file(self, imported_namespaces):
        import_names = ", ".join([c.__name__ for c in self.ssc_imports])
        import_lines = [
            "from datetime import datetime",  # not always but anyway
            f"from sscutils import {import_names}",
            *imported_namespaces_to_import_statements(imported_namespaces),
        ]

        out_statements = (
            import_lines + self._get_ordered_lo_defs() + self._table_assigns
        )
        out_str = format_code("\n".join(out_statements))
        self.script_path.write_text(out_str)

    def _add_table_features(self, table: Table):
        # not very pretty...
        cls_name = table_id_to_feature_cls_name(
            NamespacedId.from_conf_obj_id(table.name)
        ).py_obj_accessor
        self._add_feat_list_as_cls(table.features, cls_name, TableFeaturesBase)
        return cls_name

    def _add_table_index_cls(self, table: Table):
        if table.index is None:
            return
        cls_name = table_id_to_index_cls_name(
            NamespacedId.from_conf_obj_id(table.name)
        ).py_obj_accessor
        self._add_feat_list_as_cls(table.index, cls_name, IndexBase)
        return cls_name

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

    def _deps_and_att_dic_from_feats(self, feats):
        deps = []
        att_dic = {}
        for fik, full_id in feature_list_items(feats, fk_to_ind=True):
            if full_id.ns_prefix is None and not full_id.is_primitive:
                deps.append(full_id.py_obj_accessor)
            att_dic[fik] = full_id.py_obj_accessor
        return deps, att_dic

    def _add_cls_def(self, cls_name, parent_names, deps, att_dict=None):
        self.local_objects[cls_name] = LocalInScriptObject(
            get_class_def(cls_name, parent_names, att_dict), dependencies=deps
        )


class PyObjectToConfObjectConverter:
    def __init__(self, script_tables: Iterable[ScruTable]) -> None:

        self._ind_table_dict = {
            sct.index: sct.name for sct in script_tables if sct.index
        }

    def entity_class_to_conf_obj(
        self, ec_cls: Type[BaseEntity]
    ) -> EntityClass:
        ec_full_id = NamespacedId.from_py_cls(ec_cls)
        simp_mro = get_simplified_mro(ec_cls)
        if simp_mro == [BaseEntity]:
            parents = None
        else:
            parents = [
                NamespacedId.from_py_cls(p).conf_obj_id for p in simp_mro
            ]
        return EntityClass(
            ec_full_id.conf_obj_id, parents=parents, description=ec_cls.__doc__
        )

    def comp_type_to_conf_obj(
        self, ct_cls: Type[CompositeTypeBase]
    ) -> CompositeType:
        full_id = NamespacedId.from_py_cls(ct_cls)
        return CompositeType(
            name=full_id.conf_obj_id,
            features=self._feature_cls_to_conf_obj_list(ct_cls),
            description=ct_cls.__doc__,
        )

    def scrutable_to_conf_obj(
        self, scrutable: ScruTable
    ) -> Tuple[Table, EntityClass]:
        ec = scrutable.subject
        parsed_ec = self.entity_class_to_conf_obj(ec)
        table = Table(
            name=scrutable.name,
            features=self._feature_cls_to_conf_obj_list(scrutable.features),
            subject_of_records=parsed_ec.name,
            index=self._feature_cls_to_conf_obj_list(scrutable.index),
            partitioning_features=scrutable.partitioning_cols,
            partition_max_rows=scrutable.max_partition_size,
        )
        return table, parsed_ec

    def _feature_cls_to_conf_obj_list(self, cls):
        if cls is None:
            return None
        return self._parse_feature_dict(get_feature_dict(cls))

    def _parse_feature_dict(self, feature_dict: Dict[str, Type]):
        out = []
        for k, cls in feature_dict.items():
            full_id = NamespacedId.from_py_cls(cls)
            if cls.__module__ in PRIMITIVE_MODULES:
                parsed_feat = PrimitiveFeature(
                    name=k, dtype=full_id.conf_obj_id
                )
            elif IndexBase in cls.mro():
                try:
                    full_id.obj_id = self._ind_table_dict[cls]
                except KeyError:
                    raise KeyError(
                        f"Can't find table for index class {cls}"
                        f" in dict {self._ind_table_dict}"
                    )

                parsed_feat = ForeignKey(prefix=k, table=full_id.conf_obj_id)
            else:
                parsed_feat = CompositeFeature(
                    prefix=k, dtype=full_id.conf_obj_id
                )
            out.append(parsed_feat)
        return out


def import_metadata_to_script(ns: ImportedNamespace):
    """writes to script file

    so that metadata can be imported in script

    also resolves imported namespace tree


    optionally writes to imported namespaces metadata
    if a namespace is already imported with a different prefix,
    has to modify the imported full metadata

    Parameters
    ----------
    ns : ImportedNamespace
        needs to be a namespace where metadata is already serialized
    """
    metadata_to_import = load_metadata_from_imported_ns(ns)
    try:
        include_tables = ProjectConfig().has_data_env(ns.prefix)
    except ProjectSetupException:
        include_tables = False
    writer = ScriptWriter(metadata_to_import, ns.prefix, include_tables)

    return [writer.script_path.as_posix()]


def load_metadata_from_dataset_script() -> NamespaceMetadata:
    """loads namespace metadata from script

    Returns
    -------
    NamespaceMetadata
        metadata defined in the script
        where
        - types of features
        - subjects of tables
        - parents of entity classes
        can refer to ones defined elsewhere
    """

    imported_namespaces = load_imported_namespaces()
    namespace_module = import_module(namespace_metadata_abs_module)

    script_tables: Iterable[ScruTable] = get_instances_from_module(
        namespace_module, ScruTable
    ).values()
    converter = PyObjectToConfObjectConverter(script_tables)

    comp_types, entity_classes = [
        [
            *map(
                _fun,
                get_cls_defined_in_module(namespace_module, _cls).values(),
            )
        ]
        for _cls, _fun in [
            (CompositeTypeBase, converter.comp_type_to_conf_obj),
            (BaseEntity, converter.entity_class_to_conf_obj),
        ]
    ]

    tables = []

    for in_script_table in script_tables:
        table, e_class = converter.scrutable_to_conf_obj(in_script_table)
        tables.append(table)
        entity_classes.append(e_class)

    return NamespaceMetadata(
        imported_namespaces=imported_namespaces,
        composite_types=filter_for_local_ns(comp_types),
        tables=filter_for_local_ns(tables),
        entity_classes=filter_for_local_ns(entity_classes),
    )


def imported_namespaces_to_import_statements(ns_list: List[ImportedNamespace]):
    out = []
    for ns_imp in ns_list:
        out.append(f"from . import {ns_imp.prefix}")
    return out


def _resolve_namespace_import_tree(
    imported_namespaces: List[ImportedNamespace],
):
    """resolve imported namespace tree
    return rename dict
    add new imported namespaces and import them
    map prefixes that are imported
    if a prefix is already in use locally, rename it"""

    local_namespaces = load_imported_namespaces()
    namespace_dict = {ns.prefix: ns for ns in local_namespaces}

    prefix_map = {}
    for ns_external in imported_namespaces:
        currently_under_prefix = namespace_dict.get(ns_external.prefix)
        new_prefix = ns_external.prefix
        if currently_under_prefix is not None:
            if currently_under_prefix == ns_external:
                continue
            # find if imported
            new_prefix = _create_new_prefix_for_ns(
                ns_external.prefix, namespace_dict.keys()
            )

        for ns_local in local_namespaces:
            if ns_external == ns_local:
                prefix_map[ns_external.prefix] = ns_local.prefix
                break
        else:
            ns_external.prefix = new_prefix
            local_namespaces.append(ns_external)
            dump_imported_namespaces(local_namespaces)
            import_metadata_to_script(ns_external)
    return prefix_map


def _create_new_prefix_for_ns(old_prefix, used_prefixes):
    n = 2
    while True:
        new_prefix = f"{old_prefix}_{n}"
        if new_prefix not in used_prefixes:
            return new_prefix
        n += 1
