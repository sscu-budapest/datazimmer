from functools import cached_property, partial
from typing import Dict, Iterable, Type

from ...metaprogramming import get_simplified_mro
from ...module_tree import ModuleTree
from ...naming import META_MODULE_NAME
from ...utils import (
    PRIMITIVE_MODULES,
    get_cls_defined_in_module,
    get_instances_from_module,
    get_modules_from_module,
)
from ..bedrock.atoms import CompositeType, EntityClass, Table
from ..bedrock.complete_id import CompleteId, CompleteIdBase
from ..bedrock.feature_types import CompositeFeature, ForeignKey, PrimitiveFeature
from ..bedrock.namespace_metadata import NamespaceMetadata
from .bases import BaseEntity, CompositeTypeBase, IndexBase, Nullable, get_feature_dict
from .scrutable import ScruTable


class DatascriptToBedrockConverter:
    def __init__(self, name) -> None:
        module_tree = ModuleTree()
        self._namespaces = set()
        self.repos: Iterable[DatascriptObjectCollector] = []
        self._ind_table_dict = {}
        self.tables = {}
        self.composite_types = {}
        self.entity_classes = {}
        for module in module_tree.all_modules:
            int_repo = DatascriptObjectCollector(module)
            self._fill_table_dic(int_repo)
            for new_index in int_repo.index_classes:
                _base = CompleteIdBase.from_cls(new_index)
                if _base.artifact is None:
                    continue
                ext_repo = DatascriptObjectCollector(_base.ext_module_name)
                self._fill_table_dic(ext_repo)
            for ext_collector in int_repo.meta_module_collectors:
                self._fill_table_dic(ext_collector)
            self.repos.append(int_repo)

        for repo in self.repos:
            module_name = repo.namespace_module.__name__
            self._id_base = CompleteIdBase.from_module_name(module_name, name)
            for ds_obj in repo.atoms:
                self._parse(ds_obj)
            self._namespaces.add(self._id_base.namespace)

    def get_namespaces(self) -> Iterable[NamespaceMetadata]:
        _arg_bases = [self.composite_types, self.entity_classes, self.tables]
        for _ns in self._namespaces:
            ns_args = [*map(partial(_dic_by_ns, _ns), _arg_bases)]
            if any(ns_args):
                yield NamespaceMetadata(*ns_args, _ns)

    def _fill_table_dic(self, repo: "DatascriptObjectCollector"):
        for new_table in repo.tables:
            if new_table.index is None:
                continue
            self._ind_table_dict[new_table.index] = new_table.name

    def _parse(self, atom):
        if isinstance(atom, ScruTable):
            return self._add_scrutable(atom)

        ns_id = self._get_complete_id(atom)
        if not ns_id.is_local:
            return
        desc = atom.__doc__
        self._add_ds_cls(atom, ns_id, desc)

    def _add_scrutable(self, scrutable: ScruTable):
        if not self._get_complete_id(scrutable.features).is_local:
            return
        ec = scrutable.subject
        self._parse(ec)
        table = Table(
            name=scrutable.name,
            features=self._feature_cls_to_bedrock_list(scrutable.features),
            subject_of_records=self._ser_id(ec),
            index=self._feature_cls_to_bedrock_list(scrutable.index),
            partitioning_cols=scrutable.partitioning_cols,
            partition_max_rows=scrutable.max_partition_size,
            description=scrutable.features.__doc__,
        )
        self.tables[self._key(table.name)] = table

    def _add_ds_cls(self, cls, ns_id: CompleteId, desc):
        if BaseEntity in cls.mro():
            out_cls = EntityClass
            in_dic = self.entity_classes
            cls_kwargs = self._ec_kwargs(cls)
        elif CompositeTypeBase in cls.mro():
            out_cls = CompositeType
            in_dic = self.composite_types
            cls_kwargs = self._ct_kwargs(cls)
        obj = out_cls(**{"name": ns_id.obj_id, "description": desc, **cls_kwargs})
        in_dic[self._key(ns_id.obj_id)] = obj

    def _ec_kwargs(self, ec_cls: Type[BaseEntity]):
        simp_mro = get_simplified_mro(ec_cls)
        parents = [self._ser_id(p) for p in simp_mro if p is not BaseEntity]
        return dict(parents=parents)

    def _ct_kwargs(self, ct_cls: Type[CompositeTypeBase]):
        return dict(features=self._feature_cls_to_bedrock_list(ct_cls))

    def _feature_cls_to_bedrock_list(self, cls):
        if cls is None:
            return []
        return self._parse_feature_dict(get_feature_dict(cls))

    def _parse_feature_dict(self, feature_dict: Dict[str, Type]):
        out = []
        for k, cls in feature_dict.items():
            if isinstance(cls, Nullable):
                cls = cls.base
                nullable = True
            else:
                nullable = False
            full_id = self._get_complete_id(cls)
            if cls.__module__ in PRIMITIVE_MODULES:
                parsed_feat = PrimitiveFeature(
                    name=k, dtype=full_id.serialized_id, nullable=nullable
                )
            elif IndexBase in cls.mro():
                full_id.obj_id = self._table_from_index(cls)
                parsed_feat = ForeignKey(prefix=k, table=full_id.serialized_id)
            else:
                parsed_feat = CompositeFeature(prefix=k, dtype=full_id.serialized_id)
            out.append(parsed_feat)
        return out

    def _table_from_index(self, index_cls):
        try:
            return self._ind_table_dict[index_cls]
        except KeyError:
            raise KeyError(
                f"Can't find table for index class {index_cls}"
                f" in dict {self._ind_table_dict}"
            )

    def _get_complete_id(self, cls) -> CompleteId:
        return CompleteId.from_datascript_cls(cls).relative_to(self._id_base)

    def _key(self, obj_id):
        return (self._id_base.namespace, obj_id)

    def _ser_id(self, cls) -> str:
        return self._get_complete_id(cls).serialized_id


class DatascriptObjectCollector:
    def __init__(self, module) -> None:
        self.namespace_module = module

    @cached_property
    def atoms(self):
        return [*self.tables, *self.composite_types, *self.entity_classes]

    @cached_property
    def tables(self) -> Iterable[ScruTable]:
        return get_instances_from_module(self.namespace_module, ScruTable).values()

    @cached_property
    def composite_types(self) -> Iterable[Type[CompositeTypeBase]]:
        return self._obj_of_cls(CompositeTypeBase)

    @cached_property
    def entity_classes(self) -> Iterable[Type[BaseEntity]]:
        return self._obj_of_cls(BaseEntity)

    @cached_property
    def index_classes(self) -> Iterable[Type[BaseEntity]]:
        return self._obj_of_cls(IndexBase)

    @property
    def meta_module_collectors(self):
        mods = get_modules_from_module(self.namespace_module, META_MODULE_NAME).values()
        return map(type(self), mods)

    def _obj_of_cls(self, py_cls):
        return get_cls_defined_in_module(self.namespace_module, py_cls).values()


def _dic_by_ns(ns, dic):
    return [v for k, v in dic.items() if k[0] == ns]
