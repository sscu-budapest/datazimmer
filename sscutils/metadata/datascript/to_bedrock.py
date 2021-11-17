from importlib import import_module
from typing import Dict, Iterable, Type

from ...helpers import get_top_module_name
from ...metaprogramming import get_simplified_mro
from ...naming import TMP_CLS_MODULE
from ...utils import (
    PRIMITIVE_MODULES,
    get_cls_defined_in_module,
    get_instances_from_module,
)
from ..bedrock.atoms import CompositeType, EntityClass, Table
from ..bedrock.feature_types import (
    CompositeFeature,
    ForeignKey,
    PrimitiveFeature,
)
from ..bedrock.namespace_metadata import NamespaceMetadata
from ..bedrock.namespaced_id import NamespacedId
from .bases import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    Nullable,
    get_feature_dict,
)
from .scrutable import ScruTable


class DatascriptToBedrockConverter:
    def __init__(
        self,
        child_module_name: str,
    ) -> None:

        self.datascript_repo = DatascriptObjectCollector(child_module_name)
        self._top_module = get_top_module_name(child_module_name)

        self._ind_table_dict = {}
        self._fill_table_dic()
        self.tables = []
        self.composite_types = []
        self.entity_classes = []
        self._fill_atoms()

    def to_ns_metadata(self) -> NamespaceMetadata:
        return NamespaceMetadata(
            self.composite_types,
            self.entity_classes,
            self.tables,
            self._top_module,
        )

    def _fill_table_dic(self, ext_module=None):
        if ext_module is None:
            _repo = self.datascript_repo
        else:
            _repo = DatascriptObjectCollector(ext_module)
        for new_table in _repo.tables:
            if new_table.index:
                self._ind_table_dict[new_table.index] = new_table.name

    def _fill_atoms(self):
        for ds_obj in self.datascript_repo.atoms:
            self._parse(ds_obj)

    def _parse(self, atom):
        if isinstance(atom, ScruTable):
            return self._add_scrutable(atom)

        ns_id = self._get_ns_id(atom)
        if not ns_id.is_local:
            return
        desc = atom.__doc__
        self._add_ds_cls(atom, ns_id, desc)

    def _add_scrutable(self, scrutable: ScruTable):
        if not self._get_ns_id(scrutable.features).is_local:
            return
        ec = scrutable.subject
        if ec.__module__ == TMP_CLS_MODULE:
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
        self.tables.append(table)

    def _add_ds_cls(self, cls, ns_id: NamespacedId, desc):
        if BaseEntity in cls.mro():
            out_cls = EntityClass
            in_list = self.entity_classes
            cls_kwargs = self._ec_kwargs(cls)
        elif CompositeTypeBase in cls.mro():
            out_cls = CompositeType
            in_list = self.composite_types
            cls_kwargs = self._ct_kwargs(cls)

        in_list.append(
            out_cls(
                **{"name": ns_id.obj_id, "description": desc, **cls_kwargs}
            )
        )

    def _ec_kwargs(self, ec_cls: Type[BaseEntity]):
        simp_mro = get_simplified_mro(ec_cls)
        parents = [self._ser_id(p) for p in simp_mro if p is not BaseEntity]
        return dict(parents=parents)

    def _ct_kwargs(self, ct_cls: Type[CompositeTypeBase]):
        return dict(features=self._feature_cls_to_bedrock_list(ct_cls))

    def _feature_cls_to_bedrock_list(self, cls):
        if cls is None:
            return None
        return self._parse_feature_dict(get_feature_dict(cls))

    def _parse_feature_dict(self, feature_dict: Dict[str, Type]):
        out = []
        for k, cls in feature_dict.items():
            if isinstance(cls, Nullable):
                cls = cls.base
                nullable = True
            else:
                nullable = False
            full_id = self._get_ns_id(cls)
            if cls.__module__ in PRIMITIVE_MODULES:
                parsed_feat = PrimitiveFeature(
                    name=k, dtype=full_id.serialized_id, nullable=nullable
                )
            elif IndexBase in cls.mro():
                full_id.obj_id = self._table_from_index(cls)
                parsed_feat = ForeignKey(prefix=k, table=full_id.serialized_id)
            else:
                parsed_feat = CompositeFeature(
                    prefix=k, dtype=full_id.serialized_id
                )
            out.append(parsed_feat)
        return out

    def _table_from_index(self, index_cls, recurse=True):
        try:
            return self._ind_table_dict[index_cls]
        except KeyError:
            if recurse:
                self._fill_table_dic(index_cls.__module__)
                return self._table_from_index(index_cls, False)
            raise KeyError(
                f"Can't find table for index class {index_cls}"
                f" in dict {self._ind_table_dict}"
            )

    def _get_ns_id(self, cls) -> NamespacedId:
        # bit hacky .. twice
        if cls.__module__ == TMP_CLS_MODULE:
            return NamespacedId(None, cls.__name__)
        return NamespacedId.from_datascript_cls(cls, self._top_module)

    def _ser_id(self, cls) -> str:
        return self._get_ns_id(cls).serialized_id


class DatascriptObjectCollector:
    def __init__(self, module_name) -> None:
        self.namespace_module = import_module(module_name)

    @property
    def atoms(self):
        return [*self.tables, *self.composite_types, *self.entity_classes]

    @property
    def tables(self) -> Iterable[ScruTable]:
        return get_instances_from_module(
            self.namespace_module, ScruTable
        ).values()

    @property
    def composite_types(self) -> Iterable[Type[CompositeTypeBase]]:
        return self._obj_of_cls(CompositeTypeBase)

    @property
    def entity_classes(self) -> Iterable[Type[BaseEntity]]:
        return self._obj_of_cls(BaseEntity)

    def _obj_of_cls(self, py_cls):
        return get_cls_defined_in_module(
            self.namespace_module, py_cls
        ).values()
