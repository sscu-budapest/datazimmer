import sys
from dataclasses import dataclass
from functools import cached_property, partial
from importlib import import_module
from pathlib import Path
from pkgutil import walk_packages
from typing import Any, Dict, Iterable, Type

from .config_loading import Config
from .metadata.atoms import CompositeType, EntityClass
from .metadata.complete_id import CompleteIdBase
from .metadata.datascript import AbstractEntity, CompositeTypeBase, SourceUrl
from .metadata.namespace_metadata import NamespaceMetadata
from .metadata.project_metadata import ProjectMetadata
from .metadata.scrutable import ScruTable
from .naming import MAIN_MODULE_NAME, META_MODULE_NAME, VERSION_VAR_NAME
from .registry import Registry
from .utils import (
    get_cls_defined_in_module,
    get_instances_from_module,
    get_modules_from_module,
)


class ModuleTree:
    def __init__(self, config: Config, registry: Registry) -> None:
        self._project_name = config.name
        self._registry = registry
        self._conf = config
        self._module_dic = {}
        self._collected_modules = set()
        self._ns_meta_dic: Dict[CompleteIdBase, NamespaceMetadata] = {}
        self._project_meta_dic: Dict[str, ProjectMetadata] = {}

        sys.path.insert(0, Path.cwd().as_posix())
        self._walk_id(MAIN_MODULE_NAME)
        self._walk_id(META_MODULE_NAME, True)
        while self._module_dic.keys() != self._collected_modules:
            self._collect_metas()
        sys.path.pop(0)
        self._fill_projects()

        self.project_meta = self._project_meta_dic[self._project_name]

    def _walk_id(self, module_id, allow_fail=False):
        mod = import_module(module_id)
        src_dir = Path(mod.__file__).parent.as_posix()
        for _info in walk_packages([src_dir], f"{module_id}."):
            _m = import_module(_info.name)
            self._module_dic[_info.name] = _m

    def _collect_metas(self):
        # TODO: do this (optionally) for data importing as well
        for ns_module_id, module in self._module_dic.items():
            if ns_module_id in self._collected_modules:
                continue
            self._parse_module(module)
            self._collected_modules.add(ns_module_id)

    def _parse_module(self, module):
        base_id = CompleteIdBase.from_module_name(module.__name__, self._project_name)
        if base_id not in self._ns_meta_dic.keys():
            self._ns_meta_dic[base_id] = NamespaceMetadata(base_id.namespace)
        ns_meta = self._ns_meta_dic[base_id]
        oc = _ObjectCollector(module)

        _newcheck = partial(self._is_new_module, current_module=module.__name__)
        for s_url in oc.source_urls:
            ns_meta.source_urls.append(s_url)

        for scrutable in oc.tables:
            if _newcheck(scrutable.module_name):
                continue
            ns_meta.tables.append(scrutable)

        for comp_type in oc.composite_types:
            if _newcheck(comp_type.__module__):
                continue
            ns_meta.composite_types.append(CompositeType.from_cls(comp_type))

        for entity in oc.entity_classes:
            if _newcheck(entity.__module__):
                continue
            ns_meta.entity_classes.append(EntityClass.from_cls(entity))

        for ext_mod_name in oc.dz_module_names:
            _newcheck(ext_mod_name)

    def _is_new_module(self, new_base_module: str, current_module: str):
        if new_base_module != current_module:
            self._module_dic[new_base_module] = import_module(new_base_module)
            return True

    def _fill_projects(self):
        for base_id, ns_meta in self._ns_meta_dic.items():
            proj_id = base_id.project
            if proj_id == self._project_name:
                proj_v = self._conf.version
            else:
                proj_v = _get_v_of_ext_project(proj_id)
            if proj_id not in self._project_meta_dic.keys():
                init_kwargs = self._registry.get_project_meta_base(proj_id, proj_v)
                if not init_kwargs:
                    continue
                self._project_meta_dic[proj_id] = ProjectMetadata(**init_kwargs)
            self._project_meta_dic[proj_id].namespaces[base_id.namespace] = ns_meta


@dataclass
class _ObjectCollector:
    module: Any

    @cached_property
    def tables(self) -> Iterable[ScruTable]:
        return self._inst(ScruTable)

    @cached_property
    def source_urls(self) -> Iterable[SourceUrl]:
        return self._inst(SourceUrl)

    @cached_property
    def composite_types(self) -> Iterable[Type[CompositeTypeBase]]:
        return self._obj_of_cls(CompositeTypeBase)

    @cached_property
    def entity_classes(self) -> Iterable[Type[AbstractEntity]]:
        return self._obj_of_cls(AbstractEntity)

    @property
    def dz_module_names(self):
        for mod in get_modules_from_module(self.module):
            mname = mod.__name__
            for pref in [META_MODULE_NAME, MAIN_MODULE_NAME]:
                if mname.startswith(f"{pref}."):
                    yield mname

    def _obj_of_cls(self, py_cls):
        return get_cls_defined_in_module(self.module, py_cls).values()

    def _inst(self, py_cls):
        return get_instances_from_module(self.module, py_cls).values()


def _get_v_of_ext_project(project_name):
    module_name = f"{META_MODULE_NAME}.{project_name}"
    return getattr(import_module(module_name), VERSION_VAR_NAME)
