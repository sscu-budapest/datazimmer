import inspect
import sys
from dataclasses import dataclass
from functools import partial
from importlib import import_module
from pathlib import Path
from pkgutil import walk_packages
from typing import TypeVar

from dvc.repo import Repo
from structlog import get_logger

from .config_loading import Config
from .exceptions import ProjectSetupException
from .metadata.atoms import EntityClass
from .metadata.complete_id import CompleteIdBase
from .metadata.high_level import NamespaceMetadata, ProjectMetadata
from .metadata.scrutable import ScruTable
from .naming import (
    MAIN_MODULE_NAME,
    META_MODULE_NAME,
    VERSION_VAR_NAME,
    get_data_path,
    to_mod_name,
)
from .registry import Registry
from .utils import gen_rmtree

T = TypeVar("T")


logger = get_logger()


class ProjectRuntime:
    def __init__(self) -> None:
        self.config: Config = Config.load()
        self.name = self.config.name
        self.registry = Registry(self.config)
        self._module_dic = {}
        self._collected_modules = set()
        self._ns_meta_dic: dict[CompleteIdBase, NamespaceMetadata] = {}
        self.metadata_dic: dict[str, ProjectMetadata] = {
            self.name: ProjectMetadata(**self.registry.get_info())
        }

        sys.path.insert(0, Path.cwd().as_posix())
        self._walk_module(import_module(MAIN_MODULE_NAME))
        # self._walk_id(META_MODULE_NAME, True) simpler but bloating
        while self._module_dic.keys() != self._collected_modules:
            self._collect_metas()
        self._fill_projects()
        self.metadata = self.metadata_dic[self.name]
        self.data_to_load: list[DataEnvironmentToLoad] = self._get_data_envs()

    def load_all_data(self, env=None):
        dvc_repo = Repo()
        posixes = []
        for data_env in self.data_to_load:
            gen_rmtree(data_env.path)  # brave thing...
            data_env.path.parent.mkdir(exist_ok=True, parents=True)
            pull = (env is None) or (
                data_env.env == self.config.resolve_ns_env(data_env.project, env)
            )
            data_env.load_data(dvc_repo, pull)
            posixes.append(data_env.posix)
        return posixes

    def get_table_for_entity(
        self, ec: EntityClass, base_table: ScruTable, feat_elems
    ) -> ScruTable:
        fixed = base_table.key_map.get(feat_elems)
        if fixed is not None:
            return fixed
        my_proj = self.metadata_dic[base_table.id_.project]
        ns_table = my_proj.namespaces[base_table.id_.namespace].get_table_of_ec(ec)
        if ns_table:
            return ns_table
        _log = partial(logger.warning, table=base_table.id_, feat=feat_elems)
        _log("couldn't find FK source in namespace, looking in project")
        proj_table = my_proj.table_of_ec(ec)
        if proj_table:
            return proj_table
        _log("couldn't find FK source in project, looking everywhere")
        for proj in self.metadata_dic.values():
            ext_tab = proj.table_of_ec(ec)
            if ext_tab:
                return ext_tab
        msg = f"couldn't find table for {feat_elems} in {base_table.id_}"
        raise ProjectSetupException(msg)

    def run_step(self, namespace, env):
        for step in self.metadata.namespaces[namespace].pipeline_elements:
            if env in step.write_envs:
                step.run(env)
                return
        raise KeyError("no such step")

    def step_names_of_env(self, env):
        steps = self.metadata.complete.pipeline_elements
        return [step.stage_name(env) for step in steps if env in step.write_envs]

    def _get_data_envs(self):
        arg_set = set()
        for env in self.config.envs:
            for project_name, data_env in env.import_envs.items():
                a_imp = self.config.get_import(project_name)
                # TODO: KeyError here means module tree parser
                # did not find something that was needed by config
                meta = self.metadata_dic[project_name]
                tag = meta.latest_tag_of(data_env)
                nss = a_imp.data_namespaces or meta.data_namespaces
                for ns in nss:
                    arg_set.add((project_name, meta.uri, ns, data_env, tag))
        return [DataEnvironmentToLoad(*args) for args in arg_set]

    def _walk_module(self, mod):
        self._module_dic[mod.__name__] = mod
        src_dir = Path(mod.__file__).parent.as_posix()
        for _info in walk_packages([src_dir], f"{mod.__package__}."):
            _m = import_module(_info.name)
            self._module_dic[_info.name] = _m

    def _collect_metas(self):
        # TODO: do this (optionally) for data importing as well
        for ns_module_id, module in list(self._module_dic.items()):
            if ns_module_id in self._collected_modules:
                continue
            self._parse_module(module)
            self._collected_modules.add(ns_module_id)

    def _parse_module(self, module):
        base_id = CompleteIdBase.from_module_name(module.__name__, self.name)
        if base_id is None:
            return
        if base_id not in self._ns_meta_dic.keys():
            self._ns_meta_dic[base_id] = NamespaceMetadata(base_id.namespace)
        ns_meta = self._ns_meta_dic[base_id]

        for obj in map(partial(getattr, module), dir(module)):
            if inspect.ismodule(obj) and _dz_module(obj.__name__):
                self._walk_module(obj)
                continue
            mod_name = getattr(obj, "__module__", "")  # set for relevant instances
            if _dz_module(mod_name):
                if mod_name != module.__name__:
                    self._module_dic[mod_name] = import_module(mod_name)
                else:
                    ns_meta.add_obj(obj)

    def _fill_projects(self):
        for base_id, ns_meta in self._ns_meta_dic.items():
            proj_id = base_id.project
            if proj_id not in self.metadata_dic.keys():
                proj_v = _get_v_of_ext_project(proj_id)
                init_kwargs = self.registry.get_project_meta_base(proj_id, proj_v)
                if not init_kwargs:
                    continue
                self.metadata_dic[proj_id] = ProjectMetadata(**init_kwargs)
            self.metadata_dic[proj_id].namespaces[base_id.namespace] = ns_meta


@dataclass
class DataEnvironmentToLoad:
    """
    specify output_of_step if and only if
    importing from a project
    """

    project: str
    uri: str
    ns: str
    env: str
    tag: str

    @property
    def posix(self):
        return self.path.as_posix()

    @property
    def path(self):
        return get_data_path(self.project, self.ns, self.env)

    def load_data(self, dvc_repo: Repo, pull=False):
        dvc_repo.imp(
            url=self.uri,
            path=self.posix,
            out=self.posix,
            rev=self.tag,
            fname=None,
            no_exec=not pull,
        )


def dump_dfs_to_tables(df_structable_pairs, parse=True, **kwargs):
    """helper function to fill the detected env of a dataset"""
    for df, structable in df_structable_pairs:
        structable.replace_all(df, parse, **kwargs)


def _get_v_of_ext_project(project_name):
    module_name = f"{META_MODULE_NAME}.{to_mod_name(project_name)}"
    return getattr(import_module(module_name), VERSION_VAR_NAME)


def _dz_module(module_name: str):
    prefixes = [META_MODULE_NAME, MAIN_MODULE_NAME]
    return any(map(lambda s: module_name.startswith(f"{s}"), prefixes))
