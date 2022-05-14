from dataclasses import dataclass
from functools import partial
from typing import Dict, List

from dvc.repo import Repo
from structlog import get_logger

from .config_loading import Config
from .exceptions import ProjectSetupException
from .metadata.atoms import EntityClass
from .metadata.project_metadata import ProjectMetadata
from .metadata.scrutable import ScruTable
from .module_tree import ModuleTree
from .naming import PREFIX_SEP, get_data_path
from .pipeline_registry import get_global_pipereg
from .registry import Registry
from .utils import gen_rmtree, reset_meta_module, reset_src_module

logger = get_logger()


class ProjectRuntime:
    def __init__(self) -> None:
        self.config: Config = Config.load()
        self.name = self.config.name
        self.registry = Registry(self.config)
        self.pipereg = get_global_pipereg(reset=True)
        reset_src_module()
        reset_meta_module()
        module_tree = ModuleTree(self.config, self.registry)

        self.metadata: ProjectMetadata = module_tree.project_meta
        self.metadata_dic: Dict[str, ProjectMetadata] = module_tree._project_meta_dic
        self.data_to_load: List[DataEnvironmentToLoad] = self._get_data_envs()

    def load_all_data(self):
        dvc_repo = Repo()
        posixes = []
        for data_env in self.data_to_load:
            gen_rmtree(data_env.path)  # brave thing...
            data_env.path.parent.mkdir(exist_ok=True, parents=True)
            data_env.load_data(dvc_repo)
            posixes.append(data_env.posix)
        return posixes

    def get_table_for_entity(
        self, ec: EntityClass, base_table: ScruTable, feat_elems
    ) -> ScruTable:
        fixed = base_table.key_map.get(PREFIX_SEP.join(feat_elems))
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

    def _get_data_envs(self):
        arg_set = set()
        for env in self.config.envs:
            for project_name, data_env in env.import_envs.items():
                a_imp = self.config.get_import(project_name)
                meta = self.metadata_dic[project_name]
                tag = meta.latest_tag_of(data_env)
                nss = a_imp.data_namespaces or meta.data_namespaces
                for ns in nss:
                    arg_set.add((project_name, meta.uri, ns, data_env, tag))
        return [DataEnvironmentToLoad(*args) for args in arg_set]


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

    def load_data(self, dvc_repo):
        dvc_repo.imp(
            url=self.uri,
            path=self.posix,
            out=self.posix,
            rev=self.tag,
            fname=None,
        )


def dump_dfs_to_tables(df_structable_pairs, parse=True, **kwargs):
    """helper function to fill the detected env of a dataset"""
    for df, structable in df_structable_pairs:
        structable.replace_all(df, parse, **kwargs)
