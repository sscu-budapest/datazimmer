import sys
from dataclasses import dataclass
from shutil import rmtree
from typing import Dict, List

from dvc.repo import Repo

from .config_loading import Config
from .get_runtime import get_runtime
from .metadata import ArtifactMetadata
from .metadata.bedrock.complete_id import CompleteId
from .module_tree import ModuleTree
from .naming import get_data_path
from .pipeline_registry import get_global_pipereg
from .registry import Registry


class ArtifactContext:
    def __init__(self) -> None:
        self.config: Config = Config.load()
        self.name = self.config.name
        self.metadata: ArtifactMetadata = ArtifactMetadata.load_installed(self.name)
        self.ext_metas: Dict[str, ArtifactMetadata] = {}
        self._fill_ext_meta()
        self.data_to_load: List[DataEnvironmentToLoad] = self._get_data_envs()
        self.pipereg = get_global_pipereg(reset=True)
        self.module_tree = ModuleTree()
        self.registry = Registry(self.config)

    def get_atom(self, id_: CompleteId):
        if (id_.artifact is None) or (id_.artifact == self.name):
            meta = self.metadata
        else:
            meta = self.ext_metas[id_.artifact]
        return meta.get_atom(id_)

    def load_all_data(self):
        dvc_repo = Repo()
        posixes = []
        for data_env in self.data_to_load:
            rmtree(data_env.path, ignore_errors=True)  # brave thing...
            data_env.path.parent.mkdir(exist_ok=True, parents=True)
            data_env.load_data(dvc_repo)
            posixes.append(data_env.posix)
        return posixes

    def _fill_ext_meta(self):
        for a_imp in self.config.imported_artifacts:
            self._add_a_meta(a_imp.name)

    def _get_data_envs(self):
        arg_set = set()
        for env in self.config.envs:
            for artifact_name, data_env in env.import_envs.items():
                a_imp = self.config.get_import(artifact_name)
                meta = self.ext_metas[artifact_name]
                tag = meta.latest_tag_of(data_env)
                nss = a_imp.data_namespaces or meta.data_namespaces
                for ns in nss:
                    arg_set.add((artifact_name, meta.uri, ns, data_env, tag))
        return [DataEnvironmentToLoad(*args) for args in arg_set]

    def _add_a_meta(self, a_meta_name):
        if a_meta_name in self.ext_metas.keys():
            return
        a_meta = ArtifactMetadata.load_installed(a_meta_name, self.name)
        self.ext_metas[a_meta_name] = a_meta
        for sub_meta in a_meta.get_used_artifacts():
            self._add_a_meta(sub_meta)
        # TODO: do this for data importing as well


@dataclass
class DataEnvironmentToLoad:
    """
    specify output_of_step if and only if
    importing from a project
    """

    artifact: str
    uri: str
    ns: str
    env: str
    tag: str

    @property
    def posix(self):
        return self.path.as_posix()

    @property
    def path(self):
        return get_data_path(self.artifact, self.ns, self.env)

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


def run_step():
    get_runtime().pipereg.get_step(sys.argv[1]).run()
