from dataclasses import dataclass
from typing import List, Optional, Union

from parquetranger import TableRepo

from .config_loading import (
    DatasetConfig,
    ProjectConfig,
    load_branch_remote_pairs,
)
from .exceptions import (
    DatasetSetupException,
    NotAnArtifactException,
    ProjectSetupException,
)
from .helpers import get_all_top_modules
from .metadata import ArtifactMetadata
from .metadata.bedrock.namespace_metadata import NamespaceMetadata
from .naming import DATA_PATH


class ArtifactContext:
    def __init__(self) -> None:

        self.config = _load_artifact_config()
        self.is_dataset = isinstance(self.config, DatasetConfig)
        self.metadata = ArtifactMetadata.load_serialized()
        self.branch_remote_pairs = load_branch_remote_pairs()
        self.data_envs: List[DataEnvironmentToLoad] = []

        self._fill_data_envs()

    def import_namespaces(self, overwrite=True):
        for ns in self.metadata.imported_namespaces:
            self.metadata.extend_from_import(ns, overwrite)
        self.metadata.dump()

    def serialize(self):
        self.metadata.dump()
        self.config.dump()

    def create_trepo(
        self,
        name,
        namespace: str,
        partitioning_cols=None,
        max_partition_size=None,
    ):
        if self.is_dataset:
            parents_dict = {
                env.name: env.path for env in self.config.created_environments
            }
            main_path = parents_dict[self.config.default_env.name] / name
        else:
            parents_dict = {}
            main_path = DATA_PATH / namespace / name

        return TableRepo(
            main_path,
            group_cols=partitioning_cols,
            max_records=max_partition_size or 0,
            env_parents=parents_dict,
        )

    def replace_data(self, df, structable, env_name=None, parse: bool = True):
        if env_name is not None:
            assert self.is_dataset
            structable.trepo.set_env(env_name)
        structable.replace_all(df, parse)
        if self.is_dataset:
            structable.trepo.set_env(self.config.default_env.name)

    def has_data_env(self, ns_meta: NamespaceMetadata):
        if ns_meta.local_name in get_all_top_modules():
            return True
        for env in self.data_envs:
            if ns_meta.local_name == env.local_name:
                return True
        return False

    @property
    def ns_w_data(self):
        for ns in self.metadata.namespaces.values():
            if self.has_data_env(ns):
                yield ns

    @property
    def imported_namespace_meta_list(self):
        return [
            self.metadata.namespaces[ns.prefix]
            for ns in self.metadata.imported_namespaces
        ]

    def _fill_data_envs(self):
        if self.is_dataset:
            return
        for env_spec in self.config.data_envs:
            try:
                ns = self.metadata.imported_dic[env_spec.prefix]
            except KeyError:
                raise ProjectSetupException(
                    "No imported namespace corresponds to"
                    f" prefix {env_spec.prefix}"
                )
            self.data_envs.append(
                DataEnvironmentToLoad(
                    repo=ns.uri_root,
                    local_name=ns.prefix,
                    env=env_spec.env,
                    tag=env_spec.tag,
                    output_of_step=ns.uri_slug,
                )
            )


@dataclass
class DataEnvironmentToLoad:
    """
    specify output_of_step if and only if
    importing from a project
    """

    repo: str
    local_name: str
    env: str
    tag: Optional[str] = None
    output_of_step: Optional[str] = None

    @property
    def src_posix(self):
        return (DATA_PATH / (self.output_of_step or self.env)).as_posix()

    @property
    def out_posix(self):
        return self.out_path.as_posix()

    @property
    def out_path(self):
        return DATA_PATH / self.local_name

    def load_data(self, dvc_repo):
        dvc_repo.imp(
            url=self.repo,
            path=self.src_posix,
            out=self.out_posix,
            rev=self.tag or None,
            fname=None,
        )


def _load_artifact_config() -> Union[DatasetConfig, ProjectConfig]:
    try:
        return DatasetConfig()
    except DatasetSetupException as e:
        err1 = e

    try:
        return ProjectConfig()
    except ProjectSetupException as e:
        raise NotAnArtifactException(
            "Neither dataset, nor project found in working directory.\n"
            f"DatasetSetupError: {err1} \n"
            f"ProjectSetupError: {e}"
        )


def dump_dfs_to_tables(env_name, df_structable_pairs, parse=True):
    """helper function to fill an env of a dataset"""
    context = ArtifactContext()
    for df, structable in df_structable_pairs:
        context.replace_data(df, structable, env_name, parse)
