from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import List, Optional, Union

import yaml

from .exceptions import (
    DatasetSetupException,
    NotAnArtifactException,
    ProjectSetupException,
)
from .metadata.io import load_imported_namespaces
from .metadata.schema import ImportedNamespace
from .naming import (
    COMPLETE_ENV_NAME,
    DATA_PATH,
    DEFAULT_BRANCH_NAME,
    DEFAULT_REMOTES_PATH,
    DatasetConfigPaths,
    ProjectConfigPaths,
)
from .utils import load_named_dict_to_list


@dataclass
class EnvToCreate:
    name: str
    branch: str
    kwargs: dict = field(default_factory=dict)

    @property
    def path(self) -> Path:
        return DATA_PATH / self.name

    @property
    def posix(self) -> str:
        return self.path.as_posix()


COMPLETE_ENV = EnvToCreate(COMPLETE_ENV_NAME, DEFAULT_BRANCH_NAME)


@dataclass
class DataEnvSpecification:
    prefix: str
    env: str
    tag: str = None  # might override the one from imported namespace


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
    def out_path(self):
        return DATA_PATH / self.local_name


class ProjectConfig:
    """
    environment statement
    with imported namespaces

    """

    def __init__(self) -> None:

        _loader = partial(_yaml_or_err, exc_cls=ProjectSetupException)

        current_env_spec: List[DataEnvSpecification] = _loader(
            load_data_env_spec_list
        )
        imported_namespaces: List[ImportedNamespace] = _loader(
            load_imported_namespaces
        )
        ns_dic = {ns.prefix: ns for ns in imported_namespaces}

        self.data_envs: List[DataEnvironmentToLoad] = []

        for env_spec in current_env_spec:
            try:
                ns = ns_dic[env_spec.prefix]
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

    def has_data_env(self, prefix: str):
        for env in self.data_envs:
            if prefix == env.local_name:
                return True
        return False


class DatasetConfig:
    def __init__(self) -> None:

        _raw_envs = _yaml_or_err(
            DatasetConfigPaths.CREATED_ENVS,
            DatasetSetupException,
            "environments to create",
        )

        self.created_environments: List[EnvToCreate] = [
            EnvToCreate(k, **v) for k, v in _raw_envs.items()
        ]
        # TODO: define some other default env
        self.default_env: EnvToCreate = COMPLETE_ENV


def load_artifact_config() -> Union[DatasetConfig, ProjectConfig]:

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


def load_branch_remote_pairs():
    return _yaml_or_err(
        DEFAULT_REMOTES_PATH,
        "branch: remote pairs",
        NotAnArtifactException,
    ).items()


load_data_env_spec_list = partial(
    load_named_dict_to_list,
    path=ProjectConfigPaths.CURRENT_ENV,
    key_name="prefix",
    cls=DataEnvSpecification,
)


def _yaml_or_err(path_or_fun, exc_cls, desc=None):
    try:
        if callable(path_or_fun):
            return path_or_fun()
        return yaml.safe_load(path_or_fun.read_text())
    except FileNotFoundError as e:
        raise exc_cls(f"Config of {desc or path_or_fun} not found in {e}")
