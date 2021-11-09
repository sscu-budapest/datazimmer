from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml

from .exceptions import (
    DatasetSetupException,
    NotAnArtifactException,
    ProjectSetupException,
)
from .naming import (
    COMPLETE_ENV_NAME,
    DATA_PATH,
    DEFAULT_BRANCH_NAME,
    DEFAULT_REMOTES_PATH,
    DatasetConfigPaths,
    ProjectConfigPaths,
)
from .utils import named_dict_to_list


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


def load_data_env_spec_list():
    return named_dict_to_list(
        _yaml_or_err(ProjectConfigPaths.CURRENT_ENV, ProjectSetupException),
        DataEnvSpecification,
        "prefix",
    )


def load_created_env_spec_list():
    return named_dict_to_list(
        _yaml_or_err(DatasetConfigPaths.CREATED_ENVS, DatasetSetupException),
        EnvToCreate,
    )


@dataclass
class ProjectConfig:
    data_envs: List[DataEnvSpecification] = field(
        default_factory=load_data_env_spec_list
    )


@dataclass
class DatasetConfig:
    default_env: EnvToCreate = COMPLETE_ENV
    created_environments: List[EnvToCreate] = field(
        default_factory=load_created_env_spec_list
    )


def load_branch_remote_pairs():
    return _yaml_or_err(
        DEFAULT_REMOTES_PATH,
        "branch: remote pairs",
        NotAnArtifactException,
    ).items()


def _yaml_or_err(path, exc_cls, desc=None):
    try:
        return yaml.safe_load(path.read_text())
    except FileNotFoundError as e:
        raise exc_cls(f"Config of {desc or path} not found in {e}")
