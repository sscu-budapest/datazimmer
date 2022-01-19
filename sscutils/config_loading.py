from dataclasses import asdict, dataclass, field
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
    RUN_CONF_PATH,
    DatasetConfigPaths,
    ProjectConfigPaths,
)
from .utils import get_dict_factory, named_dict_to_list


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

    def dump(self):
        _dump_named_dicts(
            ProjectConfigPaths.CURRENT_ENV, "prefix", self.data_envs
        )


@dataclass
class DatasetConfig:
    default_env: EnvToCreate = COMPLETE_ENV
    created_environments: List[EnvToCreate] = field(
        default_factory=load_created_env_spec_list
    )

    def dump(self):
        _dump_named_dicts(
            DatasetConfigPaths.CREATED_ENVS, "name", self.created_environments
        )


@dataclass
class RunConfig:
    profile: bool = False

    def __enter__(self):
        RUN_CONF_PATH.write_text(yaml.safe_dump(asdict(self)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        RUN_CONF_PATH.unlink()

    @classmethod
    def load(cls):
        try:
            return cls(**_yaml_or_err(RUN_CONF_PATH, FileNotFoundError))
        except FileNotFoundError:
            return cls()


def load_branch_remote_pairs():
    return (
        _yaml_or_err(
            DEFAULT_REMOTES_PATH,
            "branch: remote pairs",
            NotAnArtifactException,
        )
        or {}
    ).items()


def _yaml_or_err(path, exc_cls, desc=None):
    try:
        return yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError as e:
        raise exc_cls(f"Config of {desc or path} not found in {e}")


def _dump_named_dicts(path, att_name, obj_list):
    named_dict = {
        getattr(obj, att_name): asdict(
            obj, dict_factory=get_dict_factory(att_name)
        )
        for obj in obj_list
    }
    path.write_text(yaml.safe_dump(named_dict))
