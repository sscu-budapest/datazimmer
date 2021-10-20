from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import List, Optional, Union

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
    IMPORTED_NAMESPACES_PATH,
    DatasetConfigPaths,
    ProjectConfigPaths,
)


@dataclass
class EnvToCreate:
    name: str
    branch: str
    kwargs: dict = field(default_factory=dict)

    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v and (k != "name")}

    def get_path(self) -> Path:
        return DATA_PATH / self.name

    def get_posix(self) -> str:
        return self.get_path().as_posix()


COMPLETE_ENV = EnvToCreate(COMPLETE_ENV_NAME, DEFAULT_BRANCH_NAME)


@dataclass
class NamespaceEnvironmentToImport:
    """

    specify output_of_step if and only if
    importing from a project
    """

    repo: str
    local_name: str
    env: str
    tag: Optional[str] = None
    output_of_step: Optional[str] = None

    def get_src_posix(self):
        return DATA_PATH / self.output_of_step or self.env

    def get_out_path(self):
        return DATA_PATH / self.local_name


class ProjectConfig:
    """
    environment statement
    with imported namespaces

    """

    def __init__(self) -> None:

        _read_yaml = partial(_yaml_or_err, exc_cls=ProjectSetupException)
        _raw_env = _read_yaml(
            ProjectConfigPaths.CURRENT_ENV, "current environment"
        )
        _raw_ns_dict = _read_yaml(
            IMPORTED_NAMESPACES_PATH, "imported namespaces"
        )

        self.imported_namespace_envs: List[NamespaceEnvironmentToImport] = []

        for ns_name, ns_kwargs in _raw_ns_dict:
            try:
                ns_env_kwargs = _parse_env_spec(_raw_env[ns_name])
            except KeyError:
                raise ProjectSetupException(
                    f"No source environment of {ns_name}"
                    f" specified in {ProjectConfigPaths.CURRENT_ENV}"
                )
            self.imported_namespace_envs.append(
                NamespaceEnvironmentToImport(
                    local_name=ns_name, **ns_env_kwargs, **ns_kwargs
                )
            )


class DatasetConfig:
    def __init__(self) -> None:

        _raw_envs = _yaml_or_err(
            DatasetConfigPaths.CREATED_ENVS,
            "environments to create",
            DatasetSetupException,
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


def _yaml_or_err(path, desc, exc_cls):
    try:
        return yaml.safe_load(path.read_text())
    except FileNotFoundError:
        raise exc_cls(f"Config of {desc} not found in {path}")


def _parse_env_spec(spec_str):
    if "@" in spec_str:
        env, tag = spec_str.split("@")
    else:
        env, tag = spec_str, None
    return {"tag": tag, "env": env}
