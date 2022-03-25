import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, List, Optional

import yaml
from dvc.repo import Repo
from parquetranger import TableRepo
from structlog import get_logger

from .exceptions import ArtifactSetupException
from .metadata.bedrock.complete_id import CompleteId, CompleteIdBase
from .naming import (
    BASE_CONF_PATH,
    DEFAULT_ENV_NAME,
    DEFAULT_REGISTRY,
    RUN_CONF_PATH,
    VERSION_PREFIX,
    VERSION_SEPARATOR,
    get_data_path,
)
from .utils import named_dict_to_list

if TYPE_CHECKING:
    from .metadata.bedrock.atoms import Table  # pragma: no cover

logger = get_logger(ctx="config loading")


@dataclass
class ArtifactEnv:
    name: str = DEFAULT_ENV_NAME
    remote: Optional[str] = None
    parent: Optional[str] = None
    params: dict = field(default_factory=dict)
    import_envs: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.remote is None:
            try:
                repo = Repo()
                self.remote = repo.config["core"]["remote"]
            except KeyError:
                msg = "can't get default remote from  dvc repo"
                logger.warning(msg)


@dataclass
class ImportedArtifact:
    name: str
    data_namespaces: list = None
    version: str = ""


@dataclass
class Config:
    name: str
    version: str
    default_env: str = None
    registry: str = DEFAULT_REGISTRY
    validation_envs: list = None
    envs: List[ArtifactEnv] = None
    imported_artifacts: List[ImportedArtifact] = field(default_factory=list)
    cron_bumps: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.envs:
            self.envs = [ArtifactEnv()]
        if self.default_env is None:
            self.default_env = self.envs[0].name
        if self.validation_envs is None:
            self.validation_envs = [self.default_env]
        _parse_version(self.version)
        msg = f"name can only contain lower case letters {self.name}"
        assert re.compile("^[a-z]+$").findall(self.name), msg
        self.version = self.version[1:]

    def get_env(self, env_name: str) -> ArtifactEnv:
        return _get(self.envs, env_name)

    def get_import(self, artifact_name: str) -> ImportedArtifact:
        return _get(self.imported_artifacts, artifact_name)

    def create_trepo(
        self,
        id_: CompleteId,
        partitioning_cols=None,
        max_partition_size=None,
    ):

        envs_of_ns = self.get_data_envs(id_.artifact, id_.namespace)
        if not envs_of_ns:
            return UnavailableTrepo()
        default_env = self.resolve_ns_env(id_.artifact, self.default_env)
        parents_dict = {
            env: get_data_path(id_.artifact, id_.namespace, env) for env in envs_of_ns
        }
        main_path = parents_dict[default_env] / id_.obj_id
        return TableRepo(
            main_path,
            group_cols=partitioning_cols,
            max_records=max_partition_size or 0,
            env_parents=parents_dict,
        )

    def get_data_envs(self, artifact, ns):
        if artifact == self.name:
            return [e.name for e in self.envs]
        for iaf in self.imported_artifacts:
            if iaf.name == artifact:
                dnss = iaf.data_namespaces
                if dnss and (ns not in dnss):
                    return []
        data_envs = [e.import_envs.get(artifact) for e in self.envs]
        return set(filter(None, data_envs))

    def get_data_env(self, env_name, data_artifact):
        env = self.get_env(env_name)
        data_env = env.import_envs.get(data_artifact)
        return data_env or self.get_data_env(env.parent, data_artifact)

    def resolve_ns_env(self, artifact, env):
        if artifact == self.name:
            return env
        return self.get_data_env(env, artifact)

    def table_to_trepo(
        self, table: "Table", id_base: CompleteIdBase, env=None
    ) -> TableRepo:
        trepo = self.create_trepo(
            id_base.to_id(table.name),
            table.partitioning_cols,
            table.partition_max_rows,
        )
        if env is not None:
            trepo.set_env(self.resolve_ns_env(id_base.artifact, env))
        return trepo

    def init_cron_bump(self, pipe_elem_name):
        if self.cron_bumps.get(pipe_elem_name) is None:
            self.cron_bumps[pipe_elem_name] = -1
            self.bump_cron(pipe_elem_name)

    def bump_cron(self, pipe_elem_name):
        self.cron_bumps[pipe_elem_name] += 1
        c_dic = yaml.safe_load(BASE_CONF_PATH.read_text())
        new_conf = {**c_dic, "cron_bumps": self.cron_bumps}
        BASE_CONF_PATH.write_text(yaml.dump(new_conf, sort_keys=False))

    def dump(self):
        d = asdict(self)
        for k in ["envs", "imported_artifacts"]:
            d[k] = {e.pop("name"): e for e in d[k]}
        d["version"] = f"v{d['version']}"
        BASE_CONF_PATH.write_text(yaml.safe_dump(d))

    @classmethod
    def load(cls):
        dic = _yaml_or_err(BASE_CONF_PATH)
        env_list = named_dict_to_list(dic.pop("envs", {}), ArtifactEnv)
        art_val = dic.pop("imported_artifacts", {})
        if isinstance(art_val, list):
            art_val = {k: {} for k in art_val}
        art_list = named_dict_to_list(art_val, ImportedArtifact)
        return cls(**dic, envs=env_list, imported_artifacts=art_list)

    @property
    def sorted_envs(self):
        _remote = self.get_env(self.default_env).remote
        return sorted(self.envs, key=lambda env: (env.remote == _remote, env.remote))


@dataclass
class RunConfig:
    profile: bool = False
    write_env: Optional[str] = None
    read_env: Optional[str] = None

    def __enter__(self):
        self.dump()

    def __exit__(self, exc_type, exc_val, exc_tb):
        RUN_CONF_PATH.unlink()

    def dump(self):
        RUN_CONF_PATH.write_text(yaml.safe_dump(asdict(self)))

    @classmethod
    def load(cls):
        return cls(**_yaml_or_err(RUN_CONF_PATH, "run config"))


class UnavailableTrepo(TableRepo):
    def __init__(self):
        pass


def get_tag(meta_version, data_version, env):
    return VERSION_SEPARATOR.join([VERSION_PREFIX, meta_version, data_version, env])


def _parse_version(v: str):
    assert v[0] == "v", f"version must start with v, but is {v}"
    return tuple(map(int, v[1:].split(".")))


def _yaml_or_err(path, desc=None):
    try:
        return yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError as e:
        msg = f"Config of {desc or path} not found in {e}"
        raise ArtifactSetupException(msg)


def _get(obj_l, key):
    for obj in obj_l:
        if obj.name == key:
            return obj
    raise KeyError(key)
