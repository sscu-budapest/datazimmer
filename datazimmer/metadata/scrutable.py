from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar

import pandas as pd
from colassigner.meta_base import ColMeta
from structlog import get_logger

from ..config_loading import Config, RunConfig, UnavailableTrepo
from ..exceptions import ProjectRuntimeException
from ..utils import camel_to_snake, get_creation_module_name
from .atoms import EntityClass, parse_df
from .complete_id import CompleteId, CompleteIdBase
from .datascript import AbstractEntity

logger = get_logger()

T = TypeVar("T")


class ScruTable:
    def __init__(
        self,
        entity: type[AbstractEntity],
        entity_key_table_map: Optional[dict[str, "ScruTable"]] = None,
        partitioning_cols: Optional[list[str]] = None,
        max_partition_size: Optional[int] = None,
    ) -> None:
        # TODO: somehow add possibility for a description

        self._conf = Config.load()
        self.__module__ = get_creation_module_name()
        self.id_: CompleteId = self._infer_id(entity, self.__module__)
        self.key_map = _parse_entity_map(entity_key_table_map)
        self.abstract_entity = entity
        self.entity_class = EntityClass.from_cls(entity)

        self.name = self.id_.obj_id
        self.index = self.entity_class.identifiers
        self.features = self.entity_class.properties
        self.index_map = self.entity_class.table_index_dt_map
        self.features_map = self.entity_class.table_feature_dt_map
        self.dtype_map = self.entity_class.table_full_dt_map
        self.index_cols = self.entity_class.table_index_cols
        self.feature_cols = self.entity_class.table_feature_cols
        self.all_cols = self.entity_class.table_all_columns

        self.partitioning_cols = partitioning_cols
        self.max_partition_size = max_partition_size
        self.trepo = self._conf.create_trepo(
            self.id_, self.partitioning_cols, self.max_partition_size
        )
        self.get_full_df = self._read_wrap(self.trepo.get_full_df)
        self.map_partitions = self._read_wrap(self.trepo.map_partitions)

        self.extend = self._write_wrap(self.trepo.extend)
        self.replace_all = self._write_wrap(self.trepo.replace_all)
        self.replace_records = self._write_wrap(self.trepo.replace_records)
        self.replace_groups = self._write_wrap(self.trepo.replace_groups)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name}, {self.__module__})"

    def purge(self):
        with self.env_ctx(RunConfig.load().write_env):
            self.trepo.purge()

    def get_partition_paths(self, partition_col, env=None):
        with self.env_ctx(env or RunConfig.load().read_env):
            for gid, paths in self.trepo.get_partition_paths(partition_col):
                yield gid, list(paths)

    @property
    def paths(self):
        with self.env_ctx(RunConfig.load().read_env):
            for _path in self.trepo.paths:
                yield _path

    @property
    def dfs(self):
        with self.env_ctx(RunConfig.load().read_env):
            for _df in self.trepo.dfs:
                yield _df

    @contextmanager
    def env_ctx(self, env):
        if isinstance(self.trepo, UnavailableTrepo):
            raise ProjectRuntimeException(f"trepo unavailable for {self.id_}")
        true_env = self._conf.resolve_ns_env(self.id_.project, env)
        with self.trepo.env_ctx(true_env):
            yield

    def _read_wrap(self, fun: Callable[..., T]) -> Callable[..., T]:
        return _RWrap(fun, self.env_ctx)

    def _write_wrap(self, fun):
        return _WWrap(fun, self.env_ctx, self._parse_df)

    def _parse_df(self, df: pd.DataFrame, verbose=True):
        if verbose:
            logger.info("parsing", table=self.name, namespace=self.id_.namespace)

        return parse_df(df, self.abstract_entity, verbose)

    def _infer_id(self, entity_cls, initing_module_name):
        id_base = CompleteIdBase.from_module_name(initing_module_name, self._conf.name)
        return id_base.to_id(camel_to_snake(entity_cls.__name__))


def _parse_entity_map(entity_map: dict):
    d = {}
    for k, v in (entity_map or {}).items():
        if isinstance(k, ColMeta):
            d[k._parent_prefixes] = v
    return d


@dataclass
class _RWrap:
    fun: Callable
    env_ctx: Callable

    def __call__(self, env=None, **kwargs):
        with self.env_ctx(env or RunConfig.load().read_env):
            return self.fun(**kwargs)


@dataclass
class _WWrap:
    fun: Callable
    env_ctx: Callable
    parse_df: Callable

    def __call__(self, df, parse=True, verbose=True, env=None, **kwargs):
        with self.env_ctx(env or RunConfig.load().write_env):
            return self.fun(self.parse_df(df, verbose) if parse else df, **kwargs)
