from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from inspect import getmodule, stack
from typing import Dict, List, Optional, Type

import sqlalchemy as sa
from colassigner.constants import PREFIX_SEP
from structlog import get_logger

from ..config_loading import Config, RunConfig, UnavailableTrepo
from ..exceptions import ProjectRuntimeException
from ..primitive_types import PrimitiveType, get_np_type, get_sa_type
from ..utils import camel_to_snake, chainmap
from .atoms import CompositeFeature, EntityClass, ObjectProperty, PrimitiveFeature
from .complete_id import CompleteId, CompleteIdBase
from .datascript import AbstractEntity

logger = get_logger()


class ScruTable:
    def __init__(
        self,
        entity: Type[AbstractEntity],
        entity_key_table_map: Optional[Dict[str, "ScruTable"]] = None,
        partitioning_cols: Optional[List[str]] = None,
        max_partition_size: Optional[int] = None,
    ) -> None:
        # TODO: somehow add possibility for a description

        self._conf = Config.load()
        self.module_name = getmodule(stack()[1][0]).__name__
        self.id_: CompleteId = self._infer_id(entity, self.module_name)
        self.key_map = entity_key_table_map or {}
        self.entity_class: EntityClass = EntityClass.from_cls(entity)

        self.name = self.id_.obj_id
        self.index = self.entity_class.identifiers
        self.features = self.entity_class.properties
        self.index_map = to_dt_map(self.index)
        self.features_map = to_dt_map(self.features)
        self.dtype_map = {**self.index_map, **self.features_map}
        self.index_cols = [*self.index_map.keys()]
        self.feature_cols = [*self.features_map.keys()]
        self.all_cols = self.index_cols + self.feature_cols

        self.partitioning_cols = partitioning_cols
        self.max_partition_size = max_partition_size
        self.trepo = self._conf.create_trepo(
            self.id_,
            self.partitioning_cols,
            self.max_partition_size,
        )
        self.get_full_df = self._read_wrap(self.trepo.get_full_df)
        self.get_full_ddf = self._read_wrap(self.trepo.get_full_ddf)
        self.map_partitions = self._read_wrap(self.trepo.map_partitions)

        self.extend = self._write_wrap(self.trepo.extend)
        self.replace_all = self._write_wrap(self.trepo.replace_all)
        self.replace_records = self._write_wrap(self.trepo.replace_records)
        self.replace_groups = self._write_wrap(self.trepo.replace_groups)

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

    def purge(self):
        with self.env_ctx(RunConfig.load().write_env):
            self.trepo.purge()

    @contextmanager
    def env_ctx(self, env):
        if isinstance(self.trepo, UnavailableTrepo):
            raise ProjectRuntimeException(f"trepo unavailable for {self.id_}")
        true_env = self._conf.resolve_ns_env(self.id_.project, env)
        with self.trepo.env_ctx(true_env):
            yield

    def _read_wrap(self, fun):
        def f(env=None, **kwargs):
            with self.env_ctx(env or RunConfig.load().read_env):
                return fun(**kwargs)

        return f

    def _write_wrap(self, fun):
        def f(df, parse=True, verbose=True, env=None, **kwargs):
            with self.env_ctx(env or RunConfig.load().write_env):
                return fun(self._parse_df(df, verbose) if parse else df, **kwargs)

        return f

    def _parse_df(self, df, verbose=True):
        if verbose:
            logger.info("parsing", table=self.name, namespace=self.id_.namespace)
        full_dic = self.features_map.copy()
        set_ind = self.index_map and (set(df.index.names) != set(self.index_cols))
        if set_ind:
            if verbose:
                logger.info("indexing needed", inds=self.index_cols)
            full_dic.update(self.index_map)

        missing_cols = set(full_dic.keys()) - set(df.columns)
        if missing_cols:
            logger.warn(f"missing from columns {missing_cols}", present=df.columns)
        out = df.astype(full_dic)
        indexed_out = out.set_index(self.index_cols) if set_ind else out
        return indexed_out.loc[:, self.feature_cols]

    def _infer_id(self, entity_cls, initing_module_name) -> str:
        id_base = CompleteIdBase.from_module_name(initing_module_name, self._conf.name)
        return id_base.to_id(camel_to_snake(entity_cls.__name__))


@dataclass
class Column:
    name: str
    dtype: PrimitiveType
    nullable: bool = False


def feats_to_cols(feats, proc_fk=None, wrap=lambda x: x) -> List:
    return chainmap(partial(feat_to_cols, proc_fk=proc_fk, wrap=wrap), feats)


def feat_to_cols(feat, proc_fk, wrap, init_prefix=(), open_to_fk=True) -> List:

    new_open_to_fk = True
    fk_to = None
    if isinstance(feat, PrimitiveFeature):
        name = PREFIX_SEP.join([*init_prefix, feat.name])
        return [wrap(Column(name, feat.dtype, feat.nullable))]

    new_feat_prefix = (*init_prefix, feat.prefix)
    if isinstance(feat, CompositeFeature):
        subfeats = feat.dtype.features
    elif isinstance(feat, ObjectProperty):
        new_open_to_fk = False
        fk_to = feat.target
        subfeats = fk_to.identifiers

    new_fun = partial(
        feat_to_cols,
        init_prefix=new_feat_prefix,
        open_to_fk=new_open_to_fk,
        proc_fk=proc_fk,
        wrap=wrap,
    )
    out = chainmap(new_fun, subfeats)
    if fk_to is not None and open_to_fk and proc_fk:
        proc_fk(out, fk_to, new_feat_prefix)
    return out


def to_dt_map(feats):
    return {c.name: get_np_type(c.dtype) for c in feats_to_cols(feats)}


def to_sa_col(col: Column, pk=False):
    sa_dt = get_sa_type(col.dtype)
    return sa.Column(col.name, sa_dt, nullable=col.nullable, primary_key=pk)
