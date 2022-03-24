from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, List

import pandas as pd
import sqlalchemy as sa
from colassigner.constants import PREFIX_SEP
from sqlalchemy.orm import sessionmaker
from structlog import get_logger
from tqdm import tqdm

from ..get_runtime import get_runtime
from ..metadata.bedrock.atoms import Table
from ..metadata.bedrock.column import to_sql_col
from ..metadata.bedrock.complete_id import CompleteId, CompleteIdBase
from ..metadata.bedrock.conversion import FeatConverter, table_id_to_dtype_map
from ..metadata.bedrock.namespace_metadata import NamespaceMetadata
from ..utils import is_postgres

if TYPE_CHECKING:
    from ..artifact_context import ArtifactContext  # pragma: no cover


logger = get_logger(ctx="sql loader")


class SqlLoader:
    """loads an entire artifact environment to an sql database

    metadata needs to be serialized

    """

    def __init__(self, constr, env, echo=False, batch_size=2000):
        """start up a loader

        Parameters
        ----------
        constr : str, optional
            constring where database is found, by default "sqlite:///:memory:"
            but needs to be postgres for foreign keys to be validated
        """

        self.env = env
        self.runtime = get_runtime()
        self.engine = sa.create_engine(constr, echo=echo)
        self.sql_meta = sa.MetaData()
        self._Session = sessionmaker(self.engine)
        self._batch_size = batch_size

    def setup_schema(self):
        for nsm in self._ns_mappers:
            nsm.create_schema()
        self.sql_meta.create_all(self.engine)

    def load_data(self):
        with self._Session() as session:
            for nsm in self._ns_mappers:
                nsm.load_data(session, self.env)
            session.commit()

    def validate_data(self):
        for nsm in self._ns_mappers:
            nsm.validate_data(self.env)

    def purge(self):
        self.sql_meta.drop_all(self.engine)

    @property
    def _ns_mappers(self):
        f_args = (self.runtime, self.sql_meta, self.engine, self._batch_size)
        for ns in self.runtime.metadata.namespaces.values():
            yield NamespaceMapper(self.runtime.name, ns, *f_args)
        for data_env in self.runtime.data_to_load:
            art_name = data_env.artifact
            resolved_env = self.runtime.config.resolve_ns_env(art_name, self.env)
            if data_env.env != resolved_env:
                continue
            ext_ns = self.runtime.ext_metas[art_name].namespaces[data_env.ns]
            yield NamespaceMapper(art_name, ext_ns, *f_args)


@dataclass
class NamespaceMapper:

    artifact_name: str
    ns_meta: NamespaceMetadata
    runtime: "ArtifactContext"
    sql_meta: sa.MetaData
    engine: sa.engine.Engine
    batch_size: int

    def create_schema(self):
        for table in self.ns_meta.tables:
            SqlTableConverter(table, self).create()

    def load_data(self, session, env):
        for table in self.ns_meta.tables:
            self._load_table(table, session, env)

    def validate_data(self, env):
        for table in self.ns_meta.tables:
            self._validate_table(table, env)

    def sql_id(self, table_name):
        return _get_sql_id(table_name, self.id_base)

    @property
    def id_base(self):
        return CompleteIdBase(self.artifact_name, self.ns_meta.name)

    def _load_table(self, table: Table, session, env):
        trepo = self.runtime.config.table_to_trepo(table, self.id_base, env)
        ins = self._get_sql_table(table.name).insert()
        has_ind = table.index
        _log = partial(logger.info, table=table.name)
        _log("loading")
        for df in trepo.dfs:
            self._partition(df.reset_index() if has_ind else df, ins, session)

    def _validate_table(self, table: Table, env):
        dt_map = {}
        table_id = self.sql_id(table.name)
        logger.info("validating table", table=table_id, env=env)
        if not is_postgres(self.engine):
            comp_tid = self.id_base.to_id(table.name)
            dt_map = table_id_to_dtype_map(comp_tid, self.runtime)
        df_sql = pd.read_sql(
            f"SELECT * FROM {table_id}",
            con=self.engine,
        ).astype(dt_map)
        trepo = self.runtime.config.table_to_trepo(table, self.id_base, env)
        df = trepo.get_full_df()
        if table.index:
            sql_conv = SqlTableConverter(table, self)
            ind_cols = [ic.name for ic in sql_conv.ind_cols]
            if len(ind_cols) > 1:
                ind_cols = [ind_cols[ind_cols.index(inc)] for inc in df.index.names]
            df_sql = df_sql.set_index(ind_cols).reindex(df.index)
        else:
            df, df_sql = [
                _df.sort_values(df_sql.columns.tolist()).reset_index(drop=True)
                for _df in [df, df_sql]
            ]
        if df.empty and df_sql.empty:
            logger.warn("empty data frames", table=table_id, env=env)

        pd.testing.assert_frame_equal(df.loc[:, df_sql.columns], df_sql)

    def _partition(self, df: pd.DataFrame, ins, session):
        for sind in tqdm(range(0, df.shape[0], self.batch_size)):
            eind = sind + self.batch_size
            recs = df.iloc[sind:eind, :].to_dict("records")
            session.execute(ins.values([*map(_parse_d, recs)]))

    def _get_sql_table(self, table_name):
        return self.sql_meta.tables[self.sql_id(table_name)]


class SqlTableConverter:
    def __init__(self, bedrock_table: Table, parent_mapper: NamespaceMapper):
        self._table = bedrock_table
        self._mapper = parent_mapper
        self.fk_constraints = []
        col_conversion_fun = FeatConverter(
            self._mapper.runtime, self._mapper.id_base, to_sql_col, self._add_fk
        ).feats_to_cols
        self.ind_cols = col_conversion_fun(self._table.index)
        self.feat_cols = col_conversion_fun(self._table.features)

    def create(self):
        sa.Table(
            self._sql_id,
            self._mapper.sql_meta,
            *self._schema_items,
        )

    @property
    def pk_constraints(self):
        if self.ind_cols:
            ind = sa.Index(f"_{self._sql_id}_i", *self.ind_cols, unique=True)
            return [ind]
        return []

    def _add_fk(self, sql_cols: List[sa.Column], table_id: CompleteId, prefix_arr):
        pref_str = PREFIX_SEP.join(prefix_arr) + PREFIX_SEP
        tab_id_str = _get_sql_id(table_id.obj_id, table_id.base) + "."
        matching_cols = [c.name.replace(pref_str, tab_id_str) for c in sql_cols]

        defer_kws = {}
        if is_postgres(self._mapper.engine):
            defer_kws["initially"] = "DEFERRED"

        fk = sa.ForeignKeyConstraint(
            sql_cols,
            matching_cols,
            name=f"_{self._sql_id}_{pref_str}_fk",
            **defer_kws,
        )
        self.fk_constraints.append(fk)

    @property
    def _schema_items(self):
        return [
            *self.feat_cols,
            *self.ind_cols,
            *self.fk_constraints,
            *self.pk_constraints,
        ]

    @property
    def _sql_id(self):
        return self._mapper.sql_id(self._table.name)


def _get_sql_id(table_name, id_base: CompleteIdBase):
    return "__".join([id_base.artifact, id_base.namespace, table_name])


def _parse_d(d):
    return {k: None if pd.isna(v) else v for k, v in d.items()}
