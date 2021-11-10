from dataclasses import dataclass
from datetime import datetime
from functools import partial
from itertools import chain
from typing import List

import pandas as pd
import sqlalchemy as sa
from colassigner.constants import PREFIX_SEP
from parquetranger import TableRepo
from sqlalchemy.orm import sessionmaker

from ..artifact_context import ArtifactContext
from ..metadata import ArtifactMetadata
from ..metadata.bedrock.atoms import Table
from ..metadata.bedrock.feature_types import (
    CompositeFeature,
    ForeignKey,
    PrimitiveFeature,
)
from ..metadata.bedrock.namespace_metadata import NamespaceMetadata
from ..metadata.bedrock.namespaced_id import NamespacedId

sa_type_map = {
    int: sa.Integer,
    float: sa.Float,
    str: sa.String,
    bytes: sa.LargeBinary,
    bool: sa.Boolean,
    datetime: sa.DateTime,
}


class SqlLoader:
    """loads an entire artifact environment to an sql database

    metadata needs to be serialized

    """

    def __init__(self, constr="sqlite:///:memory:", echo=False) -> None:
        """start up a loader

        Parameters
        ----------
        constr : str, optional
            constring where database is found, by default "sqlite:///:memory:"
            but needs to be postgres if multiple namespaces are present
        """

        self.ctx = ArtifactContext()
        self.engine = sa.create_engine(constr, echo=echo)
        self.sql_meta = sa.MetaData()
        self._Session = sessionmaker(self.engine)

    def setup_schema(self):
        for nsm in self._ns_mappers():
            ns_id = nsm.ns_meta.local_name
            if ns_id:
                self.engine.execute(sa.schema.CreateSchema(ns_id))
            nsm.create_schema()
        self.sql_meta.create_all(self.engine)

    def load_data(self, env=None):
        with self._Session() as session:
            for nsm in self._ns_mappers():
                nsm.load_data(session, env)
            session.commit()

    def validate_data(self, env=None):
        for nsm in self._ns_mappers():
            nsm.validate_data(self.engine, env)

    def purge(self):
        self.sql_meta.drop_all(self.engine)
        for ns in self._namespaces:
            ns_id = ns.local_name
            if ns_id:
                self.engine.execute(sa.schema.DropSchema(ns_id))

    def _ns_mappers(self):
        for ns in self._namespaces:
            yield NamespaceMapper(ns, self.ctx.metadata, self.sql_meta)

    @property
    def _namespaces(self):
        return self.ctx.ns_w_data


@dataclass
class NamespaceMapper:
    ns_meta: NamespaceMetadata
    a_meta: ArtifactMetadata
    sql_meta: sa.MetaData

    def create_schema(self):
        for table in self.ns_meta.tables:
            SqlTableConverter(table, self).create()

    def load_data(self, session, env=None):
        for table in self.ns_meta.tables:
            self._load_table(table, session, env)

    def validate_data(self, engine, env):
        for table in self.ns_meta.tables:
            self._validate_table(table, engine, env)

    def get_br(self, ns_id: NamespacedId, ns_name=None):
        if ns_id.is_local and ns_name is not None:
            ns_id = NamespacedId(ns_name, ns_id.obj_id)
        return self.ns_meta.get_full(ns_id, self.a_meta)

    def _load_table(self, table: Table, session, env):
        trepo = table_to_trepo(table, self.ns_id, env)
        ins = self._get_sql_table(table.name).insert()

        # TODO partition df properly
        df = trepo.get_full_df()
        relevant_ind = df.index.name is not None or isinstance(
            df.index, pd.MultiIndex
        )
        ins_obj = ins.values(
            (df.reset_index() if relevant_ind else df).to_dict("records")
        )
        session.execute(ins_obj)

    def _validate_table(self, table: Table, engine, env):
        df_sql = pd.read_sql(
            f"SELECT * FROM {_get_sql_id(table.name, self.ns_id)}", con=engine
        )
        trepo = table_to_trepo(table, self.ns_id, env)
        df = trepo.get_full_df()
        if table.index:
            sql_conv = SqlTableConverter(table, self)
            ind_cols = [ic.name for ic in sql_conv.ind_cols]
            if len(ind_cols) > 1:
                ind_cols = [
                    ind_cols[ind_cols.index(inc)] for inc in df.index.names
                ]
            df_sql = df_sql.set_index(ind_cols)
        pd.testing.assert_frame_equal(df.loc[:, df_sql.columns], df_sql)

    def _get_sql_table(self, table_name):
        return self.sql_meta.tables[_get_sql_id(table_name, self.ns_id)]

    @property
    def ns_id(self):
        return self.ns_meta.local_name


class SqlTableConverter:
    def __init__(self, bedrock_table: Table, parent_mapper: NamespaceMapper):
        self._table = bedrock_table
        self._mapper = parent_mapper
        self.fk_constraints = []
        self.ind_cols = self._feats_to_cols(self._table.index or [])
        self.feat_cols = self._feats_to_cols(self._table.features)

    def create(self):
        sa.Table(
            self._table.name,
            self._mapper.sql_meta,
            *self._schema_items,
            schema=self._mapper.ns_id or None,
        )

    @property
    def pk_constraints(self):
        if self.ind_cols:
            return [
                sa.Index(
                    f"_{self._sql_id}_index",
                    *self.ind_cols,
                    unique=True,
                )
            ]
        return []

    def _feats_to_cols(self, feats):
        return _chainmap(self._feat_to_sql_cols, feats)

    def _feat_to_sql_cols(
        self, feat, init_prefix=(), calling_ns_prefix=None, open_to_fk=True
    ):
        new_open_to_fk = True
        fk_to = None
        if isinstance(feat, PrimitiveFeature):
            name = PREFIX_SEP.join([*init_prefix, feat.name])
            return [sa.Column(name, sa_type_map[feat.dtype])]
        if isinstance(feat, CompositeFeature):
            sub_id = feat.dtype
            subfeats = self._get(sub_id, calling_ns_prefix).features
        elif isinstance(feat, ForeignKey):
            new_open_to_fk = False
            sub_id = feat.table
            fk_to = self._get_fk_tab_id(sub_id, calling_ns_prefix)
            table_obj = self._get(sub_id, calling_ns_prefix)
            subfeats = table_obj.index

        new_ns_prefix = (
            sub_id.ns_prefix
            if sub_id.ns_prefix is not None
            else calling_ns_prefix
        )
        new_feat_prefix = (*init_prefix, feat.prefix)
        new_fun = partial(
            self._feat_to_sql_cols,
            init_prefix=new_feat_prefix,
            calling_ns_prefix=new_ns_prefix,
            open_to_fk=new_open_to_fk,
        )
        out = _chainmap(new_fun, subfeats)
        if fk_to is not None and open_to_fk:
            self._add_fk(out, fk_to, new_feat_prefix)

        return out

    def _add_fk(self, cols: List[sa.Column], table_id, prefix_arr):
        pref_str = PREFIX_SEP.join(prefix_arr) + PREFIX_SEP
        tab_id_str = _get_sql_id(table_id.obj_id, table_id.ns_prefix) + "."
        matching_cols = [c.name.replace(pref_str, tab_id_str) for c in cols]

        fk = sa.ForeignKeyConstraint(
            cols,
            matching_cols,
            name=f"_{self._sql_id}_{pref_str}_fk",
            initially="DEFERRED",
        )

        self.fk_constraints.append(fk)

    def _get(self, id_, pref):
        return self._mapper.get_br(id_, pref)

    def _get_fk_tab_id(self, full_id: NamespacedId, calling_ns):
        if full_id.is_local:
            if calling_ns is None:
                calling_ns = self._mapper.ns_id
            return NamespacedId(calling_ns, full_id.obj_id)
        return full_id

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
        return _get_sql_id(self._table.name, self._mapper.ns_id)


def table_to_trepo(table: Table, ns: str, env=None) -> TableRepo:
    trepo = ArtifactContext().create_trepo(
        table.name, ns, table.partitioning_cols, table.partition_max_rows
    )
    if env is not None:
        trepo.set_env(env)
    return trepo


def _chainmap(fun, iterable):
    return [*chain(*map(fun, iterable))]


def _get_sql_id(table_name, ns_id):
    return ".".join(filter(None, [ns_id, table_name]))
