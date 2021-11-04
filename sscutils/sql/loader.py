from dataclasses import dataclass
from datetime import datetime
from itertools import chain

import pandas as pd
import sqlalchemy as sa
from colassigner.constants import PREFIX_SEP
from parquetranger import TableRepo
from sqlalchemy.orm import sessionmaker

from ..helpers import create_trepo
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
        """

        self.engine = sa.create_engine(constr, echo=echo)
        self.sql_meta = sa.MetaData()
        self._Session = sessionmaker(self.engine)
        self._a_meta = ArtifactMetadata.load_serialized()

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

    def purge(self):
        self.sql_meta.drop_all(self.engine)
        for ns in self._namespaces:
            ns_id = ns.local_name
            if ns_id:
                self.engine.execute(sa.schema.DropSchema(ns_id))

    def _ns_mappers(self):
        for ns in self._namespaces:
            yield NamespaceMapper(ns, self._a_meta, self.sql_meta)

    @property
    def _namespaces(self):
        return self._a_meta.local_namespaces


@dataclass
class NamespaceMapper:
    ns_meta: NamespaceMetadata
    a_meta: ArtifactMetadata
    sql_meta: sa.MetaData

    def create_schema(self):
        for table in self.ns_meta.tables:
            self._parse_table(table)

    def load_data(self, session, env=None):
        for table in self.ns_meta.tables:
            self._load_table(table, session, env)

    def _parse_table(self, table: Table):
        all_cols = self._get_col_list(table.features)
        if table.index:
            ind_cols = self._get_col_list(table.index)
            all_cols += ind_cols + [
                sa.Index(
                    f"_{self._ns_id}_{table.name}_index",
                    *ind_cols,
                    unique=True,
                )
            ]
        sa.Table(
            table.name,
            self.sql_meta,
            *all_cols,
            schema=self._ns_id or None,
        )

    def _load_table(self, table: Table, session, env):
        trepo = table_to_trepo(table, self._ns_id)
        if env is not None:
            trepo.set_env(env)

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

    def _get_col_list(self, feats):
        return [*chain(*map(self._to_sql_columns, feats))]

    def _to_sql_columns(self, feat, init_prefix=(), calling_prefix=None):
        if isinstance(feat, PrimitiveFeature):
            return [
                sa.Column(
                    PREFIX_SEP.join([*init_prefix, feat.name]),
                    sa_type_map[feat.dtype],
                )
            ]
        if isinstance(feat, CompositeFeature):
            sub_id = feat.dtype
            subfeats = self._get(sub_id, calling_prefix).features
        elif isinstance(feat, ForeignKey):
            sub_id = feat.table
            subfeats = self._get(sub_id, calling_prefix).index
        new_calling_prefix = (
            sub_id.ns_prefix
            if sub_id.ns_prefix is not None
            else calling_prefix
        )
        return chain(
            *[
                self._to_sql_columns(
                    sf, (*init_prefix, feat.prefix), new_calling_prefix
                )
                for sf in subfeats
            ]
        )

    def _get(self, ns_id: NamespacedId, ns_name=None):
        if ns_id.is_local and ns_name is not None:
            ns_id = NamespacedId(ns_name, ns_id.obj_id)
        return self.ns_meta.get_full(ns_id, self.a_meta)

    def _get_sql_table(self, table_name):
        full_table_id = ".".join(filter(None, [self._ns_id, table_name]))
        return self.sql_meta.tables[full_table_id]

    @property
    def _ns_id(self):
        return self.ns_meta.local_name


def table_to_trepo(table: Table, ns) -> TableRepo:
    return create_trepo(
        table.name, ns, table.partitioning_cols, table.partition_max_rows
    )
