from dataclasses import dataclass
from datetime import datetime
from itertools import chain

import pandas as pd
import sqlalchemy as sa
from colassigner.constants import PREFIX_SEP
from parquetranger import TableRepo

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
        self._a_meta = ArtifactMetadata.load_serialized()

    def setup_schema(self):
        for nsm in self._ns_mappers():
            nsm.create_schema()
        self.sql_meta.create_all(self.engine)

    def load_data(self, env=None):
        for nsm in self._ns_mappers():
            nsm.load_data(self.engine, env)

    def _ns_mappers(self):
        for ns in self._a_meta.local_namespaces:
            yield NamespaceMapper(ns, self._a_meta, self.sql_meta)


@dataclass
class NamespaceMapper:
    ns_meta: NamespaceMetadata
    a_meta: ArtifactMetadata
    sql_meta: sa.MetaData

    def create_schema(self):
        for table in self.ns_meta.tables:
            self._parse_table(table)

    def load_data(self, engine, env=None):
        for table in self.ns_meta.tables:
            self._load_table(table, engine, env)

    def _parse_table(self, table: Table):
        all_cols = self._get_col_list(table.features)
        if table.index:
            ind_cols = self._get_col_list(table.index)
            all_cols += ind_cols + [
                sa.Index(f"_{table.name}_index", *ind_cols, unique=True)
            ]
        sa.Table(
            table.name,
            self.sql_meta,
            *all_cols,
            schema=self.ns_meta.local_name or None,
        )

    def _load_table(self, table, engine, env):
        trepo = table_to_trepo(table, self.ns_meta.local_name)
        if env is not None:
            trepo.set_env(env)
        df = trepo.get_full_df()
        relevant_ind = df.index.name is not None or isinstance(
            df.index, pd.MultiIndex
        )
        df.to_sql(
            table.name,
            con=engine,
            if_exists="append",
            index=relevant_ind,
        )

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


def table_to_trepo(table: Table, ns) -> TableRepo:
    return create_trepo(
        table.name, ns, table.partitioning_cols, table.partition_max_rows
    )
