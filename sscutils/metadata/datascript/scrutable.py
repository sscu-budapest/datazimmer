from functools import partial
from typing import List, Optional, Type

from structlog import get_logger

from ...artifact_context import ArtifactContext
from ...helpers import get_associated_step
from ...metaprogramming import camel_to_snake, snake_to_camel
from ...naming import FEATURES_CLS_SUFFIX, INDEX_CLS_SUFFIX, TMP_CLS_MODULE
from ..bedrock.artifact_metadata import ArtifactMetadata
from ..bedrock.atoms import Table
from ..bedrock.column import to_dt_map
from ..bedrock.conversion import FeatConverter
from .bases import BaseEntity, IndexBase, TableFeaturesBase

logger = get_logger()


class ScruTable:
    def __init__(
        self,
        features: Type[TableFeaturesBase] = None,
        index: Optional[Type[IndexBase]] = None,
        name: Optional[str] = None,
        subject_of_records: Optional[Type[BaseEntity]] = None,
        namespace: Optional[str] = None,
        partitioning_cols: Optional[List[str]] = None,
        max_partition_size: Optional[int] = None,
    ) -> None:
        """
        name if not given, is parsed from features (and index)

        can be

        - table in dataset
        - imported table to a project
          - from a dataset
          - from a project step output
        - table of a step output in a project

        figures out whether its in a dataset, or a project
        """
        assert index or features, "No index, no features: no table"
        self.name: str = _infer_table_name(name, features, index)
        self.namespace = self._infer_namespace(namespace, features, index)
        self.index = index
        self.features = self._infer_features_cls(features)

        self.subject: Type[BaseEntity] = self._infer_subject(
            subject_of_records
        )
        self.partitioning_cols = partitioning_cols
        self.max_partition_size = max_partition_size
        self.trepo = ArtifactContext().create_trepo(
            self.name,
            self.namespace,
            self.partitioning_cols,
            self.max_partition_size,
        )
        self.get_full_df = self.trepo.get_full_df
        self.get_full_ddf = self.trepo.get_full_ddf
        self.extend = self._parsewrap(self.trepo.extend)
        self.replace_all = self._parsewrap(self.trepo.replace_all)
        self.replace_records = self._parsewrap(self.trepo.replace_records)
        self.replace_groups = self._parsewrap(self.trepo.replace_groups)

    def _parsewrap(self, fun):
        def f(df, parse: bool = True, verbose=True):
            if parse:
                return fun(self._parse_df(df, verbose))
            return fun(df)

        return f

    def _parse_df(self, df, verbose=True):
        if verbose:
            logger.info("parsing", table=self.name, namespace=self.namespace)
        try:
            feat_dic, ind_dic = self._get_dtype_maps()
        except AssertionError:
            return df
        full_dic = feat_dic.copy()
        set_ind = ind_dic and (set(df.index.names) != set(ind_dic.keys()))
        if set_ind:
            if verbose:
                logger.info("indexing needed", inds=ind_dic)
            full_dic.update(ind_dic)

        missing_cols = set(full_dic.keys()) - set(df.columns)
        if missing_cols:
            logger.warn(f"missing from columns {missing_cols}")
        out = df.astype(full_dic)
        if set_ind:
            return out.set_index([*ind_dic.keys()])
        return out.loc[:, [*feat_dic.keys()]]

    def _get_dtype_maps(self):
        # TODO make this update
        a_meta = ArtifactMetadata.load_serialized()
        try:
            ns_meta = a_meta.namespaces[self.namespace]
            table = ns_meta.get(self.name)
        except KeyError:
            logger.warn(
                f"Metadata for table '{self.name}' "
                f"in namespace '{self.namespace}' not yet serialized. "
                "can't map types of dataframe"
            )
            raise AssertionError()
        return table_to_dtype_maps(table, ns_meta, a_meta)

    def _infer_features_cls(self, features):
        return self._new_cls(
            features, TableFeaturesBase, FEATURES_CLS_SUFFIX.title()
        )

    def _infer_subject(self, subj) -> Type[BaseEntity]:
        return self._new_cls(subj, BaseEntity)

    def _infer_namespace(self, namespace, features, index):
        if namespace:
            return namespace
        return get_associated_step(features or index)

    def _new_cls(self, poss_cls, parent_cls, suffix=""):
        if poss_cls is not None:
            assert parent_cls in parent_cls.mro()
            return poss_cls
        cls_name = snake_to_camel(self.name) + suffix
        return type(cls_name, (parent_cls,), {"__module__": TMP_CLS_MODULE})


class TableFactory:
    def __init__(self, namespace) -> None:
        self.create = partial(ScruTable, namespace=namespace)


def _infer_table_name(name, features_cls, index_cls) -> str:
    if name:
        return name
    if features_cls is not None:
        return _infer_table_name_from_cls(features_cls, FEATURES_CLS_SUFFIX)
    return _infer_table_name_from_cls(index_cls, INDEX_CLS_SUFFIX)


def _infer_table_name_from_cls(cls: Type, suffix=INDEX_CLS_SUFFIX):
    cls_name = cls.__name__
    snake_name = camel_to_snake(cls_name)
    if snake_name.endswith(f"_{suffix}"):
        return snake_name[: -(len(suffix) + 1)]
    raise NameError(
        f"{cls} class name should end in {suffix},"
        f" {cls_name} given, can't infer table name"
    )


def table_to_dtype_maps(table: Table, ns_meta, a_meta):
    convfun = FeatConverter(ns_meta, a_meta).feats_to_cols
    return map(
        lambda feats: to_dt_map(convfun(feats)),
        [table.features, table.index],
    )


def table_to_dtype_map(table: Table, ns_meta, a_meta):
    feat_dic, ind_dic = table_to_dtype_maps(table, ns_meta, a_meta)
    return {**feat_dic, **ind_dic}
