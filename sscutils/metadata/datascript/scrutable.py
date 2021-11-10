from functools import partial
from typing import List, Optional, Type

from ...artifact_context import ArtifactContext
from ...helpers import get_associated_step
from ...metaprogramming import camel_to_snake, snake_to_camel
from ...naming import FEATURES_CLS_SUFFIX, INDEX_CLS_SUFFIX
from .bases import BaseEntity, IndexBase, TableFeaturesBase


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

        self.name: str = _infer_table_name(name, features, index)
        self.index = index
        self.features = self._infer_features_cls(features)

        self.subject: Type[BaseEntity] = self._infer_subject(
            subject_of_records
        )
        self.namespace = self._infer_namespace(namespace)
        self.partitioning_cols = partitioning_cols
        self.max_partition_size = max_partition_size
        self.trepo = ArtifactContext().create_trepo(
            self.name,
            self.namespace,
            self.partitioning_cols,
            self.max_partition_size,
        )

    def get_full_df(self):
        return self.trepo.get_full_df()

    def get_full_ddf(self):
        return self.trepo.get_full_ddf()

    def _infer_features_cls(self, features):
        return self._new_cls(
            features, TableFeaturesBase, FEATURES_CLS_SUFFIX.title()
        )

    def _infer_subject(self, subj) -> Type[BaseEntity]:
        return self._new_cls(subj, BaseEntity)

    def _infer_namespace(self, namespace):
        if namespace:
            return namespace
        return get_associated_step(self.features)

    def _new_cls(self, poss_cls, parent_cls, suffix=""):
        if poss_cls is not None:
            assert parent_cls in parent_cls.mro()
            return poss_cls
        cls_name = snake_to_camel(self.name) + suffix
        return type(cls_name, (parent_cls,), {})


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
