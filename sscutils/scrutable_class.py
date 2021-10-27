from functools import partial
from typing import List, Optional, Type

from parquetranger import TableRepo

from .config_loading import (
    DataEnvironmentToLoad,
    DatasetConfig,
    load_artifact_config,
)
from .exceptions import ProjectSetupException
from .metadata.bases import BaseEntity, IndexBase, TableFeaturesBase
from .metaprogramming import camel_to_snake, snake_to_camel
from .naming import DATA_PATH, FEATURES_CLS_SUFFIX


class ScruTable:
    def __init__(
        self,
        features: Type[TableFeaturesBase],
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


        if it is in a project, it is updated when it is the output of a step,
        when it is put into @pipereg.register(outputs=[...])
        """

        self.features = features
        self.index = index

        self.name: str = self._infer_table_name(name)
        self.subject: Type[BaseEntity] = self._infer_subject(
            subject_of_records
        )

        self.namespace = namespace  # possibly changed
        # in register_as_step_output

        self.artifact_config = load_artifact_config()
        self.is_in_dataset = isinstance(self.artifact_config, DatasetConfig)

        self.partitioning_cols = partitioning_cols
        self.max_partition_size = max_partition_size

        self.trepo: Optional[TableRepo] = None
        # if not set here, set in register_as_step_output

        if self.is_in_dataset:
            parents_dict = {
                env.name: env.path
                for env in self.artifact_config.created_environments
            }
            trepo_path = (
                parents_dict[self.artifact_config.default_env.name] / self.name
            )
        elif namespace is not None:
            parents_dict = {}
            trepo_path = self._get_imported_namespace().out_path / self.name
        else:
            return

        self.trepo = TableRepo(
            trepo_path,
            group_cols=self.partitioning_cols,
            max_records=self.max_partition_size or 0,
            env_parents=parents_dict,
        )

    def validate(self):
        # TODO
        # + deep validate where foreign keys are checked
        pass

    def register_as_step_output(self, namespace: str):
        self.namespace = namespace
        self.trepo = TableRepo(
            DATA_PATH / self.namespace / self.name,
            group_cols=self.partitioning_cols,
            max_records=self.max_partition_size or 0,
        )

    def get_full_df(self):
        return self.trepo.get_full_df()

    def get_full_ddf(self):
        return self.trepo.get_full_ddf()

    def _infer_table_name(self, name) -> str:
        if name:
            return name
        return _infer_table_name_from_cls(self.features, FEATURES_CLS_SUFFIX)

    def _infer_subject(self, subj) -> Type[BaseEntity]:
        if subj is not None:
            assert BaseEntity in subj.mro()
            return subj
        subj_name = snake_to_camel(self.name)
        return type(subj_name, (BaseEntity,), {})

    def _get_imported_namespace(self) -> DataEnvironmentToLoad:
        ns_list = self.artifact_config.data_envs
        for ns in ns_list:
            if ns.local_name == self.namespace:
                return ns
        raise ProjectSetupException(
            f"defined scrutable {self.name} of group {self.namespace}"
            f" could not be found among imported datasets {ns_list}"
        )


class TableFactory:
    def __init__(self, namespace) -> None:
        self.create = partial(ScruTable, namespace=namespace)


def _infer_table_name_from_cls(cls: Type, suffix="index"):
    cls_name = cls.__name__
    snake_name = camel_to_snake(cls_name)
    if snake_name.endswith(f"_{suffix}"):
        return snake_name[: -(len(suffix) + 1)]
    raise NameError(
        f"{cls} class name should end in {suffix},"
        f" {cls_name} given, can't infer table name"
    )
