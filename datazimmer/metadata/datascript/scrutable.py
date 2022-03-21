from contextlib import contextmanager
from typing import List, Optional, Type

from structlog import get_logger

from ...config_loading import Config, RunConfig, UnavailableTrepo
from ...exceptions import ArtifactRuntimeException
from ...get_runtime import get_runtime
from ...metaprogramming import camel_to_snake, snake_to_camel
from ...naming import FEATURES_CLS_SUFFIX, INDEX_CLS_SUFFIX
from ..bedrock.complete_id import CompleteId, CompleteIdBase
from ..bedrock.conversion import table_id_to_dtype_maps
from .bases import BaseEntity, IndexBase, TableFeaturesBase

logger = get_logger()


class ScruTable:
    def __init__(
        self,
        features: Type[TableFeaturesBase] = None,
        index: Optional[Type[IndexBase]] = None,
        subject_of_records: Optional[Type[BaseEntity]] = None,
        partitioning_cols: Optional[List[str]] = None,
        max_partition_size: Optional[int] = None,
    ) -> None:
        """
        name  parsed from features or index
        """
        assert index or features, "No index, no features: no table"
        try:
            self._conf = Config.load()
        except FileNotFoundError:
            raise ArtifactRuntimeException("can only init Scrutable in an artifact")
        self.id_: CompleteId = self._infer_id(features, index)
        self.index = index
        self.features = self._infer_features_cls(features)

        self.subject: Type[BaseEntity] = self._infer_subject(subject_of_records)
        self.partitioning_cols = partitioning_cols
        self.max_partition_size = max_partition_size
        self.trepo = self._conf.create_trepo(
            self.id_,
            self.partitioning_cols,
            self.max_partition_size,
        )
        self.get_full_df = self._read_wrap(self.trepo.get_full_df)
        self.get_full_ddf = self._read_wrap(self.trepo.get_full_ddf)
        self.extend = self._write_wrap(self.trepo.extend)
        self.replace_all = self._write_wrap(self.trepo.replace_all)
        self.replace_records = self._write_wrap(self.trepo.replace_records)
        self.replace_groups = self._write_wrap(self.trepo.replace_groups)

    @property
    def name(self):
        return self.id_.obj_id

    @contextmanager
    def env_ctx(self, env):
        if isinstance(self.trepo, UnavailableTrepo):
            raise ArtifactRuntimeException(f"trepo unavailable for {self.id_}")
        true_env = self._conf.resolve_ns_env(self.id_.artifact, env)
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
        runtime = get_runtime()
        feat_dic, ind_dic = table_id_to_dtype_maps(self.id_, runtime)
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
        inds = [*ind_dic.keys()]
        return (out.set_index(inds) if set_ind else out).loc[:, [*feat_dic.keys()]]

    def _infer_id(self, features_cls, index_cls) -> str:
        if features_cls is not None:
            cls, suf = features_cls, FEATURES_CLS_SUFFIX
        else:
            cls, suf = index_cls, INDEX_CLS_SUFFIX
        snake_name = camel_to_snake(cls.__name__)
        if not snake_name.endswith(f"_{suf}"):
            raise NameError(f"{cls} class name should end in {suf}")
        name = snake_name[: -(len(suf) + 1)]
        return CompleteIdBase.from_cls(cls, self._conf.name).to_id(name)

    def _infer_features_cls(self, features):
        return self._maybe_new_cls(
            features, TableFeaturesBase, FEATURES_CLS_SUFFIX.title()
        )

    def _infer_subject(self, subj) -> Type[BaseEntity]:
        return self._maybe_new_cls(subj, BaseEntity)

    def _maybe_new_cls(self, poss_cls, parent_cls, suffix=""):
        if poss_cls is not None:
            assert parent_cls in parent_cls.mro()
            return poss_cls
        cls_name = snake_to_camel(self.name) + suffix
        return type(
            cls_name,
            (parent_cls,),
            {"__module__": self.id_.module},
        )
