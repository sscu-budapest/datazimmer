from abc import abstractmethod
from dataclasses import dataclass, field
from functools import partial
from typing import List, Optional, TypeVar, Union

import pandas as pd
import sqlalchemy as sa
from colassigner.constants import PREFIX_SEP
from structlog import get_logger

from ..utils import PRIMITIVE_MODULES, chainmap, get_simplified_mro
from .datascript import (
    AbstractEntity,
    CompositeTypeBase,
    IndexIndicator,
    Nullable,
    PrimitiveType,
    get_feature_dict,
    get_np_type,
    get_sa_type,
)

logger = get_logger("atoms")

_GLOBAL_CLS_MAP: dict[type, "_AtomBase"] = {}
T = TypeVar("T")


@dataclass
class PrimitiveFeature:  # ~DataProperty
    name: str
    dtype: PrimitiveType
    nullable: bool = False
    description: Optional[str] = None


@dataclass
class ObjectProperty:
    prefix: str
    target: "EntityClass"
    description: Optional[str] = None


@dataclass
class CompositeFeature:
    prefix: str
    dtype: "CompositeType"
    description: Optional[str] = None


ANY_FEATURE_TYPE = Union[PrimitiveFeature, CompositeFeature, ObjectProperty]
ALL_FEATURE_TYPES = ANY_FEATURE_TYPE.__args__


class _AtomBase:
    @classmethod
    def from_cls(cls: type[T], ds_cls) -> T:
        inst = _GLOBAL_CLS_MAP.get(ds_cls)
        if inst is None:
            inst = cls(name=ds_cls.__name__, description=ds_cls.__doc__)
            _GLOBAL_CLS_MAP[ds_cls] = inst
        else:
            return inst
        ids, props = _ds_cls_to_feat_dicts(ds_cls)
        inst._extend(ids, props, ds_cls)
        return inst

    @abstractmethod
    def _extend(self, ids, props, ds_cls):
        pass  # pragma: no cover


@dataclass
class CompositeType(_AtomBase):
    name: str
    features: List[ANY_FEATURE_TYPE] = field(default_factory=list)
    description: Optional[str] = None

    def _extend(self, ids, props, _):
        self.features += ids + props


@dataclass
class EntityClass(_AtomBase):
    name: str
    identifiers: List[ANY_FEATURE_TYPE] = field(default_factory=list)
    properties: List[ANY_FEATURE_TYPE] = field(default_factory=list)
    parents: List["EntityClass"] = field(default_factory=list)
    description: Optional[str] = None

    @property
    def table_index_dt_map(self):
        return to_dt_map(self.identifiers)

    @property
    def table_feature_dt_map(self):
        return to_dt_map(self.properties)

    @property
    def table_full_dt_map(self):
        return self.table_index_dt_map | self.table_feature_dt_map

    @property
    def table_index_cols(self):
        return list(self.table_index_dt_map.keys())

    @property
    def table_feature_cols(self):
        return list(self.table_feature_dt_map.keys())

    @property
    def table_all_columns(self):
        return self.table_index_cols + self.table_feature_cols

    def _extend(self, ids, props, ds_cls):
        self.parents = [
            p for p in get_simplified_mro(ds_cls) if p is not AbstractEntity
        ]
        self.identifiers = ids
        self.properties = props


@dataclass
class Column:
    name: str
    dtype: PrimitiveType
    nullable: bool = False


def feats_to_cols(feats, proc_fk=None, wrap=lambda x: x) -> list[Column]:
    return chainmap(partial(feat_to_cols, proc_fk=proc_fk, wrap=wrap), feats)


def feat_to_cols(feat, proc_fk, wrap, init_prefix=(), open_to_fk=True) -> list:
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
    return {c.name: get_np_type(c.dtype, c.nullable) for c in feats_to_cols(feats)}


def to_sa_col(col: Column, pk=False):
    sa_dt = get_sa_type(col.dtype)
    return sa.Column(col.name, sa_dt, nullable=col.nullable, primary_key=pk)


def parse_df(df: pd.DataFrame, entity: AbstractEntity, verbose=False):
    entity_class = EntityClass.from_cls(entity)
    eventual_dic = to_dt_map(entity_class.properties)
    feat_list = list(eventual_dic.keys())
    ind_dic = to_dt_map(entity_class.identifiers)
    set_ind = ind_dic and (set(df.index.names) != set(ind_dic.keys()))
    if set_ind:
        if verbose:
            logger.info("indexing needed", inds=ind_dic)
        eventual_dic.update(ind_dic)

    missing_cols = set(eventual_dic.keys()) - set(df.columns)
    if missing_cols:
        logger.warning(f"missing from columns {missing_cols}", present=df.columns)
    out = df.astype(eventual_dic)
    indexed_out = out.set_index(list(ind_dic.keys())) if set_ind else out
    return indexed_out.loc[:, list(feat_list)]


def _ds_cls_to_feat_dicts(ds_cls: Union[EntityClass, CompositeType]):
    feature_dict = get_feature_dict(ds_cls)
    ids = []
    props = []
    for k, cls in feature_dict.items():
        nullable = False
        to_l = props
        bases = getattr(cls, "mro", list)()
        if isinstance(cls, Nullable):
            cls = cls.base
            nullable = True
        if IndexIndicator in bases:
            to_l = ids
            cls = bases[1]
        if cls.__module__ in PRIMITIVE_MODULES:
            parsed_feat = PrimitiveFeature(name=k, dtype=cls, nullable=nullable)
        elif AbstractEntity in bases:
            entity_class = EntityClass.from_cls(cls)
            parsed_feat = ObjectProperty(prefix=k, target=entity_class)
        elif CompositeTypeBase in bases:
            composite_type = CompositeType.from_cls(cls)
            parsed_feat = CompositeFeature(prefix=k, dtype=composite_type)
        else:
            continue  # maybe some other col to assign

        to_l.append(parsed_feat)
    return ids, props
