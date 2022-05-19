from abc import abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Union

from ..primitive_types import PrimitiveType
from ..utils import PRIMITIVE_MODULES, get_simplified_mro
from .datascript import (
    AbstractEntity,
    CompositeTypeBase,
    IndexIndicator,
    Nullable,
    get_feature_dict,
)

_GLOBAL_CLS_MAP = {}


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
    def from_cls(cls, ds_cls):
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

    def _extend(self, ids, props, ds_cls):
        self.parents = [
            p for p in get_simplified_mro(ds_cls) if p is not AbstractEntity
        ]
        self.identifiers = ids
        self.properties = props


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
