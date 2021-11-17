from dataclasses import dataclass, field
from typing import List, Optional, Union

from ...metaprogramming import snake_to_camel
from ...naming import FEATURES_CLS_SUFFIX, INDEX_CLS_SUFFIX
from ...utils import is_type_hint_origin
from .feature_types import ALL_FEATURE_TYPES, ANY_FEATURE_TYPE
from .namespaced_id import NamespacedId, NamespacedIdOf

ATOM_ID_KEY = "name"


class _AtomBase:
    def __post_init__(self):
        fl_hint = List[ANY_FEATURE_TYPE]
        for attname, hint in self.__annotations__.items():
            parsed_v = getattr(self, attname)
            if (hint == Optional[fl_hint]) or (hint == fl_hint):
                parsed_v = _parse_poss_feat_list(parsed_v)
            elif is_type_hint_origin(hint, NamespacedIdOf):
                parsed_v = NamespacedId.from_serialized_id(parsed_v)
            elif _is_ns_id_list(hint):
                parsed_v = [*map(NamespacedId.from_serialized_id, parsed_v)]

            setattr(self, attname, parsed_v)

    def to_dict(self):
        out = {}
        for attname in self.__annotations__:
            parsed_v = getattr(self, attname)
            if not parsed_v or (attname == ATOM_ID_KEY):
                continue
            if isinstance(parsed_v, list):
                parsed_v = [*map(_serialize, parsed_v)]
            else:
                parsed_v = _serialize(parsed_v)
            out[attname] = parsed_v
        return out


@dataclass
class CompositeType(_AtomBase):
    name: str
    features: List[ANY_FEATURE_TYPE]
    description: Optional[str] = None


@dataclass
class EntityClass(_AtomBase):
    name: str
    parents: List[NamespacedIdOf["EntityClass"]] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class Table(_AtomBase):
    name: str
    subject_of_records: NamespacedIdOf[EntityClass]
    features: List[ANY_FEATURE_TYPE] = field(default_factory=list)
    index: Optional[List[ANY_FEATURE_TYPE]] = field(default_factory=list)
    partitioning_cols: Optional[List[str]] = None  # TODO: not sure
    partition_max_rows: Optional[int] = None
    description: Optional[str] = None

    @property
    def index_cls_id(self) -> str:
        return get_index_cls_name(self.name)

    @property
    def feature_cls_id(self) -> str:
        return _add_suffix(self.name, FEATURES_CLS_SUFFIX)

    @property
    def features_w_ind(self) -> list:
        return self.features + self.index


def get_index_cls_name(snake):
    return _add_suffix(snake, INDEX_CLS_SUFFIX)


NS_ATOM_TYPE = Union[EntityClass, CompositeType, Table]


def _serialize(obj):
    if isinstance(obj, NamespacedId):
        return obj.serialized_id
    if isinstance(obj, ALL_FEATURE_TYPES):
        return obj.to_dict()
    return obj


def _parse_poss_feat_list(feat_list):
    if feat_list is None:
        return
    new_atts = []
    for poss_feat in feat_list:
        if type(poss_feat) in ALL_FEATURE_TYPES:
            new_atts.append(poss_feat)
            continue
        new_atts.append(_try_feat(poss_feat))
    return new_atts


def _try_feat(dic):
    for cls in ALL_FEATURE_TYPES:
        try:
            return cls(**dic)
            break
        except TypeError:
            pass
    else:
        raise TypeError(f"cant parse {dic} to any feature type")


def _is_ns_id_list(hint):
    try:
        return is_type_hint_origin(hint.__args__[0], NamespacedIdOf)
    except AttributeError:
        return False


def _add_suffix(snake, suffix: str):
    return snake_to_camel(f"{snake}_{suffix}")
