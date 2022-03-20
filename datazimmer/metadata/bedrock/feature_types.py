from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Union

from ...primitive_types import PrimitiveType
from ...utils import is_type_hint_origin
from .complete_id import CompleteId, CompleteIdOf

if TYPE_CHECKING:
    from .atoms import CompositeType, Table  # pragma: no cover


class _FeatBase:
    _content_key = ""
    _id_key = ""

    def __post_init__(self):
        for attname, hint in self.__annotations__.items():
            parsed_v = getattr(self, attname)
            if not isinstance(parsed_v, str):
                continue
            if is_type_hint_origin(hint, CompleteIdOf):
                parsed_v = CompleteId.from_serialized_id(parsed_v)
            elif hint is PrimitiveType:
                parsed_v = getattr(PrimitiveType, parsed_v).value
            setattr(self, attname, parsed_v)

    def to_dict(self):
        out = {}
        for attname in self.__annotations__:
            parsed_v = getattr(self, attname)
            if not parsed_v:
                continue
            if isinstance(parsed_v, CompleteId):
                parsed_v = parsed_v.serialized_id
            elif isinstance(parsed_v, type):
                parsed_v = parsed_v.__name__

            out[attname] = parsed_v
        return out

    @property
    def prime_id(self) -> str:
        return getattr(self, self._id_key)

    @property
    def val_id(self) -> CompleteId:
        return getattr(self, self._content_key)


@dataclass
class PrimitiveFeature(_FeatBase):
    _id_key = "name"

    name: str
    dtype: PrimitiveType
    nullable: bool = False
    description: Optional[str] = None

    @property
    def val_id(self) -> CompleteId:
        return CompleteId(None, None, self.dtype.__name__)


@dataclass
class ForeignKey(_FeatBase):
    _id_key = "prefix"
    _content_key = "table"

    prefix: str
    table: CompleteIdOf["Table"]
    description: Optional[str] = None


@dataclass
class CompositeFeature(_FeatBase):
    _id_key = "prefix"
    _content_key = "dtype"

    prefix: str
    dtype: CompleteIdOf["CompositeType"]
    description: Optional[str] = None


ANY_FEATURE_TYPE = Union[PrimitiveFeature, CompositeFeature, ForeignKey]
ALL_FEATURE_TYPES = ANY_FEATURE_TYPE.__args__
