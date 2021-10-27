from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from ..naming import PIPELINE_STEP_SEPARATOR


class _ParseFeats:
    def __post_init__(self):
        _hint = List[FEATURE_TYPE]
        feat_types = FEATURE_TYPE.__args__
        for k, v in self.__annotations__.items():
            if not ((v == Optional[_hint]) or (v == _hint)):
                continue
            att = getattr(self, k)
            if att is None:
                continue
            new_atts = []
            for elem in att:
                if type(elem) in feat_types:
                    new_atts.append(elem)
                    continue
                for cls in feat_types:
                    try:
                        new_atts.append(cls(**elem))
                        break
                    except TypeError:
                        pass
                else:
                    raise TypeError(
                        f"cant parse {elem} to any feature type {feat_types}"
                    )
            setattr(self, k, new_atts)


class PrimitiveType(Enum):
    # TODO: categorical
    float = float
    int = int
    str = str
    bytes = bytes
    bool = bool
    datetime = datetime


@dataclass
class ImportedNamespace:
    prefix: str
    uri: str
    tag: Optional[str] = None

    def __eq__(self, o: "ImportedNamespace") -> bool:
        return (self.uri == o.uri) and (self.tag == o.tag)

    @property
    def uri_slug(self):
        return self._splitret(1)

    @property
    def uri_root(self):
        return self._splitret(0)

    def _splitret(self, i):
        splitted_id = self.uri.split(PIPELINE_STEP_SEPARATOR)
        try:
            return splitted_id[i]
        except IndexError:
            return ""


@dataclass
class PrimitiveFeature:
    name: str
    dtype: PrimitiveType
    description: Optional[str] = None


@dataclass
class ForeignKey:
    prefix: str
    table: str  # an ID, that needs to link to a table
    description: Optional[str] = None


@dataclass
class CompositeFeature:
    prefix: str
    dtype: str  # an ID, that needs to link to a composite type
    description: Optional[str] = None


FEATURE_TYPE = Union[PrimitiveFeature, CompositeFeature, ForeignKey]


@dataclass
class CompositeType(_ParseFeats):
    name: str  # turns to id with namespace
    features: List[FEATURE_TYPE]
    description: Optional[str] = None


@dataclass
class EntityClass:
    name: str  # turns to id with namespace
    parents: Optional[List[str]] = None  # ids of other entity classes
    description: Optional[str] = None


@dataclass
class Table(_ParseFeats):
    """TODO: document restrictions here

    or find new location for them

    e. g.
    - no composite features with same prefix in same table
    - composite type needs to be found
    - foreign keys referencing tables need to be found
    - column name cant contain __ (and other)
    - ...
    """

    name: str  # turns to id with namespace
    features: List[FEATURE_TYPE]
    subject_of_records: str  # id of entity class
    index: Optional[List[FEATURE_TYPE]] = None
    partitioning_cols: Optional[List[str]] = None  # TODO: not sure
    partition_max_rows: Optional[int] = None
    description: Optional[str] = None


@dataclass
class NamespaceMetadata:
    """full spec of metadata for a namespace

    a dataset stores these in yaml files in the same place

    a project stores the imported namespaces on the project level
    imported namespaces for created namespaces are inferred
    based on what is used in other namespace metadata
    """

    imported_namespaces: List[ImportedNamespace]
    composite_types: List[CompositeType]
    entity_classes: List[EntityClass]
    tables: List[Table]

    @property
    def composite_type_dict(self):
        return
