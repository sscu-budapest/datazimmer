import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field
from functools import total_ordering
from itertools import chain

from structlog import get_logger

from ..aswan_integration import DzAswan
from ..config_loading import KeyMeta
from ..naming import VERSION_SEPARATOR
from ..pipeline_element import PipelineElement
from .atoms import CompositeType, EntityClass
from .datascript import AbstractEntity, CompositeTypeBase, SourceUrl
from .scrutable import ScruTable

logger = get_logger()


@dataclass
class NamespaceMetadata:
    """full spec of metadata for a namespace"""

    name: str
    composite_types: list[CompositeType] = field(default_factory=list)
    entity_classes: list[EntityClass] = field(default_factory=list)
    tables: list[ScruTable] = field(default_factory=list)
    source_urls: list[SourceUrl] = field(default_factory=list)
    pipeline_elements: list[PipelineElement] = field(default_factory=list)
    aswan_projects: list[type[DzAswan]] = field(default_factory=list)

    def get_table_of_ec(self, ec: EntityClass) -> ScruTable:
        for table in self.tables:
            if table.entity_class == ec:
                return table

    def add_obj(self, obj):
        for cls, l in [
            (ScruTable, self.tables),
            (SourceUrl, self.source_urls),
            (PipelineElement, self.pipeline_elements),
        ]:
            if isinstance(obj, cls):
                l.append(obj)
        if not isinstance(obj, type):
            return
        for cls, l, parser in [
            (CompositeTypeBase, self.composite_types, CompositeType.from_cls),
            (AbstractEntity, self.entity_classes, EntityClass.from_cls),
            (DzAswan, self.aswan_projects, lambda x: x),
        ]:
            if cls in obj.mro()[1:]:
                l.append(parser(obj))


class NsCollection(NamespaceMetadata):
    def __init__(self, project: "ProjectMetadata"):
        self._p = project

    def __getattribute__(self, att: str):
        project: ProjectMetadata = super().__getattribute__("_p")
        return chain(*[getattr(ns, att) for ns in project.namespaces.values()])


@dataclass
class ProjectMetadata:

    uri: str
    tags: list[str]
    cron: str = ""
    namespaces: dict[str, NamespaceMetadata] = field(default_factory=dict)
    complete: NsCollection = field(init=False, repr=False)
    # TODO maybe elminate / rename namespace to module

    def __post_init__(self):
        self.complete = NsCollection(self)

    def table_of_ec(self, ec: EntityClass) -> ScruTable:
        for ns in self.namespaces.values():
            _tab = ns.get_table_of_ec(ec)
            if _tab:
                return _tab

    def latest_tag_of(self, env):
        try:
            return sorted(self._tags_by_v.items())[-1][1][env]
        except IndexError:
            logger.warning(f"no tagged data release for {self.uri}")

    @property
    def next_data_v(self):
        try:
            dv = sorted(self._tags_by_v.keys())[-1].bump()
        except IndexError:
            dv = DataVersion.new_today()
        return dv.to_str()

    @property
    def data_namespaces(self):
        return [ns.name for ns in self.namespaces.values() if ns.tables]

    @property
    def _tags_by_v(self) -> dict["DataVersion", dict[str, str]]:
        dv_tags = defaultdict(dict)
        for tag in self.tags:
            data_v, env = tag.split(VERSION_SEPARATOR)[2:]
            dv_tags[DataVersion.from_str(data_v)][env] = tag
        return dv_tags


@dataclass
@total_ordering
class DataVersion:
    year: int
    month: int
    day: int
    num: int

    def __hash__(self) -> int:
        return self._args.__hash__()

    def __eq__(self, o: "DataVersion") -> bool:
        return self._args == o._args

    def __gt__(self, o: "DataVersion") -> bool:
        return self._args > o._args

    def to_str(self):
        return ".".join(map(str, self._args))

    def bump(self):
        today = dt.date.today()
        targs = (today.year, today.month, today.day)
        n = 1
        if targs == self._args[:-1]:
            n += self.num
        return DataVersion(*targs, n)

    @classmethod
    def from_str(cls, s: str):
        return cls(*map(int, s.split(".")))

    @classmethod
    def new_today(cls):
        td = dt.date.today()
        return cls(td.year, td.month, td.day, 1)

    @property
    def _args(self):
        return (self.year, self.month, self.day, self.num)


class PROJ_KEYS(ProjectMetadata, metaclass=KeyMeta):
    pass
