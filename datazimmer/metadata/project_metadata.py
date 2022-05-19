import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Dict, List

from structlog import get_logger

from ..naming import VERSION_SEPARATOR
from .atoms import EntityClass
from .namespace_metadata import NamespaceMetadata
from .scrutable import ScruTable

logger = get_logger()


@dataclass
class ProjectMetadata:

    uri: str
    tags: List[str]
    namespaces: Dict[str, NamespaceMetadata] = field(default_factory=dict)

    def table_of_ec(self, ec: EntityClass) -> ScruTable:
        for ns in self.namespaces.values():
            _tab = ns.get_table_of_ec(ec)
            if _tab:
                return _tab

    def latest_tag_of(self, env):
        try:
            return sorted(self._tags_by_v.items())[-1][1][env]
        except IndexError:
            logger.warning(f"no tagged data release for {self}")

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
    def _tags_by_v(self) -> Dict["DataVersion", Dict[str, str]]:
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
