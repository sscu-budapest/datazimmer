import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from functools import total_ordering
from typing import Dict, List

from yaml import safe_load

from ...module_tree import InstalledPaths
from ...naming import VERSION_SEPARATOR
from .atoms import NS_ATOM_TYPE
from .complete_id import CompleteId
from .namespace_metadata import NamespaceMetadata


@dataclass
class ArtifactMetadata:

    uri: str
    tags: List[str]
    namespaces: Dict[str, NamespaceMetadata]

    def get_atom(self, id_: CompleteId) -> NS_ATOM_TYPE:
        return self.namespaces[id_.namespace].get(id_.obj_id)

    def latest_tag_of(self, env):
        return sorted(self._tags_by_v.items())[-1][1][env]

    def get_used_artifacts(self):
        artids = set()
        for ns in self.namespaces.values():
            for table in ns.tables:
                _add_feats(table.features + table.index, artids)
            for ctype in ns.composite_types:
                _add_feats(ctype.features, artids)
            for enclass in ns.entity_classes:
                for parent in enclass.parents:
                    artids.add(parent.artifact)
        return filter(None, artids)

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

    @classmethod
    def load_installed(cls, name_to_load: str, name_loading_from: str = None):
        paths = InstalledPaths(name_to_load, name_loading_from or name_to_load)
        ns_dic = {d.name: NamespaceMetadata.load_serialized(d) for d in paths.ns_paths}
        return cls(**safe_load(paths.info_yaml.read_text()), namespaces=ns_dic)

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


def _add_feats(feats, a_set: set):
    for feat in feats:
        a_set.add(feat.val_id.artifact)
