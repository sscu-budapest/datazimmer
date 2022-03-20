from dataclasses import dataclass
from pathlib import Path
from typing import List

from ...naming import SDistPaths
from .atoms import NS_ATOM_TYPE, CompositeType, EntityClass, Table
from .load_util import dump_atom_list_to_dict, load_atom_dict_to_list


@dataclass
class NamespaceMetadata:
    """full spec of metadata for a namespace"""

    composite_types: List[CompositeType]
    entity_classes: List[EntityClass]
    tables: List[Table]
    name: str

    def get(self, obj_id: str) -> NS_ATOM_TYPE:
        for atom in self.atoms:
            if atom.name == obj_id:
                return atom
        raise KeyError(f"{obj_id} not found in: \n {self}")

    @classmethod
    def load_serialized(cls, subdir: Path) -> "NamespaceMetadata":
        ns_paths = SDistPaths(subdir)
        return cls(
            composite_types=load_atom_dict_to_list(
                ns_paths.composite_types, CompositeType
            ),
            entity_classes=load_atom_dict_to_list(ns_paths.entity_classes, EntityClass),
            tables=load_atom_dict_to_list(ns_paths.table_schemas, Table),
            name=subdir.name,
        )

    def dump(self, path):
        ns_paths = SDistPaths(path)
        for path, atom_list in [
            (ns_paths.table_schemas, self.tables),
            (ns_paths.entity_classes, self.entity_classes),
            (ns_paths.composite_types, self.composite_types),
        ]:
            dump_atom_list_to_dict(path, atom_list)

    @property
    def atoms(self) -> List[NS_ATOM_TYPE]:
        return [*self.composite_types, *self.entity_classes, *self.tables]
