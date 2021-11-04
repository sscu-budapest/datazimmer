from dataclasses import asdict, dataclass
from typing import Dict, List

from yaml import safe_dump

from ...helpers import get_all_child_modules, get_serialized_namespace_dirs
from ...naming import IMPORTED_NAMESPACES_PATH
from ...utils import load_named_dict_to_list
from ..datascript.from_bedrock import ns_metadata_to_script
from ..datascript.to_bedrock import load_metadata_from_child_module
from .atoms import NS_ATOM_TYPE
from .imported_namespace import ImportedNamespace
from .importer import ImportedMetadata
from .namespace_metadata import NamespaceMetadata
from .namespaced_id import NamespacedId


@dataclass
class ArtifactMetadata:

    imported_namespaces: List[ImportedNamespace]
    namespaces: Dict[str, NamespaceMetadata]

    def get_atom(self, ns_id: NamespacedId) -> NS_ATOM_TYPE:
        return self.namespaces[ns_id.ns_prefix or ""].get(ns_id.obj_id)

    def dump(self):
        serialized_str = safe_dump(
            {k: asdict(v) for k, v in self.imported_dic.items()},
            sort_keys=False,
        )
        IMPORTED_NAMESPACES_PATH.write_text(serialized_str)
        for ns in self.namespaces.values():
            ns.dump()

    def extend_from_import(self, nsi: ImportedNamespace, overwrite=True):
        if nsi.prefix in self.namespaces.keys():
            if overwrite:
                self.namespaces.pop(nsi.prefix)
            else:
                return
        imp_meta = ImportedMetadata(self, nsi)
        self._add(imp_meta.external_ns_meta)
        for new_dep in imp_meta.new_dependencies:
            self.extend_from_import(new_dep)

    def extend_from_datascript(self):
        for child_module in get_all_child_modules():
            self._add(load_metadata_from_child_module(child_module))

    def imported_nss_to_datascript(self):
        for nsi in self.imported_namespaces:
            ns_metadata_to_script(self.namespaces[nsi.prefix])

    @classmethod
    def load_serialized(cls) -> "ArtifactMetadata":
        imp_nss = load_named_dict_to_list(
            IMPORTED_NAMESPACES_PATH, ImportedNamespace, key_name="prefix"
        )

        return cls(
            imp_nss,
            {
                sd: NamespaceMetadata.load_serialized(sd)
                for sd in get_serialized_namespace_dirs()
            },
        )

    @property
    def imported_dic(self) -> Dict[str, ImportedNamespace]:
        return {ns.prefix: ns for ns in self.imported_namespaces}

    def _add(self, ns_meta: NamespaceMetadata):
        self.namespaces[ns_meta.local_name] = ns_meta
