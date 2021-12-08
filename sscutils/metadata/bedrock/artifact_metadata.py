from dataclasses import asdict, dataclass
from typing import Dict, List

from yaml import safe_dump

from ...helpers import get_serialized_namespace_dirs
from ...naming import IMPORTED_NAMESPACES_PATH, ROOT_NS_LOCAL_NAME
from ...utils import get_dict_factory, load_named_dict_to_list
from .atoms import NS_ATOM_TYPE
from .imported_namespace import ImportedNamespace
from .importer import ImportedMetadata
from .namespace_metadata import NamespaceMetadata
from .namespaced_id import NamespacedId

_NS_KEY = "prefix"


@dataclass
class ArtifactMetadata:

    imported_namespaces: List[ImportedNamespace]
    namespaces: Dict[str, NamespaceMetadata]

    def get_atom(self, ns_id: NamespacedId) -> NS_ATOM_TYPE:
        return self.namespaces[ns_id.ns_prefix or ROOT_NS_LOCAL_NAME].get(
            ns_id.obj_id
        )

    def dump(self):
        serialized_str = safe_dump(
            {
                k: asdict(v, dict_factory=get_dict_factory(_NS_KEY))
                for k, v in self.imported_dic.items()
            },
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
        self.add_ns(imp_meta.external_ns_meta)
        for new_dep in imp_meta.new_dependencies:
            self.extend_from_import(new_dep)

    def add_ns(self, ns_meta: NamespaceMetadata):
        self.namespaces[ns_meta.local_name] = ns_meta

    @classmethod
    def load_serialized(cls) -> "ArtifactMetadata":
        imp_nss = load_named_dict_to_list(
            IMPORTED_NAMESPACES_PATH, ImportedNamespace, key_name=_NS_KEY
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

    @property
    def root_ns(self) -> NamespaceMetadata:
        return self.namespaces[ROOT_NS_LOCAL_NAME]
