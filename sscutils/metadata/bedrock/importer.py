from typing import TYPE_CHECKING

from ...naming import DEFAULT_BRANCH_NAME
from ...utils import cd_into
from .feature_types import CompositeFeature, ForeignKey
from .imported_namespace import ImportedNamespace
from .namespaced_id import NamespacedId

if TYPE_CHECKING:
    from .artifact_metadata import ArtifactMetadata  # pragma: no cover


class ImportedMetadata:
    def __init__(
        self, a_meta: "ArtifactMetadata", imported_ns: "ImportedNamespace"
    ) -> None:

        self.local_meta = a_meta
        self.imported_ns = imported_ns

        self.external_meta = self._get_ext_meta()
        self.external_ns_meta = self.external_meta.namespaces[
            imported_ns.uri_slug
        ]
        self.new_dependencies = []

        self._prefix_map = {}
        self._all_dependencies = self._get_all_external_dependencies()

        self._map_ns_prefixes()
        self.external_ns_meta.local_name = imported_ns.prefix

    def _get_ext_meta(self):
        with cd_into(
            self.imported_ns.uri_root,
            checkout=self.imported_ns.tag or DEFAULT_BRANCH_NAME,
        ):
            return self.local_meta.load_serialized()

    def _get_all_external_dependencies(self):

        for prefix in self.external_ns_meta.used_prefixes:
            imp_ns = self.external_meta.imported_dic.get(
                prefix,
                ImportedNamespace.from_uri_parts(
                    prefix,
                    self.imported_ns.uri_root,
                    self.imported_ns.uri_slug,
                    self.imported_ns.tag,
                ),
            )
            self._resolve_new_dependency(imp_ns)

    def _resolve_new_dependency(self, new_dep: ImportedNamespace):

        new_prefix = new_dep.prefix
        currently_under_prefix = self.local_meta.imported_dic.get(new_prefix)
        if currently_under_prefix is not None:
            if currently_under_prefix == new_dep:
                return
            # find if imported
            new_prefix = self._create_new_prefix_for_ns(new_prefix)

        for nsi_local in self.local_meta.imported_namespaces:
            if new_dep == nsi_local:
                self._prefix_map[new_dep.prefix] = nsi_local.prefix
                break
        else:
            self.new_dependencies.append(
                ImportedNamespace(new_prefix, new_dep.uri, new_dep.tag)
            )

    def _create_new_prefix_for_ns(self, old_prefix):
        n = 2
        while True:
            new_prefix = f"{old_prefix}_{n}"
            if new_prefix not in self.local_meta.imported_dic.keys():
                return new_prefix
            n += 1

    def _map_ns_prefixes(self):

        for table in self.external_ns_meta.tables:
            self._map_feat_list_prefix(table.index)
            self._map_feat_list_prefix(table.features)
            self._map_prefix(table.subject_of_records)

        for ct in self.external_ns_meta.composite_types:
            self._map_feat_list_prefix(ct.features)

        for ec in self.external_ns_meta.entity_classes:
            [self._map_prefix(parent) for parent in ec.parents]

    def _map_prefix(self, id_: NamespacedId):
        id_.ns_prefix = self._prefix_map.get(id_.ns_prefix, id_.ns_prefix)

    def _map_feat_list_prefix(self, feat_list):
        for feat in feat_list:
            if isinstance(feat, ForeignKey):
                self._map_prefix(feat.table)
            elif isinstance(feat, CompositeFeature):
                self._map_prefix(feat.dtype)
