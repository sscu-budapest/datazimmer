from typing import List

from yaml import safe_dump

from ..naming import (
    DEFAULT_BRANCH_NAME,
    IMPORTED_NAMESPACES_PATH,
    NamespaceMetadataPaths,
)
from ..utils import cd_into, list_to_named_dict, load_named_dict_to_list
from .namespace_handling import get_used_ns_prefixes
from .schema import (
    CompositeType,
    EntityClass,
    ImportedNamespace,
    NamespaceMetadata,
    Table,
)


def load_from_yaml(subdir="", filter_namespaces=True) -> NamespaceMetadata:
    ns_paths = NamespaceMetadataPaths(subdir)

    metadata = NamespaceMetadata(
        imported_namespaces=load_named_dict_to_list(
            IMPORTED_NAMESPACES_PATH, ImportedNamespace, "prefix"
        ),
        composite_types=load_named_dict_to_list(
            ns_paths.composite_types, CompositeType
        ),
        entity_classes=load_named_dict_to_list(
            ns_paths.entity_classes, EntityClass
        ),
        tables=load_named_dict_to_list(ns_paths.table_schemas, Table),
    )
    if filter_namespaces:
        return _filter_to_used_imported_namespaces(metadata)
    return metadata


def load_imported_namespaces(subdir="") -> List[ImportedNamespace]:
    return load_from_yaml(subdir, False).imported_namespaces


def dump_to_yaml(
    ns: NamespaceMetadata, subdir="", skip_imported_namespaces=True
):
    ns_paths = NamespaceMetadataPaths(subdir)
    for path, elem_list in zip(
        [
            ns_paths.table_schemas,
            ns_paths.entity_classes,
            ns_paths.composite_types,
        ],
        [ns.tables, ns.entity_classes, ns.composite_types],
    ):
        path.write_text(
            safe_dump(list_to_named_dict(elem_list), sort_keys=False)
        )
    if not skip_imported_namespaces:
        # TODO: possibly expand
        dump_imported_namespaces(ns.imported_namespaces)


def dump_imported_namespaces(ns_list: List[ImportedNamespace], extend=False):
    ns_dict = list_to_named_dict(ns_list, key_name="prefix")
    if extend:
        ns_dict = {
            **list_to_named_dict(
                load_imported_namespaces(), key_name="prefix"
            ),
            **ns_dict,
        }

    IMPORTED_NAMESPACES_PATH.write_text(
        safe_dump(
            ns_dict,
            sort_keys=False,
        )
    )


def load_metadata_from_imported_ns(ns: ImportedNamespace):

    with cd_into(ns.uri_root, checkout=ns.tag or DEFAULT_BRANCH_NAME):
        return load_from_yaml(ns.uri_slug)


def _filter_to_used_imported_namespaces(metadata: NamespaceMetadata):
    used_prefixes = get_used_ns_prefixes(metadata)
    filtered_namespaces = []
    for ns in metadata.imported_namespaces:
        if ns.prefix in used_prefixes:
            filtered_namespaces.append(ns)
    metadata.imported_namespaces = filtered_namespaces

    return metadata
