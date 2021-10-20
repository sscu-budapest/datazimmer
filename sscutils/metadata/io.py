from dataclasses import asdict
from pathlib import Path
from typing import List, Type, TypeVar, Union

from yaml import safe_dump, safe_load

from sscutils.metadata.namespace_handling import get_used_ns_prefixes

from ..naming import (
    DEFAULT_BRANCH_NAME,
    IMPORTED_NAMESPACES_PATH,
    PIPELINE_STEP_SEPARATOR,
    NamespaceMetadataPaths,
)
from ..utils import cd_into
from .schema import (
    CompositeType,
    EntityClass,
    ImportedNamespace,
    NamespaceMetadata,
    Table,
)

T = TypeVar("T")
NAMED_DICT_TYPE = Union[CompositeType, EntityClass, Table]


def load_from_yaml(subdir="", filter_namespaces=True) -> NamespaceMetadata:
    ns_paths = NamespaceMetadataPaths(subdir)

    metadata = NamespaceMetadata(
        imported_namespaces=_load_named_dict_to_list(
            IMPORTED_NAMESPACES_PATH, ImportedNamespace, "prefix"
        ),
        composite_types=_load_named_dict_to_list(
            ns_paths.composite_types, CompositeType
        ),
        entity_classes=_load_named_dict_to_list(
            ns_paths.entity_classes, EntityClass
        ),
        tables=_load_named_dict_to_list(ns_paths.table_schemas, Table),
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
            safe_dump(_list_to_named_dict(elem_list), sort_keys=False)
        )
    if not skip_imported_namespaces:
        # TODO: possibly expand
        dump_imported_namespaces(ns.imported_namespaces)


def dump_imported_namespaces(ns_list: List[ImportedNamespace], extend=False):
    ns_dict = _list_to_named_dict(ns_list, key_name="prefix")
    if extend:
        ns_dict = {
            **_list_to_named_dict(
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
    splitted_id = ns.uri.split(PIPELINE_STEP_SEPARATOR)
    ns_path = splitted_id[0]
    if len(splitted_id) > 1:
        found_subdir = splitted_id[1]
    else:
        found_subdir = ""

    with cd_into(ns_path, checkout=ns.tag or DEFAULT_BRANCH_NAME):
        return load_from_yaml(found_subdir)


def _load_named_dict_to_list(
    path: Path, cls: Type[T], key_name="name", val_name=None
) -> List[T]:
    out = []
    for k, v in (safe_load(path.read_text()) or {}).items():
        kwargs = {val_name: v} if val_name else v
        out.append(cls(**{key_name: k, **kwargs}))
    return out


def _list_to_named_dict(
    in_list: List[NAMED_DICT_TYPE], key_name="name", val_name=None
):
    out = {}
    for elem in in_list:
        d = asdict(elem, dict_factory=_none_drop_dict_factory)
        name = d.pop(key_name)
        out[name] = d[val_name] if val_name else d
    return out


def _none_drop_dict_factory(items):
    return {k: v for k, v in items if v is not None}


def _filter_to_used_imported_namespaces(metadata: NamespaceMetadata):
    used_prefixes = get_used_ns_prefixes(metadata)
    filtered_namespaces = []
    for ns in metadata.imported_namespaces:
        if ns.prefix in used_prefixes:
            filtered_namespaces.append(ns)
    metadata.imported_namespaces = filtered_namespaces

    return metadata
