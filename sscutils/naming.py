from pathlib import Path

COMPLETE_ENV_NAME = "complete"
DEFAULT_BRANCH_NAME = "main"

FEATURES_CLS_SUFFIX = "features"
INDEX_CLS_SUFFIX = "index"

PIPELINE_STEP_SEPARATOR = "|>"

CONFIG_DIR = Path("conf")
METADATA_DIR = Path("metadata")


class NamespaceMetadataPaths:
    def __init__(self, local_name: str = "", mkdir=False) -> None:
        _parent = METADATA_DIR / local_name
        if mkdir:
            _parent.mkdir(exist_ok=True)

        def _p(s) -> Path:
            return _parent / s

        self.composite_types = _p("composite-types.yaml")
        self.entity_classes = _p("entity-classes.yaml")
        self.table_schemas = _p("tables.yaml")


DATASET_METADATA_PATHS = NamespaceMetadataPaths()
IMPORTED_NAMESPACES_PATH = METADATA_DIR / "imported-namespaces.yaml"
DEFAULT_REMOTES_PATH = CONFIG_DIR / "default-remotes.yaml"


class DatasetConfigPaths:

    CREATED_ENVS = CONFIG_DIR / "created-envs.yaml"


class ProjectConfigPaths:

    CURRENT_ENV = CONFIG_DIR / "project-env.yaml"
    PARAMS = Path("params.yaml")


DATA_PATH = Path("data")
SRC_PATH = Path("src")
IMPORTED_NAMESPACES_MODULE_NAME = "imported_namespaces"
NAMESPACE_METADATA_MODULE_NAME = "namespace_metadata"
UPDATE_DATA_MODULE_NAME = "update_data"
UPDATE_DATA_FUNCTION_NAME = "update_data"
ENV_CREATION_MODULE_NAME = "create_environments"
ENV_CREATION_FUNCTION_NAME = "create_environments"

IMPORTED_NAMESPACES_SCRIPTS_PATH = SRC_PATH / IMPORTED_NAMESPACES_MODULE_NAME


def get_top_module_name(child_module_name: str):
    mod_pref = f"{SRC_PATH}."
    if not child_module_name.startswith(mod_pref):
        raise ValueError(
            f"Can't detect top module from module {child_module_name}"
        )

    return child_module_name.replace(mod_pref, "", 1).split(".")[0]
