import os
from pathlib import Path

COMPLETE_ENV_NAME = "complete"
DEFAULT_BRANCH_NAME = "main"

FEATURES_CLS_SUFFIX = "features"
INDEX_CLS_SUFFIX = "index"

PIPELINE_STEP_SEPARATOR = "|>"
NAMESPACE_PREFIX_SEPARATOR = ":"

CONFIG_DIR = Path("conf")
METADATA_DIR = Path("metadata")

RUN_CONF_PATH = CONFIG_DIR / "_run_conf.yaml"

ROOT_NS_LOCAL_NAME = ""
TMP_CLS_MODULE = "__tmp_cls_module__"


class NamespaceMetadataPaths:
    def __init__(
        self, local_name: str = ROOT_NS_LOCAL_NAME, mkdir=False
    ) -> None:
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
PROFILES_PATH = Path("run-profiles")
SRC_PATH = Path("src")
IMPORTED_NAMESPACES_MODULE_NAME = "imported_namespaces"
NAMESPACE_METADATA_MODULE_NAME = "namespace_metadata"
UPDATE_DATA_MODULE_NAME = "update_data"
UPDATE_DATA_FUNCTION_NAME = "update_data"
ENV_CREATION_MODULE_NAME = "create_environments"
ENV_CREATION_FUNCTION_NAME = "create_environments"
PIPEREG_MODULE_NAME = "pipereg"
PIPEREG_INSTANCE_NAME = "pipereg"


IMPORTED_NAMESPACES_SCRIPTS_PATH = SRC_PATH / IMPORTED_NAMESPACES_MODULE_NAME

imported_namespaces_abs_module = (
    f"{SRC_PATH}.{IMPORTED_NAMESPACES_MODULE_NAME}"
)
ns_metadata_abs_module = f"{SRC_PATH}.{NAMESPACE_METADATA_MODULE_NAME}"
ns_metadata_file = SRC_PATH / (NAMESPACE_METADATA_MODULE_NAME + ".py")

_tfstr = "https://github.com/sscu-budapest/{}-template"

dataset_template_repo = os.environ.get(
    "SSC_LOCAL_DS_TEMPLATE", _tfstr.format("dataset")
)
project_template_repo = os.environ.get(
    "SSC_LOCAL_PROJECT_TEMPLATE", _tfstr.format("project")
)
