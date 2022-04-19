import os
from functools import partial
from pathlib import Path

PYV = ">=3.8"
DEFAULT_ENV_NAME = "complete"

FEATURES_CLS_SUFFIX = "features"
INDEX_CLS_SUFFIX = "index"

VERSION_PREFIX = "zimmer-v0"
VERSION_SEPARATOR = "/"
NS_ID_SEPARATOR = ":"


RUN_CONF_PATH = Path("__run_conf.yaml")
BASE_CONF_PATH = Path("zimmer.yaml")
EXPLORE_CONF_PATH = Path("dec.yaml")

DATA_PATH = Path("data")
PROFILES_PATH = Path("run-profiles")
REGISTRY_ROOT_DIR = Path.home() / "zimmer-registries"
SANDBOX_DIR = Path.home() / "zimmer-sandbox"
SANDBOX_NAME = "zimmersandboxproject"
MAIN_MODULE_NAME = "src"
META_MODULE_NAME = "metazimmer"
PACKAGE_SHORTHAND = "dz"
CLI = "datazimmer"

CRON_ENV_VAR = "CRON_TRIGGER"
GIT_TOKEN_ENV_VAR = "GIT_HTTPS_TOKEN"
AUTH_ENV_VAR = "ZIMMER_FULL_AUTH"


def repo_link(slug):
    return f"https://github.com/sscu-budapest/{slug}"


TEMPLATE_REPO = os.environ.get("ZIMMER_TEMPLATE", repo_link("project-template"))
DEFAULT_REGISTRY = "git@github.com:sscu-budapest/main-registry.git"
CONSTR = os.environ.get("ZIMMER_CONSTR", "sqlite:///:memory:")


class SDistPaths:
    def __init__(self, parent: Path) -> None:
        parent.mkdir(exist_ok=True, parents=True)
        _p = partial(Path, parent)
        self.meta = _p("meta.yaml")
        self.composite_types = _p("composite-types.yaml")
        self.entity_classes = _p("entity-classes.yaml")
        self.table_schemas = _p("tables.yaml")
        self.datascript = _p("__init__.py")


class RegistryPaths:
    def __init__(self, name: str, version: str) -> None:
        self.dir = REGISTRY_ROOT_DIR / name
        _dev_dir = self.dir / "dev" / name
        _meta_root = _dev_dir / META_MODULE_NAME
        self._info_dir = self.dir / "info"

        self.index_dir = self.dir / "index"
        self.toml_path = _dev_dir / "pyproject.toml"
        self.meta_init_py = _meta_root / "__init__.py"
        self.project_meta = _meta_root / name
        self.project_init_py = self.project_meta / "__init__.py"
        self.dist_dir = self.index_dir / name
        self.info_yaml = self.info_yaml_of(name, version)

        self.flit_posixes = self._relpos([_meta_root, self.toml_path])
        self.publish_paths = self._relpos([self.dist_dir, self.info_yaml])
        self.dist_gitpath = f"index/{name}/{name}-{version}.tar.gz"

    def info_yaml_of(self, name, version):
        return self._info_dir / f"{name}-{version}.yaml"

    def ensure(self):
        for d in [self.dist_dir, self.project_meta, self._info_dir]:
            d.mkdir(exist_ok=True, parents=True)

    def _relpos(self, dirlist):
        return [d.relative_to(self.dir).as_posix() for d in dirlist]


def get_data_path(project_name, namespace, env_name):
    return DATA_PATH / project_name / namespace / env_name
