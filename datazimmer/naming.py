import os
from pathlib import Path

from colassigner.constants import PREFIX_SEP  # noqa: F401

PYV = ">=3.8"
DEFAULT_ENV_NAME = "complete"

VERSION_PREFIX = "zimmer-v0"
VERSION_SEPARATOR = "/"

RUN_CONF_PATH = Path("__run_conf.yaml")
BASE_CONF_PATH = Path("zimmer.yaml")
EXPLORE_CONF_PATH = Path("dec.yaml")
REQUIREMENTS_FILE = Path("requirements.txt")

DATA_PATH = Path("data")
PROFILES_PATH = Path("run-profiles")
REGISTRY_ROOT_DIR = Path.home() / "zimmer-registries"
SANDBOX_DIR = Path.home() / "zimmer-sandbox"
SANDBOX_NAME = "zimmersandboxproject"
MAIN_MODULE_NAME = "src"
META_MODULE_NAME = "metazimmer"
PACKAGE_NAME = "datazimmer"
CLI = "dz"

CRON_ENV_VAR = "CRON_TRIGGER"
GIT_TOKEN_ENV_VAR = "GIT_HTTPS_TOKEN"
AUTH_ENV_VAR = "ZIMMER_FULL_AUTH"

VERSION_VAR_NAME = "__version__"


def repo_link(slug):
    return f"https://github.com/sscu-budapest/{slug}"


TEMPLATE_REPO = os.environ.get("ZIMMER_TEMPLATE", repo_link("project-template"))
DEFAULT_REGISTRY = "git@github.com:sscu-budapest/main-registry.git"
CONSTR = os.environ.get("ZIMMER_CONSTR", "sqlite:///:memory:")


class RegistryPaths:
    def __init__(self, name: str, version: str) -> None:
        self.dir = REGISTRY_ROOT_DIR / name
        self._info_dir = self.dir / "info"

        package_name = get_package_name(name)

        self.index_dir = self.dir / "index"
        self.dist_dir = self.index_dir / package_name
        self.info_yaml = self.info_yaml_of(name, version)
        self.publish_paths = self._relpos([self.dist_dir, self.info_yaml])
        self.dist_gitpath = f"index/{package_name}/{package_name}-{version}.tar.gz"

    def info_yaml_of(self, name, version) -> Path:
        return self._info_dir / f"{name}-{version}.yaml"

    def ensure(self):
        for d in [self.dist_dir, self._info_dir]:
            d.mkdir(exist_ok=True, parents=True)

    def _relpos(self, dirlist):
        return [d.relative_to(self.dir).as_posix() for d in dirlist]


def get_data_path(project_name, namespace, env_name):
    return DATA_PATH / project_name / namespace / env_name


def get_package_name(project_name):
    return f"{META_MODULE_NAME}-{project_name}"
