import os
import re
from pathlib import Path
from typing import Iterable, Optional

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

GIT_TOKEN_ENV_VAR = "GIT_HTTPS_TOKEN"
AUTH_HEX_ENV_VAR = "ZIMMER_AUTH_HEX"
AUTH_PASS_ENV_VAR = "ZIMMER_PHRASE"

VERSION_VAR_NAME = "__version__"


repo_link = "https://github.com/sscu-budapest/{}".format


TEMPLATE_REPO = os.environ.get("ZIMMER_TEMPLATE", repo_link("project-template"))
DEFAULT_REGISTRY = "git@github.com:sscu-budapest/main-registry.git"
CONSTR = os.environ.get("ZIMMER_CONSTR", "sqlite:///:memory:")


class RegistryPaths:
    def __init__(self, name: str, version: str) -> None:
        self.dir = REGISTRY_ROOT_DIR / name
        self._info_dir = self.dir / "info"

        package_name = get_package_name(name)

        self.index_dir = self.dir / "index"
        self.info_yaml = self.info_yaml_of(name, version)
        self.publish_paths = self._relpos([self.index_dir, self.info_yaml])
        self.dist_gitpath = f"index/{package_name}-{version}.tar.gz"

    def info_yaml_of(self, name, version: Optional[str] = None) -> Path:
        if version:
            return self._info_dir / f"{name}-{version}.yaml"
        return self.get_latest_yaml_path(name)

    def get_latest_yaml_path(self, name):
        def _get_v(fp: Path):
            vstr = re.findall(rf"{name}-([\.|\d]+)\.yaml", fp.name)[0]
            return tuple(map(int, vstr.split(".")))

        return sorted(self._info_dir.glob(f"{name}-*.yaml"), key=_get_v)[-1]

    def ensure(self):
        for d in [self.index_dir, self._info_dir]:
            d.mkdir(exist_ok=True, parents=True)

    def _relpos(self, dirlist: Iterable[Path]):
        return [d.relative_to(self.dir).as_posix() for d in dirlist]


def get_data_path(project_name, namespace, env_name) -> Path:
    return DATA_PATH / project_name / namespace / env_name


def get_package_name(project_name):
    return f"{META_MODULE_NAME}-{project_name}"


def get_stage_name(ns, write_env):
    return f"{write_env}-{ns}"


def cli_run(*funs):
    return " && ".join(f"{CLI} {_get_fun_name(f)}" for f in funs)


def to_mod_name(name: str):
    return name.replace("-", "_")


def from_mod_name(name: str):
    return name.replace("_", "-")


def _get_fun_name(fun):
    if isinstance(fun, str):
        return fun
    elif isinstance(fun, tuple):
        return " ".join(map(_get_fun_name, fun))
    return fun.__name__.replace("_", "-")
