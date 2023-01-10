import os
import re
import sys
from functools import partial
from pathlib import Path
from shutil import copy, copytree
from subprocess import PIPE, CalledProcessError, check_call, check_output
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import toml
import yaml
from flit.build import main
from structlog import get_logger

from .config_loading import get_full_auth
from .metadata.high_level import PROJ_KEYS
from .naming import (
    GIT_TOKEN_ENV_VAR,
    MAIN_MODULE_NAME,
    META_MODULE_NAME,
    PYV,
    REQUIREMENTS_FILE,
    VERSION_PREFIX,
    VERSION_SEPARATOR,
    VERSION_VAR_NAME,
    RegistryPaths,
    get_package_name,
    to_mod_name,
)
from .utils import gen_rmtree, git_run

if TYPE_CHECKING:
    from .config_loading import Config  # pragma: no cover


logger = get_logger(ctx="registry")


class Registry:
    def __init__(self, conf: "Config", reset=False) -> None:
        self.conf = conf
        self.name = conf.name
        self.paths = RegistryPaths(self.name, conf.version)
        self.posix = self.paths.dir.as_posix()
        if not self.paths.dir.exists() or reset:
            gen_rmtree(self.paths.dir)
            self.paths.dir.mkdir(parents=True)
            try:
                git_run(clone=(conf.registry, self.posix))
            except CalledProcessError:  # pragma: no cover
                git_run(clone=(_de_auth(conf.registry, True), self.posix))

            self.paths.ensure()
        self.requires = [
            get_package_name(p.name) + p.version for p in conf.imported_projects
        ]
        self._git_run = partial(git_run, wd=self.posix)

    def dump_info(self):
        meta_dic = self.get_info()
        self.paths.info_yaml.write_text(yaml.safe_dump(meta_dic))

    def get_info(self):
        remote_comm = ["git", "config", "--get", "remote.origin.url"]
        try:
            uri = check_output(remote_comm).decode("utf-8").strip()
        except CalledProcessError:
            uri = ""
            logger.info("can't get git remote.origin.url")
        # TODO: WET Project metadata params
        return {
            PROJ_KEYS.uri: _de_auth(uri),
            PROJ_KEYS.tags: self._get_tags(),
            PROJ_KEYS.cron: self.conf.cron,
        }

    def get_project_meta_base(self, project_name, version):
        ypath = self.paths.info_yaml_of(project_name, version)
        if ypath.exists():
            return yaml.safe_load(ypath.read_text())
        logger.warning(f"info for {project_name} {version} requested but not found")

    def full_build(self, global_conf=False):
        self.dump_info()
        get_full_auth().dump_dvc(local=not global_conf)
        if not self._is_released():
            self._package()
        if not self.requires:
            return
        self._install_no_server(self.requires)

    def update(self):
        self._git_run(pull=True)

    def publish(self):
        self.dump_info()
        msg = f"push {self.name}-{self.conf.version}"
        vc_paths = self.paths.publish_paths
        self._git_run(add=vc_paths, msg=msg, push=True, pull=True, check=True)

    def purge(self):
        gen_rmtree(self.posix)
        freeze_comm = [sys.executable, "-m", "pip", "freeze"]
        all_dz_projects = [*_command_out_w_prefix(freeze_comm, f"{META_MODULE_NAME}-")]
        if not all_dz_projects:
            return
        check_call([sys.executable, "-m", "pip", "uninstall", *all_dz_projects, "-y"])

    def _package(self):
        pack_paths = self._dump_meta()
        reqs_from_txt = REQUIREMENTS_FILE.read_text().strip().split("\n")
        mod_name = to_mod_name(self.name)
        proj_conf = {
            "project": {
                "name": get_package_name(self.name),
                "version": self.conf.version,
                "description": "zimmer project",
                "requires-python": PYV,
                "dependencies": self.requires + reqs_from_txt,
            },
            "tool": {"flit": {"module": {"name": f"{META_MODULE_NAME}.{mod_name}"}}},
        }
        pack_paths.toml_path.write_text(toml.dumps(proj_conf))
        ns = main(pack_paths.toml_path)
        copy(ns.sdist.file, self.paths.index_dir)
        copy(ns.wheel.file, self.paths.index_dir)
        return True

    def _install_no_server(self, packages: list):
        addr = self.paths.index_dir.as_posix()
        comm = [sys.executable, "-m", "pip", "install", f"--find-links={addr}"]
        extras = ["--no-cache", "--no-build-isolation"]
        backup_ind = ["--extra-index-url", "https://pypi.org/simple"]
        check_call(comm + backup_ind + extras + packages)  # TODO: , stdout=PIPE)

    def _dump_meta(self):
        pack_paths = PackPaths(self.name)
        copytree(MAIN_MODULE_NAME, pack_paths.project_meta)
        vstr = f'\n{VERSION_VAR_NAME} = "{self.conf.version}"'
        with (pack_paths.project_meta / "__init__.py").open("a") as fp:
            fp.write(vstr)
        return pack_paths

    def _get_tags(self):
        tagpref = VERSION_SEPARATOR.join([VERSION_PREFIX, self.conf.version])
        return [*_command_out_w_prefix(["git", "tag"], tagpref)]

    def _is_released(self):
        try:
            comm = ["git", "cat-file", "-e", f"origin/main:{self.paths.dist_gitpath}"]
            check_call(comm, cwd=self.posix, stdout=PIPE, stderr=PIPE)
            msg = f"can't package {self.name}-{self.conf.version} - already released"
            logger.warning(msg)
            return True
        except CalledProcessError:
            return False


def _de_auth(url, re_auth=False):
    r_out = re.compile(r"(.*)@(.*):(.*)\.git").findall(url)
    if not r_out:
        return url
    _, host, repo_id = r_out[0]
    if re_auth:
        base = "@".join(filter(None, [os.environ.get(GIT_TOKEN_ENV_VAR), host]))
    else:
        base = host
    return f"https://{base}/{repo_id}"


class PackPaths:
    def __init__(self, name) -> None:
        self.dir = Path(TemporaryDirectory().name)
        _meta_root = self.dir / META_MODULE_NAME
        self.toml_path = self.dir / "pyproject.toml"
        self.project_meta = _meta_root / to_mod_name(name)


def _command_out_w_prefix(comm, prefix):
    for tagbytes in check_output(comm).strip().split():
        tag = tagbytes.decode("utf-8").strip()
        if not tag.startswith(prefix):
            continue
        yield tag
