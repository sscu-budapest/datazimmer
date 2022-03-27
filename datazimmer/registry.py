import shutil
import sys
from contextlib import contextmanager
from shutil import copy, rmtree
from subprocess import CalledProcessError, Popen, check_call, check_output
from time import sleep
from typing import TYPE_CHECKING

import requests
import toml
import yaml
from flit.build import main
from requests.exceptions import ConnectionError
from structlog import get_logger

from .exceptions import ArtifactSetupException
from .metadata.datascript.from_bedrock import ScriptWriter
from .metadata.datascript.to_bedrock import DatascriptToBedrockConverter
from .naming import (
    META_MODULE_NAME,
    PYV,
    VERSION_PREFIX,
    VERSION_SEPARATOR,
    RegistryPaths,
)

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
            rmtree(self.paths.dir, ignore_errors=True)
            self.paths.dir.mkdir(parents=True)
            check_call(["git", "clone", conf.registry, self.posix])
            self.paths.ensure()
        self._port = 8087
        self.requires = [a.name + a.version for a in conf.imported_artifacts]

    def full_build(self):
        try:
            comm = ["git", "cat-file", "-e", f"origin/main:{self.paths.dist_gitpath}"]
            check_call(comm, cwd=self.posix)
            msg = f"can't package {self.name}-{self.conf.version} - already released"
            logger.warning(msg)
            # FIXME: check if already installed
            with self._index_server():
                self._install([self.name], upgrade=True)
            return
        except CalledProcessError:
            pass
        with self._index_server():
            self._install(self.requires)
            self._build_from_script()
            if self._package():
                self._install([self.name], upgrade=True)

    def update(self):
        self._git_run(pull=True)

    def publish(self):
        self._dump_meta()
        msg = f"push {self.name}-{self.conf.version}"
        self._git_run(add=self.paths.publish_paths, msg=msg, push=True)

    def purge(self):
        shutil.rmtree(self.posix, ignore_errors=True)

    def _build_from_script(self):
        proj_conf = {
            "project": {
                "name": self.name,
                "version": self.conf.version,
                "description": "zimmer artifact",
                "requires-python": PYV,
                "dependencies": self.requires,
            },
            "tool": {"flit": {"module": {"name": META_MODULE_NAME}}},
        }
        self.paths.toml_path.write_text(toml.dumps(proj_conf))
        self._dump_meta()

    def _package(self):
        msg = f"build-{self.name}-{self.conf.version}"
        try:
            self._git_run(add=self.paths.flit_posixes, msg=msg)
        except CalledProcessError:
            logger.warning("Tried building new package with no changes")
            return False
        ns = main(
            self.paths.toml_path,
            formats={"sdist"},
            # gen_setup_py=True,
        )
        copy(ns.sdist.file, self.paths.dist_dir)
        return True

    def _git_run(self, *, add=None, msg=None, pull=False, push=False):
        comm_ends = []
        if add:
            comm_ends.append(["add", *add])
        if msg:
            comm_ends.append(["commit", "-m", msg])
        if pull:
            comm_ends.append(["pull"])
        if push:
            comm_ends.append(["push"])

        for cmend in comm_ends:
            check_call(["git", *cmend], cwd=self.posix)

    @contextmanager
    def _index_server(self):
        index_root = self.paths.index_dir.as_posix()
        comm = ["twistd", "--pidfile=", "-n", "web"]
        opts = ["--path", index_root, "--listen", f"tcp:{self._port}"]
        server_popen = Popen(comm + opts)
        for attempt in range(40):
            sleep(0.01 * attempt)
            try:
                resp = requests.get(self._index_addr)
            except ConnectionError:
                logger.exception("failed index server")
                continue
            if resp.ok or (resp.status_code == 404):
                break
            logger.warning("bad response from index server", code=resp.status_code)
        else:
            server_popen.kill()
            raise ArtifactSetupException("can't start index server")
        logger.info("running index server", pid=server_popen.pid)
        try:
            yield
        finally:
            server_popen.kill()

    @property
    def _index_addr(self):
        return f"http://localhost:{self._port}"

    def _install(self, packages: list, upgrade=False):
        if not packages:
            return
        comm = [sys.executable, "-m", "pip", "install", "-i", self._index_addr]
        extras = ["--no-cache", "--no-build-isolation"]
        if upgrade:
            extras += ["--upgrade"]
        check_call(comm + extras + self._parse_package_names(packages))

    def _dump_meta(self):
        self.paths.meta_init_py.write_text("")
        vstr = f'__version__ = "{self.conf.version}"'
        self.paths.artifact_init_py.write_text(vstr)
        for ns in DatascriptToBedrockConverter(self.name).get_namespaces():
            ns_dir = self.paths.artifact_meta / ns.name
            ns.dump(ns_dir)
            ScriptWriter(ns, ns_dir / "__init__.py")

        remote_comm = ["git", "config", "--get", "remote.origin.url"]
        uri = check_output(remote_comm).decode("utf-8").strip()
        meta_dic = {"uri": uri, "tags": self._get_tags()}
        self.paths.info_yaml.write_text(yaml.safe_dump(meta_dic))

    def _get_tags(self):
        out = []
        tagpref = VERSION_SEPARATOR.join([VERSION_PREFIX, self.conf.version])
        for tagbytes in check_output(["git", "tag"]).strip().split():
            tag = tagbytes.decode("utf-8").strip()
            if not tag.startswith(tagpref):
                continue
            out.append(tag)
        return out

    def _parse_package_names(self, package_names):
        return package_names
