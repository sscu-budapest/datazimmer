import os
import sys
from contextlib import contextmanager
from pathlib import Path
from subprocess import check_call

from cookiecutter.main import generate_files
from structlog import get_logger
from yaml import safe_load

from datazimmer.naming import BASE_CONF_PATH, MAIN_MODULE_NAME
from datazimmer.typer_commands import init
from datazimmer.utils import cd_into, gen_rmtree, git_run, package_root

logger = get_logger()

dogshow_root = package_root / "dogshow"
project_cc_root = dogshow_root / "projects"

_PROJECTS = ["dog-raw", "dog-show", "dograce", "dogsuccess", "dogcombine"]
_VERSIONS = {"dog-show": ["0.0", "0.1"], "dogsuccess": ["1.0"]}
_RAW_IMPORTS = {"dog-show": ["dog-raw"], "dograce": ["dog-raw"]}
_PRIVATE_ZEN = {"dogsuccess": "sex_matches"}


class DogshowContextCreator:
    def __init__(self, local_root, remote_root=None, dvc_remotes=None):
        self.local_root = Path(local_root)
        gen_rmtree(self.local_root)
        self.local_root.mkdir()
        self.ran_dirs = []
        self.remote_root = Path(remote_root or self.local_root / "remotes")
        self.dvc_remotes = [*self._get_dvc_remotes(dvc_remotes)]
        _reg = self.remote_root / "dogshow-registry"
        self.cc_context = {
            "test_registry": self._init_if_local(_reg, True),
            "remote2": self.dvc_remotes[1][0],
        }
        self.all_contexts = map(self.project_ctx, _PROJECTS)

    @contextmanager
    def project_ctx(self, name: str):
        git_remote = self._init_if_local(self.remote_root / f"dogshow-{name}")
        with cd_into(self.local_root):
            init(name, git_remote=git_remote)
        root_dir = self.local_root / name
        template_path = project_cc_root / f"cc-{name}"
        generate_files(
            template_path,
            {"cookiecutter": {"project": name}, **self.cc_context},
            self.local_root,
            overwrite_if_exists=True,
        )
        self.ran_dirs.append(root_dir)
        with cd_into(root_dir):
            sys.path.insert(0, Path.cwd().as_posix())
            for remote_name, remote_id in self.dvc_remotes:
                check_call(["dvc", "remote", "add", remote_name, remote_id])
            check_call(["dvc", "remote", "default", self.dvc_remotes[0][0]])
            git_run(add=["*"], msg=f"setup {name} project", push=True)
            pzen = _PRIVATE_ZEN.get(name, "")
            yield name, _VERSIONS.get(name, []), _RAW_IMPORTS.get(name, []), pzen
            sys.path.pop(0)

    def check_sdists(self):
        pass  # TODO check builds

    @classmethod
    def load(cls, mode: str, tmp_path: Path):
        if mode == "test":
            kwargs = {"local_root": tmp_path}
        else:  # pragma: no cover
            kwargs = safe_load((dogshow_root / "confs-live.yaml").read_text())[mode]
        return cls(**kwargs)

    def _get_dvc_remotes(self, remotes):
        if remotes:  # pragma: no cover
            for r in remotes:
                yield r, f"s3://{r}"
            return
        for i in range(2):
            dvc_dir = self.local_root / "dvc-remotes" / f"remote-{i+1}"
            dvc_dir.mkdir(parents=True)
            yield f"dog-remote{i}", dvc_dir.absolute().as_posix()

    def _init_if_local(self, repo_path, touch=False):
        if not str(repo_path).startswith("git@"):
            repo_path.mkdir(parents=True, exist_ok=True)
            check_call(["git", "init"], cwd=repo_path)
            if touch:
                (repo_path / "T").write_text("touch")
                git_run(add=["*"], msg="touch", wd=repo_path)
        return str(repo_path)


def modify_to_version(version: str):
    # TODO
    # add one output/dependency for something + delete one of either"
    del_str = f"# remove in v{version}"
    add_str = f"# add in v{version}: "
    for path in _get_paths():
        lines = []
        for old_line in path.read_text().split("\n"):
            if old_line.startswith(add_str):
                new_line = old_line.replace(add_str, "")
            elif old_line.endswith(del_str):
                continue
            elif (path == BASE_CONF_PATH) and old_line.startswith("version: "):
                new_line = f"version: v{version}"
            else:
                new_line = old_line
            lines.append(new_line)
        path.write_text("\n".join(lines))
    git_run(add=[MAIN_MODULE_NAME, BASE_CONF_PATH], msg=f"v{version}", check=True)


def _get_paths():
    yield BASE_CONF_PATH
    for root, _, files in os.walk(Path(MAIN_MODULE_NAME), topdown=False):
        for name in files:
            sfile = Path(root, name)
            if "__pycache__" in sfile.as_posix():
                continue
            yield sfile
