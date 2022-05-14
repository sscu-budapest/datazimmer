import json
import os
from contextlib import contextmanager
from pathlib import Path
from subprocess import check_call

from cookiecutter.main import generate_files
from jinja2 import Template
from structlog import get_logger
from yaml import safe_load

from datazimmer.naming import (
    BASE_CONF_PATH,
    EXPLORE_CONF_PATH,
    MAIN_MODULE_NAME,
    TEMPLATE_REPO,
)
from datazimmer.utils import cd_into, gen_rmtree, git_run, package_root

logger = get_logger()

dogshow_root = package_root / "dogshow"
project_cc_root = dogshow_root / "projects"
dec_src_root = dogshow_root / "explorer"

_PROJECTS = ["dogshowbase", "dogracebase", "dogsuccess", "dogcombine"]
_VERSIONS = {"dogshowbase": ["0.0", "0.1"], "dogsuccess": ["1.0"]}
_CRONS = {"dogshowbase": ["0 0 1 * *"], "dogsuccess": ["0 1 13 * 5"]}


class DogshowContextCreator:
    def __init__(
        self,
        local_root,
        csv_path,
        remote_root=None,
        dvc_remotes=None,
        explore_remote=None,
    ):
        self.local_root = Path(local_root)
        gen_rmtree(self.local_root)
        self.local_root.mkdir()
        self.ran_dirs = []
        self.remote_root = Path(remote_root or self.local_root / "remotes")
        self.dvc_remotes = [*self._get_dvc_remotes(dvc_remotes)]
        self.cc_context = {
            "csv_path": csv_path,
            "test_registry": self._init_if_local(self.remote_root / "dogshow-registry"),
            "explore_remote": json.dumps(explore_remote),
            "remote2": self.dvc_remotes[1][0],
        }
        self.all_contexts = map(self.project_ctx, _PROJECTS)

    @contextmanager
    def project_ctx(self, name):
        root_dir = self.local_root / name
        root_dir.mkdir()
        template_path = project_cc_root / f"cc-{name}"
        git_remote = self._init_if_local(self.remote_root / f"dogshow-{name}")
        check_call(["git", "clone", TEMPLATE_REPO, "."], cwd=root_dir)
        Path(root_dir, MAIN_MODULE_NAME, "core.py").unlink()
        check_call(["git", "remote", "set-url", "origin", git_remote], cwd=root_dir)
        generate_files(
            template_path,
            {"cookiecutter": {"project": name}, **self.cc_context},
            self.local_root,
            overwrite_if_exists=True,
        )
        self.ran_dirs.append(root_dir)
        with cd_into(root_dir):
            _add_readme()
            for remote_name, remote_id in self.dvc_remotes:
                check_call(["dvc", "remote", "add", remote_name, remote_id])
            check_call(["dvc", "remote", "default", self.dvc_remotes[0][0]])
            git_run(add=["*"], msg="setup project")
            yield _VERSIONS.get(name, []), _CRONS.get(name, [])

    @contextmanager
    def explorer(self):
        root_dir = self.local_root / "explorer"
        root_dir.mkdir()
        conf_template = dogshow_root / "explorer" / EXPLORE_CONF_PATH
        conf_str = Template(conf_template.read_text()).render(**self.cc_context)
        with cd_into(root_dir):
            EXPLORE_CONF_PATH.write_text(conf_str)
            yield
            remote = self._init_if_local(self.remote_root / "dogshow-explorer")
            check_call(["git", "init"])
            check_call(["git", "remote", "add", "origin", remote])
            git_run(add=["*"], msg="setup-dec")
            check_call(["git", "push", "--set-upstream", "origin", "main"])

    def check_sdists(self):
        pass  # TODO check builds

    @classmethod
    def load(cls, mode: str, tmp_path: Path):
        if mode == "test":
            kwargs = {
                "csv_path": Path(dogshow_root, "data").absolute().as_posix(),
                "local_root": tmp_path,
            }
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

    def _init_if_local(self, repo_path):
        if not str(repo_path).startswith("git@"):
            repo_path.mkdir(parents=True, exist_ok=True)
            check_call(["git", "init"], cwd=repo_path)
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


def _add_readme():
    Path("README.md").write_text(f"test project for [this]({TEMPLATE_REPO}) template")


def _get_paths():
    yield BASE_CONF_PATH
    for root, _, files in os.walk(Path(MAIN_MODULE_NAME), topdown=False):
        for name in files:
            sfile = Path(root, name)
            if "__pycache__" in sfile.as_posix():
                continue
            yield sfile
