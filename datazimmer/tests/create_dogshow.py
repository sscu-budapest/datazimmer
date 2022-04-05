import os
from contextlib import contextmanager
from pathlib import Path
from shutil import rmtree
from subprocess import check_call
from typing import Optional

from cookiecutter.main import generate_files
from jinja2 import Template
from structlog import get_logger
from yaml import safe_load

from datazimmer.naming import (
    BASE_CONF_PATH,
    EXPLORE_CONF_PATH,
    MAIN_MODULE_NAME,
    TEMPLATE_REPO,
    repo_link
)
from datazimmer.utils import cd_into, git_run, package_root

logger = get_logger()

dogshow_root = package_root / "dogshow"
artifact_src_root = dogshow_root / "artifacts"
dec_src_root = dogshow_root / "explorer"

_ARTIFACTS = ["dogshowbase", "dogracebase", "dogsuccess", "dogcombine"]
_VERSIONS = {"dogshowbase": ["0.0", "0.1"], "dogsuccess": ["1.0"]}
_CRONS = {"dogshowbase": ["0 0 1 * *"]}


class DogshowContextCreator:
    def __init__(
        self,
        local_output_root: Path,
        git_remote_root: Optional[Path] = None,
        dvc_remotes: Optional[list] = None,
    ) -> None:

        self.local_root = local_output_root
        self.ran_dirs = []
        self.git_remote_root = Path(git_remote_root or self.local_root / "git-remotes")
        self.registry_path = self.git_remote_root / "test-registry"
        self._init_if_local(self.registry_path)
        self.dvc_remotes = dvc_remotes or [*self._get_dvc_remotes(2)]

        csv_path = Path(dogshow_root, "data").absolute().as_posix()
        self.cc_context = {
            "csv_path": csv_path,
            "test_registry": str(self.registry_path),
        }
        self.all_contexts = map(self.artifact_ctx, _ARTIFACTS)

    @contextmanager
    def artifact_ctx(self, name):
        root_dir = self.local_root / name
        root_dir.mkdir()
        template_path = artifact_src_root / f"cc-{name}"
        git_remote = self._init_if_local(self.git_remote_root / f"dogshow-{name}")
        check_call(["git", "clone", TEMPLATE_REPO, "."], cwd=root_dir)
        Path(root_dir, MAIN_MODULE_NAME, "core.py").unlink()
        check_call(["git", "remote", "set-url", "origin", git_remote], cwd=root_dir)
        generate_files(
            template_path,
            {"cookiecutter": {"artifact": name}, **self.cc_context},
            self.local_root,
            overwrite_if_exists=True,
        )
        self.ran_dirs.append(root_dir)
        with cd_into(root_dir):
            _add_readme()
            for i, remote in enumerate(self.dvc_remotes):
                # dog-remote1 and dog-remote2 names are set in the yamls
                check_call(["dvc", "remote", "add", f"dog-remote{i+1}", remote])
            check_call(["dvc", "remote", "default", "dog-remote1"])
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

    def check_sdists(self):
        pass  # TODO check builds

    def _get_dvc_remotes(self, n):
        for i in range(n):
            dvc_dir = self.local_root / "dvc-remotes" / f"remote-{i+1}"
            dvc_dir.mkdir(parents=True)
            yield dvc_dir.absolute().as_posix()

    def _init_if_local(self, repo_path):
        if not str(repo_path).startswith("git@"):
            repo_path.mkdir(parents=True, exist_ok=True)
            check_call(["git", "init"], cwd=repo_path)
        return repo_path


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


def setup_dogshow(mode, tmp_path: Path) -> DogshowContextCreator:
    if mode == "test":
        return DogshowContextCreator(tmp_path)
    else:  # pragma: no cover
        conf_dic = safe_load((dogshow_root / "confs-live.yaml").read_text())[mode]
        conf_root = Path(conf_dic.pop("local_output_root"))
        rmtree(conf_root, ignore_errors=True)
        conf_root.mkdir()
        return DogshowContextCreator(local_output_root=conf_root, **conf_dic)


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
