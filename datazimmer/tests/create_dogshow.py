import os
from contextlib import contextmanager
from pathlib import Path
from subprocess import CalledProcessError, check_call
from typing import Optional

from cookiecutter.main import generate_files
from structlog import get_logger

from datazimmer.naming import BASE_CONF_PATH, MAIN_MODULE_NAME, TEMPLATE_REPO
from datazimmer.utils import cd_into, package_root

logger = get_logger()

dogshow_root = package_root / "dogshow"
artifact_src_root = dogshow_root / "artifacts"
csv_path = Path(artifact_src_root, "data").absolute().as_posix()

dog_artifacts = ["dogshowbase", "dogracebase", "dogsuccess", "dogcombine"]

_VERSIONS = {"dogshowbase": ["0.0", "0.1"], "dogsuccess": ["1.0"]}
_CRONS = {"dogshowbase": ["0 0 1 * *"]}


class DogshowContextCreator:
    def __init__(
        self,
        local_output_root: Path,
        git_remote_root: Optional[Path] = None,
        dvc_remotes: Optional[list] = None,
        git_user="John Doe",
        git_email="johndoe@example.com",
    ) -> None:

        self.local_root = local_output_root
        self.git_user = git_user
        self.git_email = git_email
        self.ran_dirs = []

        self.git_remote_root = Path(git_remote_root or self.local_root / "git-remotes")
        self.csv_path = csv_path
        self.registry_path = self.git_remote_root / "test-registry"
        self._init_if_local(self.registry_path)
        self.dvc_remotes = dvc_remotes or self._get_dvc_remotes(2)

        self.cc_context = {
            "csv_path": csv_path,
            "test_registry": str(self.registry_path),
        }
        self.all_contexts = map(
            self.artifact_ctx,
            dog_artifacts,
        )

    def get_git_remote(self, name):
        remote = self.git_remote_root / f"dogshow-{name}"
        self._init_if_local(remote)
        return remote

    def init_git_repo(self, dirpath: str, remote="", template_repo=""):

        if template_repo:
            commands = [["git", "clone", template_repo, "."], ["rm", "src/core.py"]]
        else:
            commands = [["git", "init"]]

        gconfs = [
            ("user.name", self.git_user),
            ("user.email", self.git_email),
            ("receive.denyCurrentBranch", "updateInstead"),
        ]
        commands += [["git", "config", "--local", *gconf] for gconf in gconfs]
        if remote:
            _rcomm = "set-url" if template_repo else "add"
            commands.append(["git", "remote", _rcomm, "origin", remote])

        for comm in commands:
            check_call(comm, cwd=dirpath)

    def _get_dvc_remotes(self, n):
        out = []
        for i in range(n):
            dvc_dir = self.local_root / "dvc-remotes" / f"remote-{i+1}"
            dvc_dir.mkdir(parents=True)
            out.append(dvc_dir.absolute().as_posix())
        return out

    def _init_if_local(self, repo_path):
        if not str(repo_path).startswith("git@"):
            repo_path.mkdir(parents=True, exist_ok=True)
            self.init_git_repo(repo_path)

    @contextmanager
    def artifact_ctx(self, name):
        root_dir = self.local_root / name
        root_dir.mkdir()
        template_path = artifact_src_root / f"cc-{name}"
        git_remote = self.get_git_remote(name)
        self.init_git_repo(root_dir, git_remote, TEMPLATE_REPO)
        add_dvc_remotes(root_dir, self.dvc_remotes)
        generate_files(
            template_path,
            {"cookiecutter": {"artifact": name}, **self.cc_context},
            self.local_root,
            overwrite_if_exists=True,
        )
        self.ran_dirs.append(root_dir)
        with cd_into(root_dir):
            _add_readme(TEMPLATE_REPO)
            _commit_changes(root_dir)
            yield _VERSIONS.get(name, []), _CRONS.get(name, [])

    def check_sdist_checksums(self):
        pass  # TODO check builds


def add_dvc_remotes(dirpath, remotes):
    "adds remotes with names remote1, remote2"
    commands = []

    for i, remote in enumerate(remotes):
        # remote1 and remote2 names are set in the yamls
        commands.append(["dvc", "remote", "add", f"remote{i+1}", remote])
    commands.append(["dvc", "remote", "default", "remote1"])

    if commands:
        commands += [
            ["git", "add", ".dvc"],
            ["git", "commit", "-m", "setup dvc"],
        ]

    for comm in commands:
        check_call(comm, cwd=dirpath)


def modify_to_version(version: str):
    # TODO
    # add one output/dependency for something + delete one of either"
    # {"sex_match": "sex_pairing"}

    del_str = f"# remove in v{version}"
    add_str = f"# add in v{version}: "
    changes = 0

    for path in _get_paths():
        lines = []
        s = path.read_text()
        for old_line in s.split("\n"):
            if old_line.startswith(add_str):
                new_line = old_line.replace(add_str, "")
                changes += 1
            elif old_line.endswith(del_str):
                changes += 1
                continue
            elif (path == BASE_CONF_PATH) and old_line.startswith("version: "):
                new_line = f"version: v{version}"
            else:
                new_line = old_line
            lines.append(new_line)
        path.write_text("\n".join(lines))
    return changes


def _commit_changes(dirpath):
    # TODO simplify
    commands = []
    for addp in [Path(MAIN_MODULE_NAME), BASE_CONF_PATH]:
        commands += [
            ["git", "add", addp.as_posix()],
            ["git", "commit", "-m", f"add {addp}"],
        ]
    commands += [
        ["git", "add", "*.py"],
        ["git", "commit", "-m", "add plus script"],
        ["git", "add", "*.md"],
        ["git", "commit", "-m", "add documentation"],
    ]
    for comm in commands:
        try:
            check_call(comm, cwd=dirpath)
        except CalledProcessError:
            pass


def _add_readme(template_repo):
    Path("README.md").write_text(
        f"test artifact for " f"[this]({template_repo}) template"
    )


def _get_paths():
    yield BASE_CONF_PATH
    srcp = Path(MAIN_MODULE_NAME)
    for root, _, files in os.walk(srcp, topdown=False):
        for name in files:
            sfile = Path(root, name)
            if "__pycache__" in sfile.as_posix():
                continue
            yield sfile
