import os
import sys
from dataclasses import dataclass
from distutils.dir_util import copy_tree
from pathlib import Path
from subprocess import CalledProcessError, check_call
from tempfile import TemporaryDirectory

import yaml

from sscutils.constants import IMPORTED_DATASETS_CONFIG_PATH

sscutil_root = Path(__file__).parent.parent.parent
ma_dir = sscutil_root / "mock-artifacts"


@dataclass
class TmpProjectConfig:
    src_name: str
    dvc_remote_count: int

    @property
    def src_path(self):
        return ma_dir / self.src_name


ds1_config = TmpProjectConfig("dataset1", 2)
ds2_config = TmpProjectConfig("dataset2", 1)
dp1_config = TmpProjectConfig("research-project", 1)
dp2_config = TmpProjectConfig("research-project2", 1)


class TemporaryProject:

    _config = dp1_config

    def __init__(
        self,
        root_path: Path,
        git_remote=None,
        dvc_remotes=None,
        external_dvc_repos=None,
        git_user="John Doe",
        git_email="johndoe@example.com",
        csv_path=Path(ma_dir, "data").absolute().as_posix(),
        template_repo="",
        template_repo_tag="",
    ):
        """creates the test datasets and projects"""
        root_path.mkdir(exist_ok=True)
        self._root_dir = root_path
        self._git_user = git_user
        self._git_email = git_email
        self._abs_csv_path = csv_path
        self._template_repo = template_repo
        self._template_repo_tag = template_repo_tag
        self._prev_cwd = Path.cwd()

        self._tmp_dirs = []
        self._dvc_remotes = [
            self._get_dirname_or_tmp(dvc_remotes[i] if dvc_remotes else None)
            for i in range(self._config.dvc_remote_count)
        ]
        self._external_dvc = external_dvc_repos

        self._git_remote = self._get_dirname_or_tmp(git_remote)
        if not (
            self._git_remote.startswith("https://")
            or self._git_remote.startswith("git@")
        ):
            self._setup_repo(self._git_remote, to_tmp_branch=True)

    def __enter__(self):
        self._setup_repo(
            self._root_dir,
            remote=self._git_remote,
            template_repo=self._template_repo,
        )
        copy_tree(self._config.src_path.as_posix(), self._root_dir.as_posix())
        self._init_dvc(self._root_dir)

        os.chdir(self._root_dir)
        sys.path.insert(0, str(self._root_dir))

        self._add_readme()
        self._spec_enter()
        self._commit_changes()

        try:
            sys.modules.pop("src")
            sys.modules.pop("src.create_subsets")
        except KeyError:
            pass

    def __exit__(self, *args):
        os.chdir(self._prev_cwd)
        sys.path.pop(0)
        for tmp_dir in self._tmp_dirs:
            tmp_dir.__exit__(*args)

    def _get_dirname_or_tmp(self, new_dir):
        if new_dir is None:
            tmp_dir = TemporaryDirectory()
            self._tmp_dirs.append(tmp_dir)
            return tmp_dir.__enter__()
        return str(new_dir)

    def _setup_repo(
        self, cwd, to_tmp_branch=False, remote="", template_repo=""
    ):

        if template_repo:
            commands = [["git", "clone", template_repo, "."]]
            if self._template_repo_tag:
                commands += [
                    ["git", "checkout", self._template_repo_tag],
                    ["git", "checkout", "-b", "tmp"],
                    ["git", "checkout", "-B", "main", "tmp"],
                    ["git", "branch", "-d", "tmp"],
                ]
        else:
            commands = [["git", "init"]]

        commands += [
            ["git", "config", "--local", "user.name", self._git_user],
            ["git", "config", "--local", "user.email", self._git_email],
        ]
        if remote:
            commands.append(
                [
                    "git",
                    "remote",
                    "set-url" if template_repo else "add",
                    "origin",
                    remote,
                ]
            )
        if to_tmp_branch:
            commands.append(["git", "checkout", "-b", "tmp-tag"])
        for comm in commands:
            check_call(comm, cwd=cwd)

    def _init_dvc(self, cwd):
        commands = []

        if not self._template_repo:
            # assumed that dvc is set up in the template
            commands.append(["dvc", "init"])

        for i, remote in enumerate(self._dvc_remotes):
            # remote1 and remote2 names are set in the yamls
            commands.append(["dvc", "remote", "add", f"remote{i+1}", remote])

        if commands:
            commands += [
                ["git", "add", ".dvc"],
                ["git", "commit", "-m", "setup dvc"],
            ]

        for comm in commands:
            check_call(comm, cwd=cwd)

    def _commit_changes(self):
        commands = [
            ["git", "add", "src"],
            ["git", "commit", "-m", "add src"],
            ["git", "add", "*yaml"],
            ["git", "commit", "-m", "add configuration"],
            ["git", "add", "*.py"],
            ["git", "commit", "-m", "add script"],
            ["git", "add", "*.md"],
            ["git", "commit", "-m", "add documentation"],
        ]
        for comm in commands:
            try:
                check_call(comm)
            except CalledProcessError:
                pass

    def _add_readme(self):
        Path("README.md").write_text(
            f"test artifact for {self._template_repo_tag} of "
            f"[this]({self._template_repo}) template"
        )

    def _spec_enter(self):
        ds_repo1, ds_repo2 = self._external_dvc
        imported_ds_list = [
            {
                "prefix": "op",
                "repo": ds_repo1,
                "subsets": [
                    {"name": "complete", "tag": "main"},
                    {"name": "pete_dave_success", "tag": "public"},
                ],
            },
            {
                "prefix": "lv",
                "repo": ds_repo2,
                "subsets": [{"name": "complete", "tag": "main"}],
            },
        ]

        IMPORTED_DATASETS_CONFIG_PATH.write_text(yaml.dump(imported_ds_list))


class TemporaryProject2(TemporaryProject):

    _config = dp1_config

    def _spec_enter(self):
        pass
        # add transformed step from porject 1


class TemporaryDataset(TemporaryProject):
    _config = ds1_config

    def _spec_enter(self):
        check_call(
            [
                "python",
                "load_init_data.py",
                self._abs_csv_path,
            ],
            env={**os.environ, "PYTHONPATH": sscutil_root.as_posix()},
        )


class TemporaryDataset2(TemporaryDataset):
    _config = ds2_config


def switch_branch(branch, cwd):
    comm = ["git", "checkout", "-b", branch]
    try:
        check_call(comm, cwd=cwd)
    except CalledProcessError:
        comm.pop(2)
        check_call(comm, cwd=cwd)


def switch_branch_mf(branch, cwd):
    def f():
        switch_branch(branch, cwd)

    return f
