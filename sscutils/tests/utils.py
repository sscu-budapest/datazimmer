import os
import sys
from dataclasses import dataclass
from pathlib import Path
from shutil import copytree
from subprocess import CalledProcessError, check_call
from tempfile import TemporaryDirectory

import yaml

from sscutils.constants import IMPORTED_DATASETS_CONFIG_PATH

ma_path = Path("mock-artifacts")

csv_path = ma_path / "data"


@dataclass
class TmpProjectConfig:
    src_name: str
    dvc_remote_count: int
    branch_remote_pairs: list

    @property
    def src_path(self):
        return ma_path / self.src_name


ds1_config = TmpProjectConfig(
    "dataset1", 2, [("main", "remote1"), ("public", "remote2")]
)
ds2_config = TmpProjectConfig("dataset2", 1, [("main", "remote1")])
dp1_config = TmpProjectConfig("research-project", 1, [("main", "remote1")])
dp2_config = TmpProjectConfig("research-project2", 1, [("main", "remote1")])


class TemporaryProject:

    _config = dp1_config

    def __init__(
        self,
        root_path,
        git_remote=None,
        dvc_remotes=None,
        external_dvc_repos=None,
    ):
        """root path must not exist"""
        self._root_dir = root_path
        self._tmp_dirs = []
        self._dvc_remotes = [
            self._get_dirname_or_tmp(dvc_remotes[i] if dvc_remotes else None)
            for i in range(self._config.dvc_remote_count)
        ]
        self._external_dvc = external_dvc_repos

        self._prev_cwd = Path.cwd()
        self._abs_csv_path = csv_path.absolute().as_posix()

        self._git_remote = self._get_dirname_or_tmp(git_remote)
        self._setup_repo(self._git_remote, to_tmp_branch=True)

    def __enter__(self):
        copytree(self._config.src_path, self._root_dir)
        self._setup_repo(self._root_dir, remote=self._git_remote)
        self._init_dvc(self._root_dir)
        self._setup_dvc_remotes_for_branches(self._root_dir)

        os.chdir(self._root_dir)
        sys.path.insert(0, str(self._root_dir))

        self._spec_enter()
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

    def _setup_repo(self, cwd, to_tmp_branch=False, remote=""):
        commands = [
            ["git", "init"],
            ["git", "config", "--local", "user.name", "John Doe"],
            ["git", "config", "--local", "user.email", "johndoe@example.com"],
        ]
        if remote:
            commands.append(["git", "remote", "add", "origin", remote])
        if to_tmp_branch:
            commands.append(["git", "checkout", "-b", "tmp-tag"])
        for comm in commands:
            check_call(comm, cwd=cwd)

    def _init_dvc(self, cwd):
        commands = [
            ["dvc", "init"],
        ]
        for i, remote in enumerate(self._dvc_remotes):
            commands.append(["dvc", "remote", "add", f"remote{i+1}", remote])

        commands += [
            ["git", "add", ".dvc"],
            ["git", "commit", "-m", "setup dvc"],
        ]

        for comm in commands:
            check_call(comm, cwd=cwd)

    def _setup_dvc_remotes_for_branches(self, cwd):
        commands = []
        for branch_name, remote_name in self._config.branch_remote_pairs:
            commands += [
                switch_branch_mf(branch_name, cwd),
                ["dvc", "remote", "default", remote_name],
                ["git", "add", ".dvc"],
                [
                    "git",
                    "commit",
                    "-m",
                    f"update default dvc remote to {remote_name}",
                ],
                ["git", "push", "-u", "origin", branch_name],
            ]

        for comm in commands:
            if callable(comm):
                comm()
            else:
                check_call(comm, cwd=cwd)

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
                self._prev_cwd.as_posix(),
            ]
        )


class TemporaryDataset2(TemporaryDataset):
    _config = ds2_config


def switch_branch(branch, cwd):
    comm = ["git", "checkout", "-b", branch]
    try:
        check_call(comm, cwd=cwd)
    except CalledProcessError:
        comm.pop(2)
        check_call(comm, cdw=cwd)


def switch_branch_mf(branch, cwd):
    def f():
        switch_branch(branch, cwd)

    return f
