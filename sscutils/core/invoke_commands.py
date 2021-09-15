from pathlib import Path

from dvc.repo import Repo
from invoke import Collection, task
from invoke.exceptions import UnexpectedExit

from ..constants import DATA_PATH, SRC_PATH
from .io import (
    import_subset_creator_function,
    load_branch_remote_pairs,
    load_created_subsets,
    load_imported_datasets,
)
from .metadata_files import copy_all_metadata
from .path_functions import get_subset_path


@task
def lint(c):
    c.run(f"black {SRC_PATH} -l 119")
    c.run(f"isort {SRC_PATH} -m 3 --tc")
    c.run(f"flake8 {SRC_PATH} --max-line-length=119")


@task
def init_dataset(ctx):
    Path(ctx.cwd, DATA_PATH).mkdir(exist_ok=True)


@task
def set_dvc_remotes(ctx):
    for branch, remote in load_branch_remote_pairs():
        _try_checkout(ctx, branch)
        ctx.run(f"dvc remote default {remote}")
        ctx.run("git add .dvc")
        ctx.run(f'git commit -m "update dvc default remote to {remote}"')


@task
def write_subsets(ctx):
    subset_creator_fun = import_subset_creator_function()
    for ss in load_created_subsets():
        subset_creator_fun(ss.name, **ss.sampling_kwargs)


@task
def push_subsets(ctx, git_push=False):
    for ss in load_created_subsets():
        ss_posix = get_subset_path(ss.name).as_posix()
        _try_checkout(ctx, ss.branch)
        dvc_repo = Repo()
        dvc_repo.add(ss_posix)
        ctx.run(f"git add {ss_posix}.dvc **/.gitignore")
        try:
            ctx.run(f'git commit -m "add data subset {ss.name}"')
        except UnexpectedExit:
            continue
        if git_push:
            try:
                ctx.run("git push")
            except UnexpectedExit:
                ctx.run(f"git push -u origin {ss.branch}")
        dvc_repo.push()


@task
def import_data(ctx, env_only=True, git_commit=True):
    dvc_repo = Repo()
    for dset in load_imported_datasets(env_only):
        src_loc = get_subset_path(dset.subset).as_posix()
        out_path = get_subset_path(dset.subset, dset.prefix)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        out_loc = out_path.as_posix()
        dvc_repo.imp(
            url=dset.repo,  # git@github.com:/user/repo
            path=src_loc,
            out=out_loc,
            rev=dset.tag or None,
            fname=None,
        )
        meta_files = copy_all_metadata(dset.repo, dset.tag, dset.prefix)
        if git_commit:
            ctx.run(
                f"git add *.gitignore {out_loc}.dvc {' '.join(meta_files)}"
            )
            ctx.run(
                'git commit -m "add imported dataset'
                f' {dset.prefix}: {dset.subset}"'
            )


common_tasks = [set_dvc_remotes, lint]

dataset_ns = Collection(
    init_dataset, write_subsets, push_subsets, *common_tasks
)
project_ns = Collection(import_data, *common_tasks)


def _try_checkout(ctx, branch):
    try:
        ctx.run(f"git checkout {branch}")
    except UnexpectedExit:
        ctx.run(f"git checkout -b {branch}")
