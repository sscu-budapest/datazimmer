from shutil import rmtree

from dvc.repo import Repo
from invoke import Collection, task
from invoke.exceptions import UnexpectedExit

from .config_loading import (
    COMPLETE_ENV,
    DatasetConfig,
    ProjectConfig,
    load_branch_remote_pairs,
)
from .helpers import import_env_creator_function, import_update_data_function
from .metadata import ArtifactMetadata
from .naming import (
    COMPLETE_ENV_NAME,
    IMPORTED_NAMESPACES_SCRIPTS_PATH,
    METADATA_DIR,
    SRC_PATH,
)
from .utils import LINE_LEN


@task
def lint(ctx, line_length=LINE_LEN):
    ctx.run(f"black {SRC_PATH} -l {line_length}")
    ctx.run(f"isort {SRC_PATH} --profile black")
    ctx.run(f"flake8 {SRC_PATH} --max-line-length={line_length}")


@task
def set_dvc_remotes(ctx):
    for branch, remote in load_branch_remote_pairs():
        _try_checkout(ctx, branch)
        ctx.run(f"dvc remote default {remote}")
        ctx.run("git add .dvc")
        ctx.run(f'git commit -m "update dvc default remote to {remote}"')


@task
def import_namespaces(ctx, git_commit=False):

    meta = ArtifactMetadata.load_serialized()

    for ns in meta.imported_namespaces:
        meta.extend_from_import(ns)
    meta.dump()
    meta.imported_nss_to_datascript()
    if git_commit:
        try:
            _commit_serialized_meta(ctx)
            ctx.run(f"git add {IMPORTED_NAMESPACES_SCRIPTS_PATH}")
            ctx.run('git commit -m "import namespaces to datascript"')
        except UnexpectedExit:
            pass


@task(iterable=["args"])
def update_data(_, args=None):
    pargs = args or ()
    import_update_data_function()(*pargs)


@task
def write_envs(_):
    dataset_config = DatasetConfig()
    env_creator_fun = import_env_creator_function()
    for env in dataset_config.created_environments:
        if env.name == COMPLETE_ENV_NAME:
            assert (
                env.path.exists()
            ), f"{COMPLETE_ENV} env needs to exist before creating others"
            continue
        env_creator_fun(env.name, **env.kwargs)


@task
def push_envs(ctx, git_push=False):
    dataset_config = DatasetConfig()
    for env in dataset_config.created_environments:
        _try_checkout(ctx, env.branch)
        dvc_repo = Repo()
        dvc_repo.add(env.posix)
        ctx.run(f"git add {env.posix}.dvc **/.gitignore")
        try:
            ctx.run(f'git commit -m "add data subset {env.name}"')
        except UnexpectedExit:
            continue
        if git_push:
            try:
                ctx.run("git push")
            except UnexpectedExit:
                ctx.run(f"git push -u origin {env.branch}")
        dvc_repo.push()


@task
def serialize_datascript_metadata(ctx, git_commit=False):
    meta = ArtifactMetadata.load_serialized()
    meta.extend_from_datascript()
    meta.dump()
    if git_commit:
        _commit_serialized_meta(ctx)


@task
def load_external_data(ctx, git_commit=False):
    """watch out, this deletes everything

    should probably be a bit more clever,
    but dvc handles quite a lot of caching
    """
    project_config = ProjectConfig()
    dvc_repo = Repo()
    for ns_env in project_config.data_envs:
        src_loc = ns_env.src_posix
        out_path = ns_env.out_path
        rmtree(out_path, ignore_errors=True)  # brave thing...
        out_path.parent.mkdir(exist_ok=True, parents=True)
        out_loc = out_path.as_posix()
        dvc_repo.imp(
            url=ns_env.repo,
            path=src_loc,
            out=out_loc,
            rev=ns_env.tag or None,
            fname=None,
        )
        if git_commit:
            ctx.run(f"git add *.gitignore {out_loc}.dvc")
            ctx.run(
                'git commit -m "add imported dataset'
                f' {ns_env.local_name}: {ns_env.env}"'
            )


common_tasks = [
    lint,
    set_dvc_remotes,
    import_namespaces,
    serialize_datascript_metadata,
]

dataset_ns = Collection(*common_tasks, write_envs, push_envs)
project_ns = Collection(*common_tasks, load_external_data)


def _try_checkout(ctx, branch):
    try:
        ctx.run(f"git checkout {branch}")
    except UnexpectedExit:
        ctx.run(f"git checkout -b {branch}")


def _commit_serialized_meta(ctx):
    ctx.run(f"git add {METADATA_DIR}")
    ctx.run('git commit -m "update serialized metadata"')
