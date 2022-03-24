import datetime as dt
import os
from dataclasses import asdict
from shutil import rmtree

from dvc.repo import Repo
from invoke import Collection, task
from structlog import get_logger

from datazimmer.exceptions import ArtifactSetupException

from .config_loading import Config, RunConfig, get_tag
from .get_runtime import get_runtime
from .gh_actions import write_actions
from .naming import (
    BASE_CONF_PATH,
    CONSTR,
    CRON_ENV_VAR,
    MAIN_MODULE_NAME,
    SANDBOX_DIR,
    SANDBOX_NAME,
)
from .pipeline_registry import get_global_pipereg
from .registry import Registry
from .utils import LINE_LEN, get_git_diffs
from .validation_functions import validate_artifact, validate_importable

logger = get_logger(ctx="invoke task")


@task
def lint(ctx, line_length=LINE_LEN):
    ctx.run(f"black {MAIN_MODULE_NAME} -l {line_length}")
    ctx.run(f"isort {MAIN_MODULE_NAME} --profile black")
    ctx.run(f"flake8 {MAIN_MODULE_NAME} --max-line-length={line_length}")


@task
def publish_meta(_):
    _validate_empty_vc("publishing meta", [MAIN_MODULE_NAME])
    get_runtime(True).registry.publish()


@task
def publish_data(ctx, validate=False):
    # TODO: current meta should be built and published
    _validate_empty_vc("publishing data")
    runtime = get_runtime(True)
    dvc_repo = Repo()
    data_version = runtime.metadata.next_data_v
    for env in runtime.config.sorted_envs:
        targets = runtime.pipereg.step_names_of_env(env.name)
        logger.info("pushing targets", targets=targets, env=env.name)
        dvc_repo.push(targets=targets, remote=env.remote)
        vtag = get_tag(runtime.config.version, data_version, env.name)
        _commit_dvc_default(ctx, env.remote)
        ctx.run(f"git tag {vtag}")
        ctx.run(f"git push origin {vtag}")
    ctx.run("git push")
    runtime.registry.publish()
    if validate:
        validate_importable(runtime)


@task(aliases=("build",))
def build_meta(_):
    get_global_pipereg(reset=True)  # used when building meta from script
    Registry(Config.load()).full_build()
    runtime = get_runtime(True)
    crons = set(filter(None, [s.cron for s in runtime.pipereg.steps]))
    if crons:
        logger.info("writing github actions files for crons", crons=crons)
        write_actions(crons)


@task
def load_external_data(ctx, git_commit=False):
    """watch out, this deletes everything

    should probably be a bit more clever,
    but dvc handles quite a lot of caching
    """
    runtime = get_runtime(True)
    logger.info("loading external data", envs=runtime.data_to_load)
    posixes = runtime.load_all_data()
    if git_commit and posixes:
        dvc_paths = " ".join([f"{p}.dvc" for p in posixes])
        ctx.run(f"git add *.gitignore {dvc_paths}")
        if get_git_diffs(True):
            ctx.run('git commit -m "add imported datases"')


@task
def validate(_, con=CONSTR, draw=False, batch=20000):
    validate_artifact(con, draw, batch)


@task
def cleanup(ctx):
    conf = Config.load()
    aname = conf.name
    reg = Registry(conf, True)
    ctx.run(f"pip uninstall {aname} -y")
    reg.purge()
    if SANDBOX_DIR.exists():
        ctx.run(f"pip uninstall {SANDBOX_NAME} -y")
        rmtree(SANDBOX_DIR.as_posix())


@task
def run_cronjobs(ctx, cronexpr=None):
    runtime = get_runtime()
    for step in runtime.pipereg.steps:
        if step.cron == (cronexpr or os.environ.get(CRON_ENV_VAR)):
            runtime.config.bump_cron(step.name)
    run(ctx, commit=True)


@task
def run(ctx, stage=True, profile=False, force=False, env=None, commit=False):
    dvc_repo = Repo(config={"core": {"autostage": stage}})
    runtime = get_runtime()
    stage_names = []
    for step in runtime.pipereg.steps:
        logger.info("adding step", step=step)
        step.add_as_stage(dvc_repo)
        stage_names.append(step.name)

    for stage in dvc_repo.stages:
        if stage.is_data_source or stage.is_import or (stage.name in stage_names):
            continue
        logger.info("removing step", step=step)
        dvc_repo.remove(stage.name)
        dvc_repo.lock.lock()
        stage.remove_outs(force=True)
        dvc_repo.lock.unlock()

    targets = runtime.pipereg.step_names_of_env(env.name) if env else None
    rconf = RunConfig(profile=profile)
    with rconf:
        logger.info("running repro", targets=targets, **asdict(rconf))
        runs = dvc_repo.reproduce(force=force, targets=targets)
    ctx.run(f"git add dvc.yaml dvc.lock {BASE_CONF_PATH}")
    if commit and get_git_diffs(True):
        ctx.run(f'git commit -m "run {dt.datetime.now().isoformat()} {runs}"')
    return runs


all_tasks = [
    lint,
    publish_data,
    publish_meta,
    build_meta,
    validate,
    load_external_data,
    run,
    cleanup,
    run_cronjobs,
]
ns = Collection(*all_tasks)


def _commit_dvc_default(ctx, remote):
    ctx.run(f"dvc remote default {remote}")
    ctx.run("git add .dvc")
    if get_git_diffs(True):
        ctx.run(f'git commit -m "update dvc default remote to {remote}"')


def _validate_empty_vc(attempt, prefs=("dvc.", MAIN_MODULE_NAME)):
    for fp in get_git_diffs() + get_git_diffs(True):
        if any([*[fp.startswith(pref) for pref in prefs], fp == BASE_CONF_PATH]):
            msg = f"{fp} should be committed to git before {attempt}"
            raise ArtifactSetupException(msg)
