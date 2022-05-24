import datetime as dt
import os
from dataclasses import asdict
from subprocess import check_call

import typer
from dvc.repo import Repo
from structlog import get_logger

from .config_loading import Config, RunConfig, get_tag
from .exceptions import ProjectSetupException
from .explorer import build_explorer, init_explorer, load_explorer_data
from .get_runtime import get_runtime
from .gh_actions import write_cron_actions
from .naming import (
    BASE_CONF_PATH,
    CRON_ENV_VAR,
    MAIN_MODULE_NAME,
    SANDBOX_DIR,
    TEMPLATE_REPO,
)
from .pipeline_registry import get_global_pipereg
from .registry import Registry
from .sql.draw import dump_graph
from .sql.loader import tmp_constr
from .utils import gen_rmtree, get_git_diffs, git_run
from .validation_functions import validate, validate_importable

logger = get_logger(ctx="CLI command")
app = typer.Typer()

app.command()(validate)
app.command()(init_explorer)
app.command()(build_explorer)
app.command()(load_explorer_data)


@app.command()
def run_step(name: str):
    get_runtime().pipereg.get_step(name).run()


@app.command()
def publish_meta():
    _validate_empty_vc("publishing meta", [MAIN_MODULE_NAME])
    get_runtime(True).registry.publish()


@app.command()
def update_registry():
    get_runtime(True).registry.update()


@app.command()
def init(name: str):
    check_call(["git", "clone", TEMPLATE_REPO, name])
    check_call(["git", "remote", "rename", "origin", "template"], cwd=name)


@app.command()
def update():
    update_registry()
    git_run(pull=True)
    Repo().pull()


@app.command()
def draw(v: bool = False):
    with tmp_constr(v) as constr:
        dump_graph(constr)


@app.command()
def publish_data(validate: bool = False):
    build_meta()
    _validate_empty_vc("publishing data")
    runtime = get_runtime(True)
    dvc_repo = Repo()
    data_version = runtime.metadata.next_data_v
    vtags = []
    for env in runtime.config.sorted_envs:
        targets = runtime.pipereg.step_names_of_env(env.name)
        logger.info("pushing targets", targets=targets, env=env.name)
        dvc_repo.push(targets=targets, remote=env.remote)
        vtag = get_tag(runtime.config.version, data_version, env.name)
        _commit_dvc_default(env.remote)
        check_call(["git", "tag", vtag])
        vtags.append(vtag)
    git_run(push=True)
    for tag_to_push in vtags:
        check_call(["git", "push", "origin", tag_to_push])
    runtime.registry.publish()
    if validate:
        validate_importable(runtime)


@app.command()
def build_meta():
    get_global_pipereg(reset=True)  # used when building meta from script
    Registry(Config.load()).full_build()
    runtime = get_runtime(True)
    crons = set(filter(None, [s.cron for s in runtime.pipereg.steps]))
    if crons:
        logger.info("writing github actions files for crons", crons=crons)
        write_cron_actions(crons)


@app.command()
def load_external_data(git_commit: bool = False):
    """watch out, this deletes everything

    should probably be a bit more clever,
    but dvc handles quite a lot of caching
    """
    runtime = get_runtime(True)
    logger.info("loading external data", envs=runtime.data_to_load)
    posixes = runtime.load_all_data()
    if git_commit and posixes:
        git_run(add=["*.gitignore", *[f"{p}.dvc" for p in posixes]])
        if get_git_diffs(True):
            git_run(msg="add imported datases")


@app.command()
def cleanup():
    conf = Config.load()
    Registry(conf).purge()
    gen_rmtree(SANDBOX_DIR)


@app.command()
def run_cronjobs(cronexpr: str = None, commit: bool = True):
    runtime = get_runtime()
    for step in runtime.pipereg.steps:
        if step.cron == (cronexpr or os.environ.get(CRON_ENV_VAR)):
            runtime.config.bump_cron(step.name)
    run(commit=commit)


@app.command()
def run(
    stage: bool = True, profile: bool = False, env: str = None, commit: bool = False
):
    dvc_repo = Repo(config={"core": {"autostage": stage}})
    runtime = get_runtime(True)
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
    targets = runtime.pipereg.step_names_of_env(env) if env else None
    rconf = RunConfig(profile=profile)
    with rconf:
        logger.info("running repro", targets=targets, **asdict(rconf))
        runs = dvc_repo.reproduce(targets=targets)
    git_run(add=["dvc.yaml", "dvc.lock", BASE_CONF_PATH])
    if commit and get_git_diffs(True):
        now = dt.datetime.now().isoformat(" ", "minutes")
        git_run(msg=f"at {now} ran: {runs}")
    return runs


def _commit_dvc_default(remote):
    check_call(["dvc", "remote", "default", remote])
    git_run(add=[".dvc"])
    if get_git_diffs(True):
        git_run(msg=f"update dvc default remote to {remote}")


def _validate_empty_vc(attempt, prefs=("dvc.", MAIN_MODULE_NAME)):
    for fp in get_git_diffs() + get_git_diffs(True):
        if any([*[fp.startswith(pref) for pref in prefs], fp == BASE_CONF_PATH]):
            msg = f"{fp} should be committed to git before {attempt}"
            raise ProjectSetupException(msg)
