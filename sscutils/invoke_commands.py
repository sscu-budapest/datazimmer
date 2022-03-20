from dataclasses import asdict

from dvc.repo import Repo
from invoke import Collection, UnexpectedExit, task
from structlog import get_logger

from .config_loading import Config, RunConfig, get_tag
from .get_runtime import get_runtime
from .naming import CONSTR, MAIN_MODULE_NAME
from .pipeline_registry import get_global_pipereg
from .registry import Registry
from .utils import LINE_LEN
from .validation_functions import validate_artifact

logger = get_logger(ctx="invoke task")


@task
def lint(ctx, line_length=LINE_LEN):
    ctx.run(f"black {MAIN_MODULE_NAME} -l {line_length}")
    ctx.run(f"isort {MAIN_MODULE_NAME} --profile black")
    ctx.run(f"flake8 {MAIN_MODULE_NAME} --max-line-length={line_length}")


@task
def release(ctx, constr=CONSTR):
    # TODO
    # check if version tag is already present in remote registry
    # check dvc remotes from created-envs are present
    # validate dataset
    # tag first
    # try it all, push tags if good, remove tags if bad
    # need to push to dvc though!
    # for the switching of dvc repos, prep work needs to be done

    runtime = get_runtime(True)
    assert runtime.config.validation_envs
    dvc_repo = Repo()
    run(ctx)  # TODO: nothing should run (?)
    # avoid tagging the same data differently
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
    validate(ctx, con=constr)


@task(aliases=("build",))
def build_meta(_):
    get_global_pipereg(reset=True)
    Registry(Config.load()).full_build()


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


@task
def run(_, stage=True, profile=False, force=False, env=None):
    conf = {}
    if stage:
        conf["core"] = {"autostage": True}
    dvc_repo = Repo(config=conf)

    runtime = get_runtime()

    stage_names = []
    for step in runtime.pipereg.steps:
        logger.info("adding step", step=step)
        step.add_as_stage(dvc_repo)
        stage_names.append(step.name)

    for stage in dvc_repo.stages:
        if stage.is_data_source or stage.is_import:
            continue
        if stage.name in stage_names:
            continue

        logger.info("removing step", step=step)
        dvc_repo.remove(stage.name)
        dvc_repo.lock.lock()
        stage.remove_outs(force=True)
        dvc_repo.lock.unlock()

    for env in runtime.config.envs:
        rconf = RunConfig(write_env=env.name, profile=profile)
        with rconf:
            env_stages = runtime.pipereg.step_names_of_env(env.name)
            logger.info("running repro", stages=env_stages, **asdict(rconf))
            dvc_repo.reproduce(force=force, targets=env_stages)


ns = Collection(*[lint, release, build_meta, validate, load_external_data, run])


def _commit_dvc_default(ctx, remote):
    ctx.run(f"dvc remote default {remote}")
    ctx.run("git add .dvc")
    try:
        ctx.run(f'git commit -m "update dvc default remote to {remote}"')
    except UnexpectedExit:
        pass
