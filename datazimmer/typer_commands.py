import datetime as dt
import re
from dataclasses import asdict
from subprocess import check_call

import typer
from dvc.repo import Repo
from structlog import get_logger

from .config_loading import CONF_KEYS, Config, RunConfig, get_tag, meta_version_from_tag
from .exceptions import ProjectSetupException
from .explorer import build_explorer, init_explorer, load_explorer_data
from .get_runtime import get_runtime
from .gh_actions import write_aswan_crons, write_project_cron
from .metadata.high_level import ProjectMetadata
from .naming import BASE_CONF_PATH, MAIN_MODULE_NAME, SANDBOX_DIR, TEMPLATE_REPO
from .raw_data import IMPORTED_RAW_DATA_DIR, RAW_DATA_DIR, RAW_ENV_NAME
from .registry import Registry
from .sql.draw import dump_graph
from .sql.loader import tmp_constr
from .utils import gen_rmtree, get_git_diffs, git_run
from .validation_functions import validate

logger = get_logger(ctx="CLI command")
app = typer.Typer()

app.command()(validate)
app.command()(init_explorer)
app.command()(build_explorer)
app.command()(load_explorer_data)


@app.command()
def run_step(name: str, env: str):
    get_runtime().run_step(name, env)


@app.command()
def publish_meta():
    _validate_empty_vc("publishing meta", [MAIN_MODULE_NAME])
    get_runtime().registry.publish()


@app.command()
def update_registry():
    Registry(Config.load()).update()


@app.command()
def init(name: str):
    git_run(clone=(TEMPLATE_REPO, name), depth=None)
    c_p = name / BASE_CONF_PATH
    cstr = re.sub(f"{CONF_KEYS.name}: .+", f"{CONF_KEYS.name}: {name}", c_p.read_text())
    c_p.write_text(cstr)
    check_call(["git", "remote", "rm", "origin"], cwd=name)


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
def import_raw(project: str, tag: str = ""):
    dvc_repo = Repo()
    reg = get_runtime().registry
    v = meta_version_from_tag(tag) if tag else None
    uri = ProjectMetadata(**reg.get_project_meta_base(project, v)).uri
    IMPORTED_RAW_DATA_DIR.mkdir(exist_ok=True)
    dvc_repo.imp(
        uri,
        path=RAW_DATA_DIR.as_posix(),
        out=(IMPORTED_RAW_DATA_DIR / project).as_posix(),
        rev=tag or None,
    )


@app.command()
def publish_data():
    build_meta()
    _validate_empty_vc("publishing data")
    runtime = get_runtime()
    dvc_repo = Repo()
    data_version = runtime.metadata.next_data_v
    vtags = []

    def _tag(env_name, env_remote):
        vtag = get_tag(runtime.config.version, data_version, env_name)
        _commit_dvc_default(env_remote)
        check_call(["git", "tag", vtag])
        vtags.append(vtag)

    default_remote = runtime.config.get_env(runtime.config.default_env).remote
    if RAW_DATA_DIR.exists():
        dvc_repo.add(RAW_DATA_DIR.as_posix())
        dvc_repo.push(targets=RAW_DATA_DIR.as_posix(), remote=default_remote)
        git_run(add=[f"{RAW_DATA_DIR}.dvc"], msg="save raw data", check=True)
        _tag(RAW_ENV_NAME, default_remote)
    for env in runtime.config.sorted_envs:
        targets = runtime.step_names_of_env(env.name)
        logger.info("pushing targets", targets=targets, env=env.name)
        dvc_repo.push(targets=targets, remote=env.remote)
        _tag(env.name, env.remote)
    git_run(push=True, pull=True)
    for tag_to_push in vtags:
        check_call(["git", "push", "origin", tag_to_push])
    runtime.registry.publish()


@app.command()
def build_meta(global_conf: bool = False):
    config = Config.load()
    Registry(config).full_build(global_conf)
    if config.cron:
        write_project_cron(config.cron)
    runtime = get_runtime()
    write_aswan_crons(runtime.metadata.complete.aswan_projects)


@app.command()
def load_external_data(git_commit: bool = False, env: str = None):
    """watch out, this deletes everything

    should probably be a bit more clever,
    but dvc handles quite a lot of caching
    """
    runtime = get_runtime()
    logger.info("loading external data", envs=runtime.data_to_load)
    posixes = runtime.load_all_data(env=env)
    if git_commit and posixes:
        vc_paths = ["*.gitignore", *[f"{p}.dvc" for p in posixes]]
        git_run(add=vc_paths, msg="add imported datasets", check=True)


@app.command()
def cleanup():
    conf = Config.load()
    Registry(conf).purge()
    gen_rmtree(SANDBOX_DIR)


@app.command()
def run_aswan_project(project: str = "", publish: bool = True):
    runtime = get_runtime()
    for asw_project in runtime.metadata.complete.aswan_projects:
        if project and (project != asw_project.name):
            continue
        asw_project(global_run=True).run()
    if publish:
        git_run(add=(BASE_CONF_PATH,), msg="asw-run", push=True, pull=True, check=True)


@app.command()
def run(
    stage: bool = True, profile: bool = False, env: str = None, commit: bool = False
):
    dvc_repo = Repo(config={"core": {"autostage": stage}})
    runtime = get_runtime()
    stage_names = [
        stage_name
        for step in runtime.metadata.complete.pipeline_elements
        for stage_name in step.add_stages(dvc_repo)
    ]
    for stage in dvc_repo.stages:
        if stage.is_data_source or stage.is_import or (stage.name in stage_names):
            continue
        logger.info("removing dvc stage", stage=stage.name)
        dvc_repo.remove(stage.name)
        dvc_repo.lock.lock()
        stage.remove_outs(force=True)
        dvc_repo.lock.unlock()
    if not stage_names:
        return
    targets = runtime.step_names_of_env(env) if env else None
    rconf = RunConfig(profile=profile)
    with rconf:
        logger.info("running repro", targets=targets, **asdict(rconf))
        runs = dvc_repo.reproduce(targets=targets, pull=True)
    git_run(add=["dvc.yaml", "dvc.lock", BASE_CONF_PATH])
    if commit:
        now = dt.datetime.now().isoformat(" ", "minutes")
        git_run(msg=f"at {now} ran: {runs}", check=True)
    return runs


def _commit_dvc_default(remote):
    check_call(["dvc", "remote", "default", remote])
    git_run(add=[".dvc"], msg=f"update dvc default remote to {remote}", check=True)


def _validate_empty_vc(attempt, prefs=("dvc.", MAIN_MODULE_NAME)):
    for fp in get_git_diffs() + get_git_diffs(True):
        if any([*[fp.startswith(pref) for pref in prefs], fp == BASE_CONF_PATH]):
            msg = f"{fp} should be committed to git before {attempt}"
            raise ProjectSetupException(msg)
