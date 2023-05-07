import datetime as dt
import re
from dataclasses import asdict
from itertools import chain
from pathlib import Path
from subprocess import check_call
from typing import TYPE_CHECKING

import typer
from dvc.repo import Repo
from structlog import get_logger

from .config_loading import CONF_KEYS, Config, RunConfig, UserConfig
from .exceptions import ProjectSetupException
from .get_runtime import get_runtime
from .gh_actions import write_aswan_crons, write_project_cron
from .metadata.high_level import ProjectMetadata
from .naming import (
    BASE_CONF_PATH,
    MAIN_MODULE_NAME,
    README_PATH,
    SANDBOX_DIR,
    TEMPLATE_REPO,
    VERSION_PREFIX,
    env_from_tag,
    get_tag,
    meta_version_from_tag,
)
from .raw_data import IMPORTED_RAW_DATA_DIR, RAW_DATA_DIR, RAW_ENV_NAME
from .registry import Registry
from .sql.draw import dump_graph
from .sql.loader import tmp_constr
from .utils import cd_into, command_out_w_prefix, gen_rmtree, get_git_diffs, git_run
from .validation_functions import validate
from .zenodo import CITATION_FILE, ZenApi

if TYPE_CHECKING:  # pragma: no cover
    from .project_runtime import ProjectRuntime

logger = get_logger(ctx="CLI command")
app = typer.Typer()

app.command()(validate)


@app.command()
def run_step(name: str, env: str):
    get_runtime().run_step(name, env)


@app.command()
def init(name: str, github_org: str = "", git_remote: str = ""):
    git_run(clone=(TEMPLATE_REPO, name), depth=None)
    comms = [["git", "remote", "rm", "origin"]]
    if github_org:  # pragma: no cover
        from ghapi.all import GhApi

        api = GhApi()
        api.repos.create_in_org(github_org, name)
        git_remote = f"git@github.com:{github_org}/{name}.git"
    if git_remote:
        comms.extend(
            [
                ["git", "remote", "add", "origin", git_remote],
                ["git", "push", "-u", "origin", "main"],
            ]
        )
    with cd_into(name):
        cstr = re.sub(
            f"{CONF_KEYS.name}: .+",
            f"{CONF_KEYS.name}: {name}",
            BASE_CONF_PATH.read_text(),
        )
        BASE_CONF_PATH.write_text(cstr)
        README_PATH.write_text(README_PATH.read_text().replace("{{title}}", name))
        for c in comms:
            check_call(c)
        git_run(add=["*"], msg=f"init {name}", push=bool(git_remote))


@app.command()
def update(registry: bool = True, code: bool = True, dvc: bool = True):
    if registry:
        Registry(Config.load()).update()
    if code:
        git_run(pull=True)
    if dvc:
        Repo().pull()


@app.command()
def draw(v: bool = False):
    with tmp_constr(v) as constr:
        dump_graph(constr)


@app.command()
def set_whoami(first_name: str, last_name: str, orcid: str):
    UserConfig(first_name, last_name, orcid).dump()


@app.command()
def import_raw(project: str, tag: str = "", commit: bool = False):
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
    if commit:
        _dvc_commit(paths=[f"{IMPORTED_RAW_DATA_DIR}/*"], msg=f"import raw {project}")


@app.command()
def publish_data():
    build_meta()
    # TODO: ensure that this build gets published
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
        _dvc_commit([RAW_DATA_DIR], "save raw data")
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
def deposit_to_zenodo(
    env: str = "",
    publish: bool = False,
    test: bool = False,
    path_filter: str = "",
    private: bool = False,
    key_path: str = "",
):
    """path_filter: regex to filter paths to be uploaded
    key_path: used if private=True, a path to a fernet key file in hex text"""
    # must be at a zimmer tag
    if private and not key_path:
        raise ValueError("if upload is private, a fernet key path is needed")
    _validate_empty_vc("publishing to zenodo")
    runtime = get_runtime()
    tag_env, tag = _get_current_tag_of_env(env)
    if tag is None:
        if not env:
            raise ProjectSetupException(
                "cant publish untagged commit. "
                f"either checkout a published tag or run {publish_data}"
            )
        else:
            raise ValueError(f"can't find {env} among tags of HEAD")

    zapi = ZenApi(runtime.config, private=private, test=test, tag=tag)
    if tag_env == RAW_ENV_NAME:
        paths_to_upload = [RAW_DATA_DIR]
    else:
        paths_to_upload = _iter_dvc_paths(runtime, tag_env)
    for path in paths_to_upload:
        if (path_filter != "") and (not re.findall(path_filter, path.as_posix())):
            continue
        zapi.upload(path, key_path if private else None)
    if publish:
        zapi.publish()
        zapi.update_readme()
        git_run(add=[README_PATH, CITATION_FILE], msg=f"doi for {tag}", check=True)
    else:
        logger.info(f"{zapi.url_base}/deposit/{zapi.depo_id}")


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
        _dvc_commit(posixes, "add imported datasets")


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
    stage: bool = True,
    profile: bool = False,
    env: str = None,
    commit: bool = False,
    reset_aswan: bool = False,
):
    # TODO: add validation that all scrutables belong somewhere as an output
    dvc_repo = Repo(config={"core": {"autostage": stage}})
    runtime = get_runtime()
    stage_names = []
    no_cache_outputs = []
    for step in runtime.metadata.complete.pipeline_elements:
        no_cache_outputs.extend(chain(*step.get_no_cache_outs(env)))
        stage_names.extend(step.add_stages(dvc_repo))

    for dvc_stage in dvc_repo.stages:
        if (
            dvc_stage.is_data_source
            or dvc_stage.is_import
            or (dvc_stage.name in stage_names)
        ):
            continue
        logger.info("removing dvc stage", stage=dvc_stage.name)
        dvc_repo.remove(dvc_stage.name)
        dvc_repo.lock.lock()
        dvc_stage.remove_outs(force=True)
        dvc_repo.lock.unlock()
    if not stage_names:
        return
    targets = runtime.step_names_of_env(env) if env else None
    rconf = RunConfig(profile=profile, reset_aswan=reset_aswan)
    with rconf:
        logger.info("running repro", targets=targets, **asdict(rconf))
        runs = dvc_repo.reproduce(targets=targets, pull=True)
    git_run(add=["dvc.yaml", "dvc.lock", BASE_CONF_PATH, *no_cache_outputs])
    if commit:
        now = dt.datetime.now().isoformat(" ", "minutes")
        git_run(msg=f"at {now} ran: {runs}", check=True)
    return runs


def _iter_dvc_paths(runtime: "ProjectRuntime", env):
    dvc_repo = Repo()
    for step in runtime.step_names_of_env(env):
        for out in list(dvc_repo.stage.collect(step))[0].outs:
            op = Path(out.fs_path)
            yield op.relative_to(Path.cwd()) if op.is_absolute() else op


def _get_current_tag_of_env(env: str):
    tag_comm = ["git", "tag", "--points-at", "HEAD"]
    for tag in command_out_w_prefix(tag_comm, VERSION_PREFIX):
        tag_env = env_from_tag(tag)
        if (env == tag_env) or not env:
            return tag_env, tag
    return None, None


def _commit_dvc_default(remote):
    check_call(["dvc", "remote", "default", remote])
    git_run(add=[".dvc"], msg=f"update dvc default remote to {remote}", check=True)


def _dvc_commit(paths, msg):
    git_run(add=["*.gitignore", *[f"{p}.dvc" for p in paths]], msg=msg, check=True)


def _validate_empty_vc(attempt, prefs=("dvc.", MAIN_MODULE_NAME)):
    # TODO: in case of raw data, might need to check notebooks
    for fp in get_git_diffs() + get_git_diffs(True) + get_git_diffs(untracked=True):
        if any([*[fp.startswith(pref) for pref in prefs], fp == BASE_CONF_PATH]):
            msg = f"{fp} should be committed to git before {attempt}"
            raise ProjectSetupException(msg)
