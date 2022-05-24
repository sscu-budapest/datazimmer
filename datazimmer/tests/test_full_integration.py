import os
from pathlib import Path

from datazimmer.config_loading import Config
from datazimmer.explorer import init_explorer
from datazimmer.metadata.atoms import _GLOBAL_CLS_MAP
from datazimmer.naming import BASE_CONF_PATH, MAIN_MODULE_NAME
from datazimmer.typer_commands import (
    build_explorer,
    build_meta,
    cleanup,
    draw,
    load_external_data,
    publish_data,
    publish_meta,
    run,
    run_cronjobs,
    update,
    validate,
)
from datazimmer.utils import cd_into, get_git_diffs, git_run, reset_meta_module

from .create_dogshow import DogshowContextCreator, modify_to_version


def test_full_dogshow(tmp_path: Path, pytestconfig):
    # TODO: turn this into documentation
    mode = pytestconfig.getoption("mode")
    ds_cc = DogshowContextCreator.load(mode, tmp_path)

    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    if pg_host == "sqlite":  # pragma: no cover
        constr = "sqlite:///_db.sqlite"
    else:
        constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"
    try:
        for ds in ds_cc.all_contexts:
            run_project_test(ds, constr)
        ds_cc.check_sdists()
        with ds_cc.explorer():
            init_explorer()
            build_explorer()
    finally:
        for ran_dir in ds_cc.ran_dirs:
            with cd_into(ran_dir):
                cleanup()


def run_project_test(dog_context, constr):
    reset_meta_module()
    with dog_context as (versions, crons):
        init_version = Config.load().version
        _complete(constr)
        for testv in versions:
            [_GLOBAL_CLS_MAP.pop(k) for k in [*_GLOBAL_CLS_MAP.keys()]]
            modify_to_version(testv)
            if testv == init_version:
                # should warn and just try install
                build_meta()
                continue
            _complete(constr)
            build_meta()
        for cronexpr in crons:
            # TODO: warn if same data is tagged differently
            run_cronjobs(cronexpr)
            validate()
            publish_data(validate=True)


def _complete(constr):
    draw()
    build_meta()
    git_run(add=[MAIN_MODULE_NAME, BASE_CONF_PATH])
    get_git_diffs(True) and git_run(msg="build")
    load_external_data(git_commit=True)
    run(commit=True)
    validate(constr)
    publish_meta()
    reset_meta_module()
    publish_data()
    update()
    reset_meta_module()
