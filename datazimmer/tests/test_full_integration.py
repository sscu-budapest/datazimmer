import os
from pathlib import Path

from invoke import Context

from datazimmer.config_loading import Config
from datazimmer.invoke_commands import (
    build_meta,
    cleanup,
    lint,
    load_external_data,
    publish_data,
    publish_meta,
    run,
    run_cronjobs,
    validate,
)
from datazimmer.naming import BASE_CONF_PATH, MAIN_MODULE_NAME
from datazimmer.utils import cd_into, get_git_diffs, reset_meta_module

from .create_dogshow import modify_to_version
from .init_dogshow import setup_dogshow


def test_full_dogshow(tmp_path: Path, pytestconfig):
    # TODO: turn this into documentation

    mode = pytestconfig.getoption("mode")
    ds_cc = setup_dogshow(mode, tmp_path)

    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"
    c = Context()
    try:
        for ds in ds_cc.all_contexts:
            run_artifact_test(ds, c, constr)
        ds_cc.check_sdist_checksums()
    finally:
        for ran_dir in ds_cc.ran_dirs:
            with cd_into(ran_dir):
                cleanup(c)


def run_artifact_test(dog_context, c, constr):
    reset_meta_module()
    with dog_context as (versions, crons):
        init_version = Config.load().version
        _complete(c, constr)
        for testv in versions:
            modify_to_version(testv)
            if testv == init_version:
                # should warn and just try install
                build_meta(c)
                continue
            _complete(c, constr)
        for cronexpr in crons:
            # TODO: warn if same data is tagged differently
            run_cronjobs(c, cronexpr)
            validate(c)
            publish_data(c, validate=True)


def _complete(c, constr):
    lint(c)
    build_meta(c)
    c.run(f"git add {MAIN_MODULE_NAME} {BASE_CONF_PATH}")
    get_git_diffs(True) and c.run('git commit -m "lint and build"')
    load_external_data(c, git_commit=True)
    run(c, commit=True)
    validate(c, constr)
    publish_meta(c)
    reset_meta_module()
    publish_data(c)
    reset_meta_module()
