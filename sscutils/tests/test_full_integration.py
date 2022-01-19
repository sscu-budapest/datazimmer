import os
from contextlib import contextmanager
from pathlib import Path

import pytest
from invoke import Context

from sscutils.artifact_context import ArtifactContext
from sscutils.exceptions import DatasetSetupException, ProjectSetupException
from sscutils.helpers import import_pipereg
from sscutils.invoke_commands import (
    import_namespaces,
    lint,
    load_external_data,
    push_envs,
    run,
    serialize_datascript_metadata,
    set_dvc_remotes,
    update_data,
    validate,
    write_envs,
)
from sscutils.naming import (
    DATASET_METADATA_PATHS,
    ENV_CREATION_MODULE_NAME,
    SRC_PATH,
    ProjectConfigPaths,
)
from sscutils.tests.create_dogshow import csv_path, package_root
from sscutils.utils import cd_into, reset_src_module
from sscutils.validation_functions import sql_validation, validate_dataset

from .init_dogshow import setup_dogshow


def test_full_dogshow(tmp_path: Path, pytestconfig):
    # TODO: turn this into documentation

    mode = pytestconfig.getoption("mode")
    ds_cc = setup_dogshow(mode, tmp_path)

    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"

    c = Context()
    for ds in [ds_cc.dataset_a, ds_cc.dataset_b]:
        run_ds_test(ds, c)

    with ds_cc.project_a as validator:
        run_project_a_test(c, validator, ds_cc, constr)

    with ds_cc.project_b as validator:
        import_namespaces(c, git_commit=True)
        import_namespaces(c, git_commit=False)  # test repeating import
        import_namespaces(c, git_commit=False, overwrite=False)
        load_external_data(c, git_commit=True)
        validator()
        env_path = ProjectConfigPaths.CURRENT_ENV
        with _move_file(c, env_path):
            env_path.write_text(r"a: {env: b}")
            with pytest.raises(ProjectSetupException):
                ArtifactContext()

    with cd_into(ds_cc.get_git_remote("dataset-a"), force_clone=True):
        run_wrong_ds_a_test(c)


@contextmanager
def _move_file(c, file_path):
    tmp_name = "_tmp.py"
    c.run(f"mv {file_path} {tmp_name}")
    reset_src_module()
    yield
    c.run(f"mv -f {tmp_name} {file_path}")


def run_ds_test(ds_context, c):
    env_fun_script = SRC_PATH / (ENV_CREATION_MODULE_NAME + ".py")
    with ds_context as validator:
        import_namespaces(c, git_commit=True)
        lint(c)
        with pytest.raises(DatasetSetupException):
            validate_dataset()
        serialize_datascript_metadata(c, git_commit=True)
        update_data(c, (csv_path,))
        set_dvc_remotes(c)
        write_envs(c)
        push_envs(c, git_push=True)
        validator()
        sql_validation(
            "postgresql://postgres:postgres@localhost:5432/postgres", draw=True
        )
        validate(c)
        with _move_file(c, env_fun_script):
            with pytest.raises(DatasetSetupException):
                validate_dataset()


def run_wrong_ds_a_test(c):
    c.run("dvc pull")
    validate(c, env="top_comps")
    _spath = DATASET_METADATA_PATHS.table_schemas
    bad_str = _spath.read_text().replace(
        "name: prize_pool", "name: bad_prize_pool"
    )
    _spath.write_text(bad_str)
    with pytest.raises(DatasetSetupException):
        validate(c)


def run_project_a_test(c, validator, ds_context, constr):
    c.run(f"pip install {package_root}")
    c.run("dvc cache dir ../cache-dir")
    import_namespaces(c, git_commit=True)
    load_external_data(c, git_commit=True)
    set_dvc_remotes(c)
    serialize_datascript_metadata(c, git_commit=True)
    run(c, profile=True)
    c.run('git add reports metadata data dvc.yaml;git commit -m "ran steps"')
    c.run("git push; dvc push")
    ds_context.modify_project_a()
    run(c)
    validator()
    validate(c)
    sql_validation(constr, draw=True)
    for s in import_pipereg().steps:
        s.run()  # for coverage
