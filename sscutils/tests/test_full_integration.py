import os
from contextlib import contextmanager
from pathlib import Path

import pytest
from invoke import Context

from sscutils.exceptions import DatasetSetupException
from sscutils.helpers import run_step
from sscutils.invoke_commands import (
    import_namespaces,
    lint,
    load_external_data,
    push_envs,
    serialize_datascript_metadata,
    set_dvc_remotes,
    update_data,
    write_envs,
)
from sscutils.naming import ENV_CREATION_MODULE_NAME, SRC_PATH
from sscutils.sql.loader import SqlLoader
from sscutils.tests.create_dogshow import DogshowContextCreator, csv_path
from sscutils.utils import cd_into, reset_src_module
from sscutils.validation_functions import (
    validate_dataset_setup,
    validate_project_env,
)


def sql_validation():
    pg_host = os.environ.get("POSTGRES_HOST", "localhost")

    constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"
    loader = SqlLoader(constr, echo=False)
    loader.setup_schema()
    loader.load_data()
    loader.purge()


def test_full_dogshow(tmp_path: Path):

    lroot = tmp_path / "root-dir"
    lroot.mkdir()
    env_fun_script = SRC_PATH / (ENV_CREATION_MODULE_NAME + ".py")

    ds_cc = DogshowContextCreator(
        local_output_root=lroot,
    )

    c = Context()

    for ds in [ds_cc.dataset_a, ds_cc.dataset_b]:
        with ds as validator:
            import_namespaces(c, git_commit=True)
            lint(c)
            with pytest.raises(DatasetSetupException):
                validate_dataset_setup()
            serialize_datascript_metadata(c, git_commit=True)
            update_data(c, (csv_path,))
            set_dvc_remotes(c)
            write_envs(c)
            push_envs(c, git_push=True)
            validator()
            validate_dataset_setup()
            sql_validation()
            with _move_file(c, env_fun_script):
                with pytest.raises(DatasetSetupException):
                    validate_dataset_setup()

    with ds_cc.project_a as validator:
        import_namespaces(c, git_commit=True)
        load_external_data(c, git_commit=True)
        run_step("success")
        run_step("base_report")
        c.run("git add reports metadata")
        c.run('git commit -m "add step output"')
        c.run("git push")
        validator()
        validate_project_env()

    with ds_cc.project_b as validator:
        import_namespaces(c, git_commit=True)
        # load_external_data(c, git_commit=True)
        # pipereg = import_pipereg()
        validator()

    with cd_into(ds_cc.get_git_remote("dataset-a"), force_clone=True):
        validate_dataset_setup()


@contextmanager
def _move_file(c, file_path):
    tmp_name = "_tmp.py"
    c.run(f"mv {file_path} {tmp_name}")
    reset_src_module()
    yield
    c.run(f"mv {tmp_name} {file_path}")
