import os
from pathlib import Path

import pytest
from invoke import Context

from datazimmer.config_loading import Config
from datazimmer.exceptions import ArtifactSetupException
from datazimmer.invoke_commands import (
    build_meta,
    cleanup,
    lint,
    load_external_data,
    release,
)
from datazimmer.utils import cd_into, reset_meta_module

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
    with dog_context as versions:
        init_version = Config.load().version
        lint(c)
        build_meta(c)
        load_external_data(c, git_commit=True)
        release(c, constr)
        for testv in versions:
            modify_to_version(testv)
            if testv == init_version:
                with pytest.raises(ArtifactSetupException):
                    build_meta(c)
                continue
            lint(c)
            build_meta(c)
            release(c, constr)
        # release(c)  # TODO: some should work with sqlite
