import os
from pathlib import Path

from invoke import Context

from sscutils.config_loading import Config
from sscutils.invoke_commands import build_meta, lint, load_external_data, release
from sscutils.utils import reset_meta_module

from .create_dogshow import modify_to_version
from .init_dogshow import setup_dogshow


def test_full_dogshow(tmp_path: Path, pytestconfig):
    # TODO: turn this into documentation

    mode = pytestconfig.getoption("mode")
    ds_cc = setup_dogshow(mode, tmp_path)

    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"
    c = Context()
    for ds in ds_cc.all_contexts:
        run_artifact_test(ds, c, constr)

    ds_cc.check_sdist_checksums()
    # TODO: cleanup


def run_artifact_test(dog_context, c, constr):
    reset_meta_module()
    with dog_context as versions:
        init_version = Config.load().version
        lint(c)
        build_meta(c)
        load_external_data(c, git_commit=True)
        release(c, constr)
        for testv in versions:
            modified = modify_to_version(testv)
            lint(c)
            build_meta(c)
            if modified and testv == init_version:
                # TODO: don't allow publishing with same version
                pass
            release(c, constr)
        # release(c)  # TODO: some should work with sqlite
