from pathlib import Path
from subprocess import check_call
from tempfile import TemporaryDirectory

import pytest
from invoke import Context

import datazimmer.tests as testmod
from datazimmer.config_loading import RunConfig
from datazimmer.invoke_commands import build_meta, cleanup
from datazimmer.naming import DEFAULT_ENV_NAME, MAIN_MODULE_NAME, TEMPLATE_REPO
from datazimmer.utils import cd_into, reset_meta_module
from datazimmer.get_runtime import get_runtime

CORE_PY = Path(testmod.__file__).parent / "core_of_test_artifact.py"


def pytest_addoption(parser):
    # test / explore / live
    parser.addoption("--mode", action="store", default="test")


@pytest.fixture(scope="session")
def empty_template():
    ctx = Context()
    with TemporaryDirectory() as tmpdir:
        check_call(["git", "clone", TEMPLATE_REPO, tmpdir])
        with cd_into(tmpdir):
            check_call(["dvc", "remote", "add", f"testrem", "/nothing"])
            check_call(["dvc", "remote", "default", "testrem"])
            Path(MAIN_MODULE_NAME, "core.py").write_text(CORE_PY.read_text())
            build_meta(ctx)
        yield tmpdir
        with cd_into(tmpdir):
            cleanup(ctx)


@pytest.fixture
def in_template(empty_template):
    with cd_into(empty_template):
        yield


@pytest.fixture
def running_template(in_template):
    with RunConfig(write_env=DEFAULT_ENV_NAME, read_env=DEFAULT_ENV_NAME):
        reset_meta_module()
        get_runtime(reset=True)
        yield
