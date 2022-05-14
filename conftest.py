import sys
from pathlib import Path
from subprocess import check_call
from tempfile import TemporaryDirectory

import pytest

from datazimmer.config_loading import RunConfig
from datazimmer.get_runtime import get_runtime
from datazimmer.naming import DEFAULT_ENV_NAME, MAIN_MODULE_NAME, TEMPLATE_REPO
from datazimmer.tests.create_dogshow import dogshow_root
from datazimmer.typer_commands import cleanup
from datazimmer.utils import cd_into, gen_rmtree, reset_meta_module

CORE_PY = dogshow_root / "minimal.py"


def pytest_addoption(parser):
    # test / explore / live
    parser.addoption("--mode", action="store", default="test")


@pytest.fixture(scope="session")
def empty_template():
    tmpdir = TemporaryDirectory().name
    check_call(["git", "clone", TEMPLATE_REPO, tmpdir])
    with cd_into(tmpdir):
        check_call(["dvc", "remote", "add", "testrem", "/nothing"])
        check_call(["dvc", "remote", "default", "testrem"])
        Path(MAIN_MODULE_NAME, "core.py").write_text(CORE_PY.read_text())
    yield tmpdir
    with cd_into(tmpdir):
        cleanup()
    gen_rmtree(tmpdir)


@pytest.fixture
def in_template(empty_template):
    with cd_into(empty_template):
        sys.path.insert(0, empty_template)
        yield
        sys.path.pop(0)


@pytest.fixture
def running_template(in_template):
    _env = DEFAULT_ENV_NAME
    with RunConfig(write_env=_env, read_env=_env, profile=True):
        reset_meta_module()
        get_runtime(reset=True)
        yield
