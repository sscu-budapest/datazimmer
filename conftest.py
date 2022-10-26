import os
import sys
from pathlib import Path
from subprocess import check_call
from tempfile import TemporaryDirectory

import pytest
import moto
import boto3

from datazimmer.config_loading import RunConfig
from datazimmer.get_runtime import get_runtime
from datazimmer.naming import (
    AUTH_HEX_ENV_VAR,
    AUTH_PASS_ENV_VAR,
    DEFAULT_ENV_NAME,
    MAIN_MODULE_NAME,
    TEMPLATE_REPO,
)
from datazimmer.tests.create_dogshow import dogshow_root
from datazimmer.typer_commands import cleanup
from datazimmer.utils import cd_into, gen_rmtree, reset_meta_module
from zimmauth import ZimmAuth

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


@pytest.fixture(scope="session")
def test_bucket():

    with moto.mock_s3():
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket="bucket-1")
        conn.create_bucket(Bucket="bucket-2")
        conn.create_bucket(Bucket="bucket-3")
        yield conn


@pytest.fixture
def proper_env():

    my_pw = "ldb-siu"

    dic = {
        "keys": {
            "s3-key-name-1": {"key": "XYZ", "secret": "XXX"},
            "s3-key-name-2": {"key": "AB", "secret": "X", "endpoint": "http://sg.co"},
        },
        "bucket-1": {"key": "s3-key-name-1"},
        "bucket-2": {"key": "s3-key-name-2"},
        "bucket-3": {"key": "s3-key-name-1"},
    }

    os.environ[AUTH_HEX_ENV_VAR] = ZimmAuth.dumps_dict(dic, my_pw)
    os.environ[AUTH_PASS_ENV_VAR] = my_pw
