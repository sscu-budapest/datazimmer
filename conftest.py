import os
import sys
from pathlib import Path
from subprocess import check_call
from tempfile import TemporaryDirectory

import boto3
import moto
import pytest
from aswan.constants import DEFAULT_REMOTE_ENV_VAR, DEPOT_ROOT_ENV_VAR
from aswan.depot.remote import HEX_ENV, PW_ENV
from zimmauth import ZimmAuth
from zimmauth.core import LOCAL_HOST_NAMES_ENV_VAR

from datazimmer.config_loading import RunConfig
from datazimmer.dvc_util import run_dvc
from datazimmer.naming import (
    AUTH_HEX_ENV_VAR,
    AUTH_PASS_ENV_VAR,
    DEFAULT_ENV_NAME,
    MAIN_MODULE_NAME,
    META_MODULE_NAME,
)
from datazimmer.tests.create_dogshow import dogshow_root
from datazimmer.tests.util import dz_ctx
from datazimmer.typer_commands import init, setup_dvc
from datazimmer.utils import cd_into, gen_rmtree

CORE_PY = dogshow_root / "minimal.py"


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    setup_dvc()


def pytest_addoption(parser):
    # test / explore / live
    parser.addoption("--mode", action="store", default="test")


@pytest.fixture(scope="session")
def empty_template():
    _tmp = TemporaryDirectory()
    tmp_dir = Path(_tmp.name)
    dvc_rem = tmp_dir / "dvc-rem"
    pname = "test-project"
    with cd_into(tmp_dir):
        check_call(["git", "init", "remote"])
        init(pname, git_remote=(tmp_dir / "remote").as_posix())
    pdir = tmp_dir / pname
    with cd_into(pdir):
        run_dvc("remote", "add", "testrem", dvc_rem.as_posix())
        run_dvc("remote", "default", "testrem")
        Path(MAIN_MODULE_NAME, "core.py").write_text(CORE_PY.read_text())

    with dz_ctx([pdir]):
        yield pdir
    gen_rmtree(tmp_dir)


@pytest.fixture
def in_template(empty_template: Path):
    with cd_into(empty_template):
        sys.path.insert(0, empty_template.as_posix())
        yield empty_template
        for k in list(sys.modules.keys()):
            if k.startswith(META_MODULE_NAME) or k.startswith(MAIN_MODULE_NAME):
                sys.modules.pop(k)
        sys.path.pop(0)


@pytest.fixture
def running_template(in_template):
    _env = DEFAULT_ENV_NAME
    with RunConfig(write_env=_env, read_env=_env, profile=True):
        yield


@pytest.fixture(scope="session")
def test_bucket():
    _bconf = {"CreateBucketConfiguration": {"LocationConstraint": "us-west-1"}}
    with moto.mock_aws():
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket="bucket-1", **_bconf)
        conn.create_bucket(Bucket="bucket-2", **_bconf)
        conn.create_bucket(Bucket="bucket-3", **_bconf)
        yield conn


@pytest.fixture
def proper_env():
    gpath = Path.home() / ".config" / "dvc" / "config"

    old_conf = None
    if gpath.exists():
        old_conf = gpath.read_text()

    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)
    rem_path, local_path = tmp_path / "aswan-remote", tmp_path / "aswan-local"
    rem_path.mkdir()
    local_path.mkdir()

    _CONN_NAME = "aswan-conn"
    _HOST = "localhost"
    my_pw = "ldb-siu"
    dic = {
        "keys": {
            "s3-key-name-1": {"key": "XYZ", "secret": "XXX"},
            "s3-key-name-2": {"key": "AB", "secret": "X", "endpoint": "http://sg.co"},
        },
        "bucket-1": {"key": "s3-key-name-1"},
        "bucket-2": {"key": "s3-key-name-2"},
        "bucket-3": {"key": "s3-key-name-1"},
        "rsa-keys": {"rand-key": "XYZ"},
        "ssh": {"ssh-name-1": {"host": _HOST, "user": "suzer", "rsa_key": "rand-key"}},
        _CONN_NAME: {"connection": "ssh-name-1", "path": rem_path.as_posix()},
    }
    my_hex = ZimmAuth.dumps_dict(dic, my_pw)

    os.environ[AUTH_HEX_ENV_VAR] = my_hex
    os.environ[AUTH_PASS_ENV_VAR] = my_pw
    os.environ[HEX_ENV] = my_hex
    os.environ[PW_ENV] = my_pw
    os.environ[LOCAL_HOST_NAMES_ENV_VAR] = _HOST
    os.environ[DEFAULT_REMOTE_ENV_VAR] = _CONN_NAME
    os.environ[DEPOT_ROOT_ENV_VAR] = local_path.as_posix()
    yield
    tmp_dir.cleanup()
    if old_conf:
        gpath.write_text(old_conf)
