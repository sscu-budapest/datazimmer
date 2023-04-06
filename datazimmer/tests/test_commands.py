from pathlib import Path

import pytest

import datazimmer.typer_commands as tc
from datazimmer.exceptions import ProjectSetupException
from datazimmer.naming import DATA_PATH, DEFAULT_ENV_NAME, MAIN_MODULE_NAME
from datazimmer.typer_commands import _validate_empty_vc

from .util import run_in_process


def test_around(in_template, proper_env):
    # ./dogshow/minimal.py
    # as core

    run_in_process(tc.build_meta)
    # assert not (DATA_PATH / "test-project" / "core" / DEFAULT_ENV_NAME).exists()
    run_in_process(tc.run, commit=True)
    assert (DATA_PATH / "test-project" / "core" / DEFAULT_ENV_NAME).exists()
    run_in_process(tc.update)


def test_vc_validation(in_template, proper_env):
    Path(MAIN_MODULE_NAME, "other.py").write_text("a = 10")
    with pytest.raises(ProjectSetupException):
        _validate_empty_vc("err")
