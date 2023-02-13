import datazimmer.typer_commands as tc
from datazimmer.naming import DATA_PATH, DEFAULT_ENV_NAME

from .util import run_in_process


def test_around(in_template, proper_env):
    # ./dogshow/minimal.py
    # as core

    run_in_process(tc.build_meta)
    # assert not (DATA_PATH / "test-project" / "core" / DEFAULT_ENV_NAME).exists()
    run_in_process(tc.run, commit=True)
    assert (DATA_PATH / "test-project" / "core" / DEFAULT_ENV_NAME).exists()
    run_in_process(tc.update)
