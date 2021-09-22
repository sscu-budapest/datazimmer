import pytest

from sscutils.core.validation_functions import (
    validate_dataset_setup,
    validate_project_env,
    validate_repo_name,
    validate_step_name,
)
from sscutils.tests.utils import TemporaryDataset, TemporaryProject


def test_dataset_validation(tmp_path):
    dspath = tmp_path / "dst"
    with TemporaryDataset(dspath):
        validate_dataset_setup()


def test_project_validation(tmp_path):
    dspath = tmp_path / "dst"
    with TemporaryProject(dspath, external_dvc_repos=["r1", "r2"]):
        validate_project_env()


@pytest.mark.parametrize(
    "fname,is_valid",
    [
        ("abc", True),
        ("abc123", False),
        ("abc-a", True),
        ("abc--a", False),
        ("-abc", False),
        ("abc-", False),
    ],
)
def test_repo_name_valid(fname, is_valid):
    if is_valid:
        validate_repo_name(fname)
    else:
        with pytest.raises(NameError):
            validate_repo_name(fname)


@pytest.mark.parametrize(
    "fname,is_valid",
    [
        ("abc", True),
        ("abc123", False),
        ("abc_a", True),
        ("abc__a", False),
        ("_abc", False),
        ("abc_", False),
    ],
)
def test_step_name_valid(fname, is_valid):
    if is_valid:
        validate_step_name(fname)
    else:
        with pytest.raises(NameError):
            validate_step_name(fname)
