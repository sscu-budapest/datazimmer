from sscutils.core.validation_functions import (
    validate_dataset_setup,
    validate_project_env,
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
