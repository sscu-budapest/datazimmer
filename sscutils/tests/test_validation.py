import pytest

from sscutils.artifact_context import ArtifactContext
from sscutils.exceptions import NotAnArtifactException
from sscutils.utils import cd_into
from sscutils.validation_functions import is_repo_name, is_step_name


@pytest.mark.parametrize(
    "repo_name,is_valid",
    [
        ("abc", True),
        ("abc123", False),
        ("abc-a", True),
        ("abc--a", False),
        ("-abc", False),
        ("abc-", False),
    ],
)
def test_repo_name_valid(repo_name, is_valid):
    if is_valid:
        is_repo_name(repo_name)
    else:
        with pytest.raises(NameError):
            is_repo_name(repo_name)


@pytest.mark.parametrize(
    "step_name,is_valid",
    [
        ("abc", True),
        ("abc123", False),
        ("abc_a", True),
        ("abc__a", False),
        ("_abc", False),
        ("abc_", False),
    ],
)
def test_step_name_valid(step_name, is_valid):
    if is_valid:
        is_step_name(step_name)
    else:
        with pytest.raises(NameError):
            is_step_name(step_name)


def test_failing_config(tmp_path):
    with cd_into(tmp_path):
        with pytest.raises(NotAnArtifactException):
            ArtifactContext()


def test_missing_fk(tmp_path):
    # TODO
    pass
