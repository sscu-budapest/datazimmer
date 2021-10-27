import pytest

from sscutils.validation_functions import (
    validate_repo_name,
    validate_step_name,
)


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
        validate_repo_name(repo_name)
    else:
        with pytest.raises(NameError):
            validate_repo_name(repo_name)


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
        validate_step_name(step_name)
    else:
        with pytest.raises(NameError):
            validate_step_name(step_name)
