import pytest

from datazimmer.config_loading import Config
from datazimmer.exceptions import ProjectSetupException


def test_missing_config():
    with pytest.raises(ProjectSetupException):
        Config.load()


def test_project_config(in_template):
    conf = Config.load()

    with pytest.raises(KeyError):
        conf.get_env("nothing")
