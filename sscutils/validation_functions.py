import re

from .config_loading import DatasetConfig, ProjectConfig
from .exceptions import DatasetSetupException
from .helpers import import_env_creator_function, import_update_data_function
from .metadata.inscript_converters import (
    PyObjectCollector,
    dataset_ns_metadata_abs_module,
    load_metadata_from_dataset_script,
)
from .metadata.io import load_from_yaml


def validate_project_env():
    """asserts a few things about a dataset

    - all prefixes in envs have imported namespaces
    - configuration files are present
    - metadata is same across all branches
    - metadata fits what is in the data files
    - one step per module

    Raises
    ------
    ProjectSetupException
        explains what is wrong
    """
    _ = ProjectConfig()


def validate_dataset_setup():
    """asserts a few things about a dataset

    - metadata is properly exported
    - configuration files are present
    - metadata is same across all branches
    - metadata fits what is in the data files

    Raises
    ------
    DatasetSetupException
        explains what is wrong
    """
    _ = DatasetConfig()
    from_script_meta = load_metadata_from_dataset_script()
    serialized_meta = load_from_yaml()
    py_obj_repo = PyObjectCollector(dataset_ns_metadata_abs_module)

    ser_t_names = [t.name for t in serialized_meta.tables]
    loaded_t_names = [t.name for t in from_script_meta.tables]

    for scrutable in py_obj_repo.tables:
        if scrutable.name not in ser_t_names:
            raise DatasetSetupException(f"{scrutable} metadata not serialized")
        if scrutable.name not in loaded_t_names:
            raise DatasetSetupException(f"{scrutable} can't be loaded")

    import_env_creator_function()
    import_update_data_function()


def _check_match(bc, s):
    rex = r"[a-z]+((?!{bc}{bc})[a-z|\{bc}])*[a-z]+".format(bc=bc)
    if re.compile(rex).fullmatch(s) is None:
        raise NameError(
            f"{s} does not fit the expected format of "
            f"lower case letters and non-duplicated {bc}"
        )


def validate_step_name(s):
    _check_match("_", s)


def validate_repo_name(s):
    _check_match("-", s)
