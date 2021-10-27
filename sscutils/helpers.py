import importlib

from .config_loading import DatasetConfig
from .exceptions import DatasetSetupException
from .naming import (
    ENV_CREATION_FUNCTION_NAME,
    ENV_CREATION_MODULE_NAME,
    SRC_PATH,
    UPDATE_DATA_FUNCTION_NAME,
    UPDATE_DATA_MODULE_NAME,
)


def dump_dfs_to_tables(env_name, df_structable_pairs):
    """helper function to fill an env of a dataset"""
    dataset_config = DatasetConfig()
    default_env_name = dataset_config.default_env.name
    for df, structable in df_structable_pairs:
        if env_name is not None:
            structable.trepo.set_env(env_name)
        structable.trepo.replace_all(df)
        structable.trepo.set_env(default_env_name)


def import_env_creator_function():
    return _import_fun(ENV_CREATION_MODULE_NAME, ENV_CREATION_FUNCTION_NAME)


def import_update_data_function():
    return _import_fun(UPDATE_DATA_MODULE_NAME, UPDATE_DATA_FUNCTION_NAME)


def _import_fun(module_name, fun_name):
    full_module = f"{SRC_PATH}.{module_name}"

    try:
        cs_module = importlib.import_module(full_module)
        return getattr(cs_module, fun_name)
    except (ModuleNotFoundError, AttributeError):
        raise DatasetSetupException(
            f"Couldnt find {fun_name} in {full_module}"
        )
