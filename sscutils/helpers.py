import importlib
from typing import TYPE_CHECKING

from parquetranger import TableRepo

from .config_loading import DatasetConfig, load_artifact_config
from .exceptions import DatasetSetupException, ProjectSetupException
from .naming import (
    DATA_PATH,
    DATASET_METADATA_PATHS,
    ENV_CREATION_FUNCTION_NAME,
    ENV_CREATION_MODULE_NAME,
    METADATA_DIR,
    PIPEREG_INSTANCE_NAME,
    PIPEREG_MODULE_NAME,
    SRC_PATH,
    UPDATE_DATA_FUNCTION_NAME,
    UPDATE_DATA_MODULE_NAME,
    imported_namespaces_abs_module,
    ns_metadata_abs_module,
    ns_metadata_file,
)

if TYPE_CHECKING:
    from .pipeline_registry import PipelineRegistry  # pragma: no cover


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


def import_pipereg() -> "PipelineRegistry":
    return _import_fun(
        PIPEREG_MODULE_NAME, PIPEREG_INSTANCE_NAME, ProjectSetupException
    )


def run_step(step):
    pipereg = import_pipereg()
    pipereg.get_step(step).run()


def get_all_child_modules():
    try:
        pipereg = import_pipereg()
        steps = pipereg.steps
    except ProjectSetupException:
        steps = []

    out = [step.runner.__module__ for step in steps]
    if ns_metadata_file.exists():
        out.append(ns_metadata_abs_module)

    return out


def get_top_module_name(child_module_name: str):
    mod_pref = f"{SRC_PATH}."
    module_ind = 1
    if not child_module_name.startswith(mod_pref):
        raise ValueError(
            f"Can't detect top module from module {child_module_name}"
        )
    elif child_module_name == ns_metadata_abs_module:
        return ""
    elif child_module_name.startswith(imported_namespaces_abs_module):
        module_ind = 2
    return child_module_name.split(".")[module_ind]


def get_associated_step(caller):
    return get_top_module_name(caller.__module__)


def get_serialized_namespace_dirs():
    subdirs = [p.name for p in METADATA_DIR.iterdir() if p.is_dir()]
    if DATASET_METADATA_PATHS.entity_classes.exists():
        subdirs.append("")
    return subdirs


def create_trepo(
    name, namespace, partitioning_cols=None, max_partition_size=None
):

    artifact_config = load_artifact_config()
    is_in_dataset = isinstance(artifact_config, DatasetConfig)

    if is_in_dataset:
        parents_dict = {
            env.name: env.path for env in artifact_config.created_environments
        }
        trepo_path = parents_dict[artifact_config.default_env.name] / name
    else:
        parents_dict = {}
        trepo_path = DATA_PATH / namespace / name

    return TableRepo(
        trepo_path,
        group_cols=partitioning_cols,
        max_records=max_partition_size or 0,
        env_parents=parents_dict,
    )


def _import_fun(module_name, fun_name, err=DatasetSetupException):
    full_module = f"{SRC_PATH}.{module_name}"

    try:
        cs_module = importlib.import_module(full_module)
        return getattr(cs_module, fun_name)
    except (ModuleNotFoundError, AttributeError):
        raise err(f"Couldnt find {fun_name} in {full_module}")
