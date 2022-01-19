import importlib
import sys
from typing import TYPE_CHECKING

from .exceptions import DatasetSetupException, ProjectSetupException
from .naming import (
    DATASET_METADATA_PATHS,
    ENV_CREATION_FUNCTION_NAME,
    ENV_CREATION_MODULE_NAME,
    METADATA_DIR,
    PIPEREG_INSTANCE_NAME,
    PIPEREG_MODULE_NAME,
    ROOT_NS_LOCAL_NAME,
    SRC_PATH,
    UPDATE_DATA_FUNCTION_NAME,
    UPDATE_DATA_MODULE_NAME,
    imported_namespaces_abs_module,
    ns_metadata_abs_module,
    ns_metadata_file,
)

if TYPE_CHECKING:
    from .pipeline_registry import PipelineRegistry  # pragma: no cover


def run_step(pipereg: "PipelineRegistry"):
    pipereg.get_step(sys.argv[1]).run()


def import_env_creator_function():
    return _import_fun(ENV_CREATION_MODULE_NAME, ENV_CREATION_FUNCTION_NAME)


def import_update_data_function():
    return _import_fun(UPDATE_DATA_MODULE_NAME, UPDATE_DATA_FUNCTION_NAME)


def import_pipereg() -> "PipelineRegistry":
    return _import_fun(
        PIPEREG_MODULE_NAME, PIPEREG_INSTANCE_NAME, ProjectSetupException
    )


def get_all_child_modules():
    try:
        pipereg = import_pipereg()
        steps = pipereg.steps
    except ProjectSetupException:
        steps = []

    out = [step.child_module for step in steps]
    if ns_metadata_file.exists():
        out.append(ns_metadata_abs_module)

    return out


def get_all_top_modules():
    return map(get_top_module_name, get_all_child_modules())


def get_top_module_name(child_module_name: str):
    mod_pref = f"{SRC_PATH}."
    module_ind = 1
    if not child_module_name.startswith(mod_pref):
        raise ValueError(
            f"Can't detect top module from module {child_module_name}"
        )
    elif child_module_name == ns_metadata_abs_module:
        return ROOT_NS_LOCAL_NAME
    elif child_module_name.startswith(imported_namespaces_abs_module):
        module_ind = 2
    return child_module_name.split(".")[module_ind]


def get_associated_step(caller):
    return get_top_module_name(caller.__module__)


def get_serialized_namespace_dirs():
    subdirs = [p.name for p in METADATA_DIR.iterdir() if p.is_dir()]
    if DATASET_METADATA_PATHS.entity_classes.exists():
        subdirs.append(ROOT_NS_LOCAL_NAME)
    return subdirs


def _import_fun(module_name, fun_name, err=DatasetSetupException):
    full_module = f"{SRC_PATH}.{module_name}"

    try:
        cs_module = importlib.import_module(full_module)
        return getattr(cs_module, fun_name)
    except (ModuleNotFoundError, AttributeError, ImportError) as e:
        raise err(f"Couldnt find {fun_name} in {full_module} - {e}")
