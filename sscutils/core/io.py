import importlib
from typing import List

import yaml

from ..constants import (
    DVC_DEFAULT_REMOTES_CONFIG_PATH,
    ENVIRONMENT_CONFIG_PATH,
    IMPORTED_DATASETS_CONFIG_PATH,
    SRC_PATH,
    SUBSET_CONFIG_PATH,
)
from .subset_management import Subset, SubsetToImport

# DATASET IO FUNCTIONS


def import_subset_creator_function():
    cs_module = importlib.import_module(f"{SRC_PATH}.create_subsets")
    return cs_module.create_subsets


def load_created_subsets() -> List[Subset]:
    path = SUBSET_CONFIG_PATH
    return [
        Subset(k, **v) for k, v in yaml.safe_load(path.read_text()).items()
    ]


def load_default_subset():
    sss = load_created_subsets()
    for ss in sss:
        if ss.default:
            return ss
    return sss[0]


def dump_subsets_to_config(subsets):
    SUBSET_CONFIG_PATH.write_text(
        yaml.safe_dump({ss.name: ss.to_dict() for ss in subsets})
    )


def dump_dfs_to_trepos(env_name, df_trepo_pairs):
    default_env_name = load_default_subset().name
    for df, trepo in df_trepo_pairs:
        if env_name is not None:
            trepo.set_env(env_name)
        trepo.replace_all(df)
        trepo.set_env(default_env_name)


# RESEARCH PROJECT IO FUNCIONS


def load_raw_dataset_config():
    return yaml.safe_load(IMPORTED_DATASETS_CONFIG_PATH.read_text()) or []


def load_imported_datasets(current_env_only=False) -> List[SubsetToImport]:
    subsets = []
    for dset_repo in load_raw_dataset_config():
        ss_list = dset_repo.pop("subsets")
        for ss in ss_list:
            subset_name = ss.pop("name")
            ss["subset"] = subset_name
            if current_env_only and subset_name != load_subset_from_env(
                dset_repo["prefix"]
            ):
                continue
            subsets.append(SubsetToImport(**dset_repo, **ss))
    return subsets


def load_subset_names_of_prefix(prefix):
    return [
        ss.subset for ss in load_imported_datasets() if ss.prefix == prefix
    ]


def load_project_env_config():
    return yaml.safe_load(ENVIRONMENT_CONFIG_PATH.read_text())


def load_subset_from_env(prefix: str):
    return load_project_env_config()[prefix]


# COMMON IO FUNCTIONS


def load_branch_remote_pairs():
    try:
        return yaml.safe_load(
            DVC_DEFAULT_REMOTES_CONFIG_PATH.read_text()
        ).items()
    except FileNotFoundError:
        return []
