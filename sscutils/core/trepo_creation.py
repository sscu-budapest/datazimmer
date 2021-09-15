from parquetranger import TableRepo

from .io import (
    load_created_subsets,
    load_default_subset,
    load_subset_from_env,
    load_subset_names_of_prefix,
)
from .path_functions import get_subset_path


def create_trepo_with_subsets(
    filename: str,
    group_cols=None,
    max_records=0,
    prefix=None,
    no_subsets: bool = False,
) -> TableRepo:
    """create a TableRepo

    should work for dataset and project equally

    `prefix`: set if and only if within a project - not a dataset
    `no_subsets`: set to True for intermediate output of a project
    """

    if prefix is None:
        ss_names = [ss.name for ss in load_created_subsets()]
        default_ss_name = load_default_subset().name
    elif no_subsets:
        ss_names = [prefix]
        default_ss_name = prefix
        prefix = None
    else:
        ss_names = load_subset_names_of_prefix(prefix)
        default_ss_name = load_subset_from_env(prefix)

    env_dict = {
        ss_name: get_subset_path(ss_name, prefix) for ss_name in ss_names
    }
    trepo_path = env_dict[default_ss_name] / filename

    return TableRepo(
        trepo_path,
        group_cols=group_cols,
        max_records=max_records,
        env_parents=env_dict,
    )
