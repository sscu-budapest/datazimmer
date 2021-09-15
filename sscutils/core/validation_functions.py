from collections import Counter

from ..exceptions import DatasetSetupException, ProjectSetupException
from .io import (
    import_subset_creator_function,
    load_project_env_config,
    load_raw_dataset_config,
)


def validate_project_env():
    env_dic = load_project_env_config()
    datasets = load_raw_dataset_config()

    prefix_list = [d["prefix"] for d in datasets]
    duplicated_prefixes = [
        *filter(lambda kv: kv[1] > 1, Counter(prefix_list).items())
    ]
    if duplicated_prefixes:
        raise ProjectSetupException(
            f"Some prefixes appera more than once: {duplicated_prefixes}"
        )

    if set(prefix_list) != set(env_dic.keys()):
        raise ProjectSetupException(
            f"Imported dataset prefixes ({prefix_list}) "
            f"don't mach ones in environment ({env_dic.keys()})"
        )

    possible_subsets = {
        dset["prefix"]: [ss["name"] for ss in dset["subsets"]]
        for dset in datasets
    }

    for prefix, subset_id in env_dic.items():
        if subset_id not in possible_subsets[prefix]:
            raise ProjectSetupException(
                f"Invalid subset ({subset_id}) "
                f"set for dataset prefixed {prefix}. "
                f"Imported ones: {possible_subsets[prefix]}"
            )


def validate_dataset_setup():
    # should assert that metadata is same across all branches
    try:
        import_subset_creator_function()
    except ImportError:
        raise DatasetSetupException("Subset creator function not found")
