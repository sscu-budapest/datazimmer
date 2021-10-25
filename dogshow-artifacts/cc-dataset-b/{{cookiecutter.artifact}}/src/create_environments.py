from sscutils import dump_dfs_to_tables

from . import namespace_metadata as ns


def create_environments(env_name, dog_sizes):

    dog_size_set = set(dog_sizes)
    dogsize_df = ns.dog_size_table.get_full_df().loc[dog_sizes, :]

    dog_df = ns.dog_table.get_full_df().loc[
        lambda df: df[ns.DogFeatures.size].isin(dog_size_set), :
    ]

    comp_df = ns.competition_table.get_full_df().loc[
        lambda df: df[ns.CompetitionFeatures.champion].isin(dog_df.index), :
    ]

    dotm_df = ns.dog_of_the_month_table.get_full_df().loc[
        lambda df: df[ns.DogOfTheMonthFeatures.winner].isin(dog_df.index), :
    ]

    dump_dfs_to_tables(
        env_name,
        [
            (dogsize_df, ns.dog_size_table),
            (dog_df, ns.dog_table),
            (comp_df, ns.competition_table),
            (dotm_df, ns.dog_of_the_month_table),
        ],
    )
