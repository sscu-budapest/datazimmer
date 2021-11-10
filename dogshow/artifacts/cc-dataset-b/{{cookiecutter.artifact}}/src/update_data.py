import pandas as pd
import src.namespace_metadata as ns
from colassigner import get_all_cols
from sscutils import dump_dfs_to_tables


def update_data(data_root):

    dogsize_df = pd.read_csv(f"{data_root}/sizes.csv").set_index(
        ns.DogSizeIndex.dogsize_name
    )

    dog_df = pd.read_csv(f"{data_root}/dog2.csv").set_index(
        ns.DogIndex.canine_id
    )

    comp_df = pd.read_csv(f"{data_root}/race.csv").set_index(
        ns.CompetitionIndex.competition_id
    ).astype({ns.CompetitionFeatures.held_date: "datetime64"})

    dotm_df = pd.read_csv(f"{data_root}/dog_of_the_month.csv").set_index(
        get_all_cols(ns.DogOfTheMonthIndex)
    )

    dump_dfs_to_tables(
        None,
        [
            (dogsize_df, ns.dog_size_table),
            (dog_df, ns.dog_table),
            (comp_df, ns.competition_table),
            (dotm_df, ns.dog_of_the_month_table),
        ],
    )
