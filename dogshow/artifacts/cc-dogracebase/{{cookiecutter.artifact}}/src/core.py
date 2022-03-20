from datetime import datetime

import pandas as pd
from metazimmer.dogshowbase import core

from sscutils import (
    CompositeTypeBase,
    IndexBase,
    Nullable,
    ScruTable,
    TableFeaturesBase,
    dump_dfs_to_tables,
    register_data_loader,
    register_env_creator,
)


class DogSizeIndex(IndexBase):
    dogsize_name = str


class DogIndex(IndexBase):
    canine_id = str


class CompetitionIndex(IndexBase):
    competition_id = str


class DogCategory(CompositeTypeBase):
    pure = bool
    neutered = bool


class DogOfTheMonthIndex(IndexBase):
    dog_type = DogCategory
    year = int
    month = int


class IntLimitType(CompositeTypeBase):
    min = int
    max = int


class DogSizeFeatures(TableFeaturesBase):
    waist_limit = IntLimitType
    weight_limit = IntLimitType


class DogFeatures(TableFeaturesBase):
    name = str
    color = Nullable(str)
    size = DogSizeIndex


class CompetitionFeatures(TableFeaturesBase):
    held_date = datetime
    fastest_time = float
    champion = DogIndex


class DogOfTheMonthFeatures(TableFeaturesBase):
    winner = DogIndex


dog_size_table = ScruTable(DogSizeFeatures, DogSizeIndex)
dog_table = ScruTable(DogFeatures, DogIndex, subject_of_records=core.Dog)
competition_table = ScruTable(CompetitionFeatures, CompetitionIndex)
dog_of_the_month_table = ScruTable(
    DogOfTheMonthFeatures, DogOfTheMonthIndex, max_partition_size=3
)


@register_data_loader
def create_data(data_root):

    dogsize_df = pd.read_csv(f"{data_root}/sizes.csv")
    dog_df = pd.read_csv(f"{data_root}/dog2.csv")
    comp_df = (
        pd.read_csv(f"{data_root}/race.csv")
        .set_index(CompetitionIndex.competition_id)
        .astype({CompetitionFeatures.held_date: "datetime64"})
    )
    dotm_df = pd.read_csv(f"{data_root}/dog_of_the_month.csv")
    dump_dfs_to_tables(
        [
            (dogsize_df, dog_size_table),
            (dog_df, dog_table),
            (comp_df, competition_table),
            (dotm_df, dog_of_the_month_table),
        ],
    )


@register_env_creator
def create_environments(dog_sizes):

    dog_size_set = set(dog_sizes)
    dogsize_df = dog_size_table.get_full_df().loc[dog_sizes, :]
    dog_df = dog_table.get_full_df().loc[
        lambda df: df[DogFeatures.size].isin(dog_size_set), :
    ]
    comp_df = competition_table.get_full_df().loc[
        lambda df: df[CompetitionFeatures.champion].isin(dog_df.index), :
    ]
    dotm_df = dog_of_the_month_table.get_full_df().loc[
        lambda df: df[DogOfTheMonthFeatures.winner].isin(dog_df.index), :
    ]

    dump_dfs_to_tables(
        [
            (dogsize_df, dog_size_table),
            (dog_df, dog_table),
            (comp_df, competition_table),
            (dotm_df, dog_of_the_month_table),
        ],
    )
