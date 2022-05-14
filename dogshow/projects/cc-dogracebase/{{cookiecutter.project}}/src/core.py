from datetime import datetime

import pandas as pd
from metazimmer.dogshowbase.core import ns_meta

import datazimmer as dz

# TODO: self referencing!!


class IntLimitType(dz.CompositeTypeBase):
    min = int
    max = int


class DogCategory(dz.CompositeTypeBase):
    pure = bool
    neutered = bool


class DogSize(dz.AbstractEntity):
    dogsize_name = dz.Index & str
    waist_limit = IntLimitType
    weight_limit = IntLimitType


class SizedDog(ns_meta.Creature, ns_meta.Pet):
    color = dz.Nullable(str)
    size = DogSize


class Competition(dz.AbstractEntity):
    competition_id = dz.Index & str

    held_date = datetime
    fastest_time = float
    champion = SizedDog


class DogOfTheMonth(dz.AbstractEntity):
    dog_type = dz.Index & DogCategory
    year = dz.Index & int
    month = dz.Index & int

    winner = SizedDog


dog_size_table = dz.ScruTable(DogSize)
dog_table = dz.ScruTable(SizedDog)
competition_table = dz.ScruTable(Competition)
dog_of_the_month_table = dz.ScruTable(DogOfTheMonth, max_partition_size=3)


@dz.register_data_loader
def create_data(data_root):

    dogsize_df = pd.read_csv(f"{data_root}/sizes.csv")
    dog_df = pd.read_csv(f"{data_root}/dog2.csv")
    comp_df = (
        pd.read_csv(f"{data_root}/race.csv")
        .set_index(Competition.competition_id)
        .astype({Competition.held_date: "datetime64"})
    )
    dotm_df = pd.read_csv(f"{data_root}/dog_of_the_month.csv")
    dz.dump_dfs_to_tables(
        [
            (dogsize_df, dog_size_table),
            (dog_df, dog_table),
            (comp_df, competition_table),
            (dotm_df, dog_of_the_month_table),
        ],
    )


@dz.register_env_creator
def create_environments(dog_sizes):

    dog_size_set = set(dog_sizes)
    dogsize_df = dog_size_table.get_full_df().loc[dog_sizes, :]
    dog_df = dog_table.get_full_df().loc[
        lambda df: df[SizedDog.size.dogsize_name].isin(dog_size_set), :
    ]
    comp_df = competition_table.get_full_df().loc[
        lambda df: df[Competition.champion.cid].isin(dog_df.index), :
    ]
    dotm_df = dog_of_the_month_table.get_full_df().loc[
        lambda df: df[DogOfTheMonth.winner.cid].isin(dog_df.index), :
    ]

    dz.dump_dfs_to_tables(
        [
            (dogsize_df, dog_size_table),
            (dog_df, dog_table),
            (comp_df, competition_table),
            (dotm_df, dog_of_the_month_table),
        ],
    )
