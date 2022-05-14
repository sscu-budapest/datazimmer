from itertools import chain
from typing import Type

import metazimmer.dogracebase.core as doglast
import metazimmer.dogshowbase.core.ns_meta as dogfirst
import numpy as np
import pandas as pd
from metazimmer.dogracebase.core import SizedDog

import datazimmer as dz


def fit_to_limit(series, df_w_limits, LimitFeat: Type[doglast.IntLimitType]):
    out = pd.Series(dtype=df_w_limits.index.dtype, index=series.index)
    for cat, catrow in df_w_limits.iterrows():
        # the ~ bc of nan's
        out.loc[
            ~(series < catrow[LimitFeat.min]) & ~(series > catrow[LimitFeat.max])
        ] = cat
    return out


class Status(dz.AbstractEntity):
    status_name = dz.Index & str
    wins = doglast.IntLimitType


status_table = dz.ScruTable(Status)
sized_dog_table = dz.ScruTable(SizedDog)

status_md = dz.ReportFile("status_table.md")


@dz.register(
    dependencies=[
        dogfirst.competition_table,
        dogfirst.dog_table,
        doglast.dog_size_table,
    ],
    outputs=[
        status_table,
        sized_dog_table,
    ],
    outputs_nocache=[status_md],
    cron="0 1 13 * 5",
)
def proc(top_status_multiplier: int):

    limits = {
        "novice": [0, 0.5],
        "up_and_coming": [0.4, 0.7],
        "goodboy": [0.7, 0.9],
        "legend": [0.9, 1],
    }
    ends = np.unique([*chain(*limits.values())])

    # one way to fill a table
    # using featuresbase class for naming
    comp_df = dogfirst.competition_table.get_full_df()
    win_count = comp_df.groupby(dogfirst.Competition.winner.pet.cid)[
        dogfirst.Competition.prize_pool
    ].count()
    q_arr = np.quantile(win_count, sorted(ends))
    q_arr[-1] = q_arr[-1] * top_status_multiplier
    q_map = dict(zip(ends, q_arr.astype(int)))
    status_df = pd.DataFrame(
        [
            {
                Status.status_name: lname,
                Status.wins.min: q_map[lminq],
                Status.wins.max: q_map[lmaxq],
            }
            for lname, (lminq, lmaxq) in limits.items()
        ]
    )
    status_table.replace_all(status_df)
    status_md.write_text(status_df.to_markdown())

    dog1_df = dogfirst.dog_table.get_full_df()
    dog1_size_cat = fit_to_limit(
        dog1_df[dogfirst.Dog.waist],
        doglast.dog_size_table.get_full_df(),
        doglast.DogSize.waist_limit,
    )

    sized_dog_table.replace_all(
        dog1_df.assign(
            **{SizedDog.size.dogsize_name: dog1_size_cat, SizedDog.color: None}
        )
    )
