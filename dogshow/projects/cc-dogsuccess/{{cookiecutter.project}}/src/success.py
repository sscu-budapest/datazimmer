from itertools import chain
from typing import Type

import metazimmer.dogracebase.core as doglast
import metazimmer.dogshowbase.core as dogfirst
import numpy as np
import pandas as pd
from colassigner import Col

import datazimmer as dz


def fit_to_limit(
    series,
    df_w_limits,
    LimitFeat: Type[doglast.IntLimitType] = doglast.IntLimitType,
):
    out = pd.Series(dtype=df_w_limits.index.dtype, index=series.index)
    for cat, catrow in df_w_limits.iterrows():
        # the ~ bc of nan's
        out.loc[
            ~(series < catrow[LimitFeat.min]) & ~(series > catrow[LimitFeat.max])
        ] = cat
    return out


class StatusIndex(dz.IndexBase):
    status_name = str


class StatusFeatures(dz.TableFeaturesBase):
    wins = doglast.IntLimitType


class SizedDogFeatures(dz.TableFeaturesBase):
    def __init__(self) -> None:
        self.limit_df = doglast.dog_size_table.get_full_df()

    def size(self, dog_df) -> Col[doglast.DogSizeIndex]:
        return fit_to_limit(
            dog_df[dogfirst.DogFeatures.waist],
            self.limit_df,
            doglast.DogSizeFeatures.waist_limit,
        )


status_table = dz.ScruTable(StatusFeatures, StatusIndex)
sized_dog_table = dz.ScruTable(
    SizedDogFeatures,
    index=dogfirst.DogIndex,
    subject_of_records=dogfirst.Dog,
)

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
    win_count = comp_df.groupby(dogfirst.CompetitionFeatures.winner.pet.dog_id)[
        dogfirst.CompetitionFeatures.prize_pool
    ].count()
    q_arr = np.quantile(win_count, sorted(ends))
    q_arr[-1] = q_arr[-1] * top_status_multiplier
    q_map = dict(zip(ends, q_arr.astype(int)))
    status_df = pd.DataFrame(
        [
            {
                StatusIndex.status_name: lname,
                StatusFeatures.wins.min: q_map[lminq],
                StatusFeatures.wins.max: q_map[lmaxq],
            }
            for lname, (lminq, lmaxq) in limits.items()
        ]
    ).set_index(StatusIndex.status_name)
    status_table.replace_all(status_df)
    status_md.write_text(status_df.to_markdown())

    # other way is with colassigners
    # type hinted with Col[...]
    # (merging data from two sources)
    dogfirst.dog_table.get_full_df().pipe(SizedDogFeatures()).loc[
        :, [SizedDogFeatures.size]
    ].pipe(sized_dog_table.replace_all)
