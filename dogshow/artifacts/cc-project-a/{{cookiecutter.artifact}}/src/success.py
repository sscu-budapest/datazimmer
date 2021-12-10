from itertools import chain
from pathlib import Path
from typing import Type

import numpy as np
import pandas as pd
from colassigner import ChildColAssigner, ColAssigner, Col
from sscutils import IndexBase, ScruTable, TableFeaturesBase

from .imported_namespaces import dogfirst, doglast
from .imported_namespaces.dogfirst import Dog
from .pipereg import pipereg
from .util import report_md


def fit_to_limit(
    series,
    df_w_limits,
    LimitFeat: Type[doglast.IntLimitType] = doglast.IntLimitType,
):
    out = pd.Series(dtype=df_w_limits.index.dtype, index=series.index)
    for cat, catrow in df_w_limits.iterrows():
        # the ~ bc of nan's
        out.loc[
            ~(series < catrow[LimitFeat.min])
            & ~(series > catrow[LimitFeat.max])
        ] = cat
    return out


class StatusIndex(IndexBase):
    status_name = str


class StatusFeatures(TableFeaturesBase):
    wins = doglast.IntLimitType


class DogSizeCalculation(TableFeaturesBase):
    def __init__(self) -> None:
        self.limit_df = doglast.dog_size_table.get_full_df()

    def size(self, dog_df) -> Col[doglast.DogSizeIndex]:
        return fit_to_limit(
            dog_df[dogfirst.DogFeatures.waist],
            self.limit_df,
            doglast.DogSizeFeatures.waist_limit,
        )


class SuccessType(ChildColAssigner):

    _gb_cols = ...
    _success_col = ...
    _winnings_cols = ...

    def __init__(self, df, parent_assigner: "PersonByDogsizeFeatures") -> None:
        self.pbd = parent_assigner
        self.gseries = df.groupby(self._gb_cols).agg(
            {
                self._success_col: "count",
            }
        )

    # Col[...] type hint is necessary for foreign keys
    def best_status(self) -> Col[StatusIndex]:
        # function finding fitting intlimit feature
        # replicated here
        self.pbd.status_df.sample(1).index[0]

    # can be given in other cases
    def total_wins(self) -> Col[int]:
        return self.df

    # can not be inferred if not given, yet
    def total_earnings(self):
        pass


class PersonByDogsizeIndex(IndexBase):
    person = dogfirst.PersonIndex
    size = doglast.DogSizeIndex


class PersonByDogsizeFeatures(ColAssigner):
    def __init__(self, status_df) -> None:
        self.status_df = status_df

    success = SuccessType


class DominationIndex(IndexBase):
    dominator = dogfirst.DogIndex
    dominated = dogfirst.DogIndex


class DominationType(ChildColAssigner):
    _c1 = ...  # col where dominator is
    _c2 = ...  # col where dominated is

    def over_count(self, df) -> Col[int]:
        pass

    def over_rate(self, df) -> Col[float]:
        pass


class DominationFeatures(ColAssigner):
    class WinnerOverRunnerUp(DominationType):
        _c1 = dogfirst.CompetitionFeatures.winner.pet
        _c2 = dogfirst.CompetitionFeatures.runner_up.pet

    class WinnerOverSpecialMention(DominationType):
        _c1 = dogfirst.CompetitionFeatures.winner.pet
        _c2 = dogfirst.CompetitionFeatures.special_mention.pet

    class RunnerUpOverSpecialMention(DominationType):
        _c1 = dogfirst.CompetitionFeatures.runner_up.pet
        _c2 = dogfirst.CompetitionFeatures.special_mention.pet


class DogsizeLegendFeatures(ColAssigner):
    result = dogfirst.ResultType
    total_winnings_in_size = int
    win_rate_of_winner = float


status_table = ScruTable(StatusFeatures, StatusIndex)
sized_dogs_table = ScruTable(
    DogSizeCalculation,
    index=dogfirst.DogIndex,
    subject_of_records=Dog,
    name="dogs_w_sizes",
)
# TODO:
#  person_by_dogsize_table = ScruTable(
#    PersonByDogsizeFeatures, PersonByDogsizeIndex
#  )
# domination_table = ScruTable(DominationFeatures)
# dogsize_leaders_table = ScruTable(
#    DogsizeLegendFeatures, subject_of_records=doglast.DogSize
# )

# TODO: formalize reporting one day
status_md_path = report_md("status_table")


@pipereg.register(
    dependencies=[
        dogfirst.competition_table,
        dogfirst.dog_table,
        doglast.dog_size_table,
    ],
    outputs=[status_table, sized_dogs_table],
    outputs_nocache=[status_md_path],
)  # TODO: check if these are correctly recorded in dvc
def calculate_success(top_status_multiplier: int):

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
    win_count = comp_df.groupby(
        dogfirst.CompetitionFeatures.winner.pet.dog_id
    )[dogfirst.CompetitionFeatures.prize_pool].count()
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
    status_md_path.parent.mkdir(exist_ok=True)
    status_df.to_markdown(status_md_path)

    # other way is with colassigners
    # type hinted with Col[...]
    # (merging data from two sources)
    dogfirst.dog_table.get_full_df().pipe(DogSizeCalculation()).loc[
        :, [DogSizeCalculation.size]
    ].pipe(sized_dogs_table.replace_all)

    # more complex nested structure
    # pbd_assigner = PersonByDogsizeFeatures(status_df)
