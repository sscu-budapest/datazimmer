import metazimmer.dogracebase.core as doglast
import metazimmer.dogshowbase.core as dogfirst
from colassigner import ChildColAssigner, Col, ColAssigner

import datazimmer as dz

from .success import StatusIndex

# TODO: complex examples in this namespace


class DogPair(dz.BaseEntity):
    pass


class DominationIndex(dz.IndexBase):
    dominator = dogfirst.DogIndex
    dominated = dogfirst.DogIndex


class DominationType(ChildColAssigner):
    # TODO: simple demonstration of parsing composite type from
    # child col assigner

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


class SuccessType(ChildColAssigner):
    # TODO: compex demonstration of parsing composite type from
    # child col assigner

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
    def total_earnings(self) -> Col[float]:
        pass


class PersonByDogsizeIndex(dz.IndexBase):
    person = dogfirst.PersonIndex
    size = doglast.DogSizeIndex


class PersonByDogsizeFeatures(ColAssigner):
    def __init__(self, status_df) -> None:
        self.status_df = status_df

    success = SuccessType


class DogSizeLegendFeatures(ColAssigner):
    result = dogfirst.ResultType
    total_winnings_in_size = int
    win_rate_of_winner = float


# domination_table = ScruTable(DominationFeatures, DominationIndex, DogPair)
# person_by_dogsize_table = ScruTable(PersonByDogsizeFeatures, PersonByDogsizeIndex)
# dogsize_leader_table = ScruTable(
#     DogSizeLegendFeatures, subject_of_records=doglast.DogSize
# )


# @register
def todo():
    pass

    # TODO more complex nested structure
    # pbd_assigner = PersonByDogsizeFeatures(status_df)
