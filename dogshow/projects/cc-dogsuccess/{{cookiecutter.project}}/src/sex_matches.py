import metazimmer.dogshowbase.core.ns_meta as dogfirst
from colassigner import Col

import datazimmer as dz


class SexMatch(dz.AbstractEntity):
    def __init__(self, dog_df) -> None:
        self.dog_df = dog_df

    def count(self, df) -> Col[int]:
        return 1

    def sex_1(self, df) -> Col[dz.Index & str]:
        return self._get_sex(df, dogfirst.Spotting.dog_1.cid)

    def sex_2(self, df) -> Col[dz.Index & str]:
        return self._get_sex(df, dogfirst.Spotting.dog_2.cid)

    def _get_sex(self, spot_df, spot_ind):
        return self.dog_df[dogfirst.Dog.sex].reindex(spot_df[spot_ind].values).values


sex_match_table = dz.ScruTable(SexMatch)


@dz.register(
    dependencies=[
        dogfirst.spot_table,
        dogfirst.dog_table,
    ],
    outputs=[sex_match_table],
)
def calculate_sex_match():

    spot_df = dogfirst.spot_table.get_full_df()
    dog_df = dogfirst.dog_table.get_full_df()

    sm_df = (
        spot_df.pipe(SexMatch(dog_df))
        .groupby(sex_match_table.index_cols)[[SexMatch.count]]
        .sum()
    )

    sex_match_table.replace_all(sm_df)
