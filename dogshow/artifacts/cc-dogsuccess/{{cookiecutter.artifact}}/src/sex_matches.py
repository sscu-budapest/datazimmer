import metazimmer.dogshowbase.core as dogfirst
from colassigner import Col, ColAssigner
from colassigner.meta_base import get_all_cols

import datazimmer as dz


class SexMatchIndex(ColAssigner):
    def __init__(self, dog_df) -> None:
        self.dog_df = dog_df

    def sex_1(self, df) -> Col[str]:
        return self._get_sex(df, dogfirst.SpotFeatures.dog_1.dog_id)

    def sex_2(self, df) -> Col[str]:
        return self._get_sex(df, dogfirst.SpotFeatures.dog_2.dog_id)

    def _get_sex(self, spot_df, spot_ind):
        return (
            self.dog_df[dogfirst.DogFeatures.sex]
            .reindex(spot_df[spot_ind].values)
            .values
        )


class SexMatchFeatures(dz.TableFeaturesBase):
    count = int


sex_match_table = dz.ScruTable(SexMatchFeatures, SexMatchIndex)


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
        spot_df.assign(**{SexMatchFeatures.count: 1})
        .pipe(SexMatchIndex(dog_df))
        .groupby(get_all_cols(SexMatchIndex))[[SexMatchFeatures.count]]
        .sum()
    )

    sex_match_table.replace_all(sm_df)
