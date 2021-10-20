from sscutils import dump_dfs_to_tables

from .namespace_metadata import (
    CompetitionFeatures,
    RelationshipFeatures,
    RelationshipIndex,
    SpotFeatures,
    competition_table,
    dog_table,
    person_table,
    photo_table,
    relationship_table,
    spot_table,
)


def create_environments(subset_name, min_prize_pool):
    """create environments that are described in the config of the repo"""
    comps_df = competition_table.get_full_df().loc[
        lambda df: df[CompetitionFeatures.prize_pool] >= min_prize_pool, :
    ]

    # all this should be possible automatically
    # based on the metadata
    comprescols = [
        CompetitionFeatures.winner,
        CompetitionFeatures.runner_up,
        CompetitionFeatures.special_mention,
    ]
    relevant_persons = (
        comps_df.loc[
            :,
            [C.owner.person_id for C in comprescols],
        ]
        .unstack()
        .unique()
    )

    relevant_dogs = (  # not very dry...
        comps_df.loc[
            :,
            [C.pet.dog_id for C in comprescols],
        ]
        .unstack()
        .unique()
    )

    persons_df = person_table.get_full_df().loc[relevant_persons, :]
    dogs_df = dog_table.get_full_df().loc[relevant_dogs, :]
    spots_df = spot_table.get_full_df().loc[
        lambda df: df[SpotFeatures.dog_1.dog_id].isin(relevant_dogs)
        & df[SpotFeatures.dog_2.dog_id].isin(relevant_dogs),
        :,
    ]
    rels_df = relationship_table.get_full_df().pipe(
        lambda df: df.reset_index()
        .loc[
            lambda _df: _df[RelationshipIndex.owner.person_id].isin(
                relevant_persons
            )
            & _df[RelationshipIndex.dog.dog_id].isin(relevant_dogs),
            :,
        ]
        .set_index(df.index.names)
    )

    dump_dfs_to_tables(
        subset_name,
        [
            (persons_df, person_table),
            (dogs_df, dog_table),
            (comps_df, competition_table),
            (spots_df, spot_table),
            (rels_df, relationship_table),
        ],
    )
