import pandas as pd
import src.namespace_metadata as ns
from colassigner import get_all_cols
from sscutils import dump_dfs_to_tables


def update_data(data_root):

    persons_df = pd.read_csv(f"{data_root}/people.csv").set_index(
        ns.PersonIndex.person_id
    )

    dogs_df = pd.read_csv(f"{data_root}/dog.csv").set_index(ns.DogIndex.dog_id)
    comps_df = pd.read_csv(f"{data_root}/comp.csv").set_index(
        ns.CompetitionIndex.competition_id
    )
    rels_df = (
        pd.read_csv(f"{data_root}/rel.csv")
        .rename(
            columns={
                "owner_id": ns.RelationshipIndex.owner.person_id,
                "dog_id": ns.RelationshipIndex.dog.dog_id,
            }
        )
        .set_index(get_all_cols(ns.RelationshipIndex))
    )
    spots_df = pd.read_csv(f"{data_root}/spotted.csv")
    photo_df = pd.read_csv(f"{data_root}/photo.csv").set_index(
        ns.PhotoIndex.photo_id
    )

    dump_dfs_to_tables(
        None,
        [
            (persons_df, ns.person_table),
            (dogs_df, ns.dog_table),
            (comps_df, ns.competition_table),
            (rels_df, ns.relationship_table),
            (spots_df, ns.spot_table),
            (photo_df, ns.photo_table),
        ],
    )
