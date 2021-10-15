import sys

import pandas as pd
import src.namespace_metadata as ns
from colassigner import get_all_cols

from sscutils import dump_dfs_to_tables

if __name__ == "__main__":

    droot = sys.argv[1]

    persons_df = pd.read_csv(f"{droot}/people.csv").set_index(
        ns.PersonIndex.person_id
    )

    dogs_df = pd.read_csv(f"{droot}/dog.csv").set_index(ns.DogIndex.dog_id)
    comps_df = pd.read_csv(f"{droot}/comp.csv").set_index(
        ns.CompetitionIndex.competition_id
    )
    rels_df = (
        pd.read_csv(f"{droot}/rel.csv")
        .rename(
            columns={
                "owner_id": ns.RelationshipIndex.owner.person_id,
                "dog_id": ns.RelationshipIndex.dog.dog_id,
            }
        )
        .set_index(get_all_cols(ns.RelationshipIndex))
    )
    spots_df = pd.read_csv(f"{droot}/spotted.csv")
    photo_df = pd.read_csv(f"{droot}/photo.csv").set_index(
        ns.PhotoIndex.photo_id
    )

    dump_dfs_to_tables(
        None,
        [
            (persons_df, ns.persons_table),
            (dogs_df, ns.dogs_table),
            (comps_df, ns.competitions_table),
            (rels_df, ns.relationships_table),
            (spots_df, ns.spots_table),
            (photo_df, ns.photos_table),
        ],
    )
