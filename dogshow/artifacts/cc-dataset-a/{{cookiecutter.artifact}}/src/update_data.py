import pandas as pd
import src.namespace_metadata as ns
from sscutils import dump_dfs_to_tables


def update_data(data_root):

    persons_df = pd.read_csv(f"{data_root}/people.csv")

    dogs_df = pd.read_csv(f"{data_root}/dog.csv")
    comps_df = pd.read_csv(f"{data_root}/comp.csv")
    rel_renamer = {
        "owner_id": ns.RelationshipIndex.owner.person_id,
        "dog_id": ns.RelationshipIndex.dog.dog_id,
    }
    rels_df = (
        pd.read_csv(f"{data_root}/rel.csv")
        .rename(columns=rel_renamer)
    )
    spots_df = pd.read_csv(f"{data_root}/spotted.csv", dtype=str)
    photo_df = (
        pd.read_csv(f"{data_root}/photo.csv")
        .rename(
            columns={f"rel__{k}": f"rel__{v}" for k, v in rel_renamer.items()}
        )
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
