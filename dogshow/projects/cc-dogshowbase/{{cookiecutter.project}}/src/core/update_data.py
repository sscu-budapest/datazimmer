from time import time

import pandas as pd

import datazimmer as dz

from . import ns_meta as ns


@dz.register_data_loader(extra_deps=[ns], cron="0 0 1 * *")  # cron: run weekly
def update_data(data_root):

    persons_df = pd.read_csv(f"{data_root}/people.csv")
    dogs_df = pd.read_csv(f"{data_root}/dog.csv")
    comps_df = pd.read_csv(f"{data_root}/comp.csv")
    rel_renamer = {
        "owner_id": ns.Relationship.owner.cid,
        "dog_id": ns.Relationship.dog.cid,
    }
    rels_df = pd.read_csv(f"{data_root}/rel.csv").rename(columns=rel_renamer)
    spots_df = pd.read_csv(f"{data_root}/spotted.csv", dtype=str)
    photo_df = pd.read_csv(f"{data_root}/photo.csv").rename(
        columns={f"rel__{k}": f"rel__{v}" for k, v in rel_renamer.items()}
    )

    # for cron - data update
    randog = {
        "cid": f"d-{dogs_df.shape[0] + 1}",
        "name": "Randog",
        "date_of_birth": "2014-04-01",
        "waist": (time() - 1648 * 10**6) / 10**4,
        "sex": "male",
    }
    extended_dog_df = pd.concat([dogs_df, pd.DataFrame([randog])])

    pairs = [
        (persons_df, ns.person_table),
        (extended_dog_df, ns.dog_table),
        (comps_df, ns.competition_table),
        (rels_df, ns.relationship_table),
        (spots_df, ns.spot_table),
        (photo_df, ns.photo_table),
    ]
    dz.dump_dfs_to_tables(pairs)
