from dataclasses import dataclass
from time import time

import aswan
import pandas as pd

import datazimmer as dz

from . import ns_meta as ns

depot_name = "dogshowbase-core"

rel_renamer = {
    "owner_id": ns.Relationship.owner.cid,
    "dog_id": ns.Relationship.dog.cid,
}


class PhotoCollector(aswan.RequestHandler):
    def load_cache(self, url):
        ps = PhotoState.load()
        return (
            pd.read_csv(f"{url}/photo.csv")
            .iloc[ps.photos_loaded : ps.photos_loaded + 10]
            .rename(columns={f"rel__{k}": f"rel__{v}" for k, v in rel_renamer.items()})
        )


@dataclass
class PhotoState(dz.PersistentState):
    photos_loaded: int = 0


@dataclass
class DataStoreState(dz.PersistentState):
    data_root: str = ""


class PhotoProject(dz.DzAswan):
    name = depot_name
    cron: str = "0 11 * * 2"

    def extend_starters(self):
        data_root = DataStoreState.load().data_root
        self.starters[PhotoCollector] = [data_root]


# TODO: state as dependency
@dz.register_data_loader(extra_deps=[ns, PhotoProject, PhotoState])
def update_data(data_root):

    persons_df = pd.read_csv(f"{data_root}/people.csv")
    dogs_df = pd.read_csv(f"{data_root}/dog.csv")
    comps_df = pd.read_csv(f"{data_root}/comp.csv")

    rels_df = pd.read_csv(f"{data_root}/rel.csv").rename(columns=rel_renamer)
    spots_df = pd.read_csv(f"{data_root}/spotted.csv", dtype=str)

    for coll_ev in PhotoProject().get_unprocessed_events(PhotoCollector):
        ns.photo_table.replace_records(coll_ev.content)

    # test if output got extended
    old_state = PhotoState.load()
    assert ns.photo_table.get_full_df().shape[0] > old_state.photos_loaded

    PhotoState(ns.photo_table.get_full_df().shape[0]).save()
    # for cron - data update
    randog = {
        ns.Dog.cid: f"d-{dogs_df.shape[0] + 1}",
        ns.Dog.name: "Randog",
        ns.Dog.date_of_birth: "2014-04-01",
        ns.Dog.waist: (time() - 1648 * 10**6) / 10**4,
        ns.Dog.sex: "female",
    }
    extended_dog_df = pd.concat([dogs_df, pd.DataFrame([randog])])

    pairs = [
        (persons_df, ns.person_table),
        (extended_dog_df, ns.dog_table),
        (comps_df, ns.competition_table),
        (rels_df, ns.relationship_table),
        (spots_df, ns.spot_table),
    ]
    dz.dump_dfs_to_tables(pairs)
