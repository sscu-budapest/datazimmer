from dataclasses import dataclass
from time import time

import aswan
import pandas as pd
from atqo import parallel_map

import datazimmer as dz

from . import ns_meta as ns

rel_renamer = {
    "owner_id": ns.Relationship.owner.cid,
    "dog_id": ns.Relationship.dog.cid,
}

PHOTO_INC = 10


class PhotoCollector(aswan.RequestHandler):
    def load_cache(self, url):
        ps = PhotoState.load()
        return (
            pd.read_csv(f"{url}/photo.csv")
            .iloc[ps.photos_loaded : ps.photos_loaded + PHOTO_INC]
            .rename(columns={f"rel__{k}": f"rel__{v}" for k, v in rel_renamer.items()})
        )


@dataclass
class PhotoState(dz.PersistentState):
    photos_loaded: int = 0


@dataclass
class DataStoreState(dz.PersistentState):
    data_root: str = ""


class PhotoProject(dz.DzAswan):
    name = "dog-show-core"
    cron: str = "0 11 * * 2"

    def prepare_run(self):
        data_root = DataStoreState.load().data_root
        self.starters[PhotoCollector] = [data_root]


@dz.register_data_loader(extra_deps=[ns, PhotoProject, PhotoState])
def update_data():
    ext_src = "dog-raw"
    persons_df = pd.read_csv(dz.get_raw_data_path("people.csv", ext_src))
    dogs_df = pd.read_csv(dz.get_raw_data_path("dog.csv", ext_src))
    comps_df = pd.read_csv(dz.get_raw_data_path("comp.csv", ext_src))

    rels_df = pd.read_csv(dz.get_raw_data_path("rel.csv", ext_src)).rename(
        columns=rel_renamer
    )
    spots_df = pd.read_csv(dz.get_raw_data_path("spotted.csv", ext_src), dtype=str)

    # for coll_ev in PhotoProject().get_unprocessed_events(PhotoCollector):
    #    ns.photo_table.replace_records(coll_ev.content)
    pp = PhotoProject()
    list(parallel_map(_rep, pp.get_unprocessed_events(PhotoCollector)))

    # test if output got extended
    old_state = PhotoState.load()
    photo_df = ns.photo_table.get_full_df()
    assert photo_df.shape[0] == (old_state.photos_loaded + PHOTO_INC)

    all_full = (
        pd.concat(
            [p.content for p in pp.get_all_events(PhotoCollector, only_latest=False)]
        )
        .drop_duplicates()
        .set_index(ns.photo_table.index_cols)
        .loc[photo_df.index, photo_df.columns]
    )

    assert photo_df.equals(all_full)

    PhotoState(photo_df.shape[0]).save()
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


def _rep(coll_ev: aswan.ParsedCollectionEvent):
    ns.photo_table.replace_records(coll_ev.content)
