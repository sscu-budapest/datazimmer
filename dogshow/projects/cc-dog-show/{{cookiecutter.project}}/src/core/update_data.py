import random
from dataclasses import dataclass
from functools import partial
from time import time

import aswan
import pandas as pd
from atqo import parallel_map

import datazimmer as dz

from . import ns_meta as ns

rel_renamer = {"owner_id": ns.Relationship.owner.cid, "dog_id": ns.Relationship.dog.cid}

PHOTO_INC = 10


def read_ext_csv(name, **kwargs) -> pd.DataFrame:
    return pd.read_csv(dz.get_raw_data_path(f"{name}.csv", "dog-raw"), **kwargs)


class PhotoCollector(aswan.RequestHandler):
    def load_cache(self, _):
        ps = PhotoState.load()
        rng = random.Random(7 + ps.photos_loaded)
        rel_df = read_ext_csv("rel").set_index(list(rel_renamer.keys()))
        rec_part = partial(_get_prec, rdf=rel_df, rng=rng, n=ps.photos_loaded)
        photo_df = pd.DataFrame(map(rec_part, range(PHOTO_INC))).set_index("photo_id")
        raw_dir = dz.get_raw_data_path(f"b-{ps.photos_loaded}")
        raw_dir.mkdir()
        photo_df.to_markdown(raw_dir / "p.md")
        return photo_df.rename(
            columns={f"rel__{k}": f"rel__{v}" for k, v in rel_renamer.items()}
        )


def _get_prec(i, rng: random.Random, rdf: pd.DataFrame, n: int):
    rels = {
        f"rel__{ind_id}": val
        for ind_id, val in zip(rdf.index.names, rng.choice(rdf.index))
    }
    return {"photo_id": f"ph-{i+1+n}", "cuteness": rng.betavariate(2, 3), **rels}


@dataclass
class PhotoState(dz.PersistentState):
    photos_loaded: int = 0


class PhotoProject(dz.DzAswan):
    name = "dog-show-core"
    cron: str = "0 11 * * 2"
    starters = {PhotoCollector: ["any"]}


@dz.register_data_loader(extra_deps=[ns, PhotoProject, PhotoState])
def update_data():  # TODO: add raw paths as dependencies
    persons_df = read_ext_csv("people")
    dogs_df = read_ext_csv("dog")
    comps_df = read_ext_csv("comp")

    rels_df = read_ext_csv("rel").rename(columns=rel_renamer)
    spots_df = read_ext_csv("spotted", dtype=str)

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
        .loc[photo_df.index, photo_df.columns]
    )

    assert photo_df.equals(all_full)

    PhotoState(photo_df.shape[0]).save()


def _rep(coll_ev: aswan.ParsedCollectionEvent):
    ns.photo_table.replace_records(coll_ev.content)
