from functools import reduce
from itertools import chain
from operator import and_

import datazimmer as dz

from . import ns_meta as ns


@dz.register_env_creator(extra_deps=[ns])
def create_environments(min_prize_pool):
    """create environments that are described in the config of the repo"""
    comps_df = ns.competition_table.get_full_df().loc[
        lambda df: df[ns.Competition.prize_pool] >= min_prize_pool, :
    ]

    # all this should be possible automatically
    # based on the metadata
    comprescols = [
        ns.Competition.winner,
        ns.Competition.runner_up,
        ns.Competition.special_mention,
    ]

    pcols = [C.owner.cid for C in comprescols]
    dcols = [C.pet.cid for C in comprescols]
    _persons, _dogs = [_uelems(comps_df, _cols) for _cols in [pcols, dcols]]

    persons_df = ns.person_table.get_full_df().loc[_persons, :]
    dogs_df = ns.dog_table.get_full_df().loc[_dogs, :]

    _spot_dcols = (ns.Spotting.dog_1.cid, ns.Spotting.dog_2.cid)
    spots_df = (
        ns.spot_table.get_full_df()
        .pipe(_allin, {_spot_dcols: _dogs})
        .reset_index(drop=True)
    )
    _rel_coldic = {
        (ns.Relationship.owner.cid,): _persons,
        (ns.Relationship.dog.cid,): _dogs,
    }
    _pic_coldic = {
        (ns.Photo.rel.owner.cid,): _persons,
        (ns.Photo.rel.dog.cid,): _dogs,
    }
    rels_df = ns.relationship_table.get_full_df().pipe(_allin, _rel_coldic)
    photo_df = ns.photo_table.get_full_df().pipe(_allin, _pic_coldic)
    pairs = [
        (persons_df, ns.person_table),
        (dogs_df, ns.dog_table),
        (comps_df, ns.competition_table),
        (spots_df, ns.spot_table),
        (rels_df, ns.relationship_table),
        (photo_df, ns.photo_table),
    ]
    dz.dump_dfs_to_tables(pairs)


def _uelems(df, cols):
    return df.loc[:, cols].unstack().unique()


def _allin(df, cols_to_sets: dict):
    _bmap = map(lambda kv: _isin(df, *kv), cols_to_sets.items())
    return df.loc[reduce(and_, chain(*_bmap)), :]


def _isin(df, cols, set_):
    for col in cols:
        if col in df.index.names:
            _ser = df.index.get_level_values(col)
        else:
            _ser = df.loc[:, col]
        yield _ser.isin(set_)
