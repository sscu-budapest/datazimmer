import datetime as dt

import pandas as pd

import datazimmer as dz


class Thing(dz.AbstractEntity):
    d = dt.datetime
    num = float
    c = str

    ind = dz.Index & int


class Thang(dz.AbstractEntity):
    ti = Thing
    tio = Thing


scrutable = dz.ScruTable(Thing, partitioning_cols=[Thing.c])

thang_table = dz.ScruTable(Thang, entity_key_table_map={Thang.ti: scrutable})


@dz.register
def proc():
    dfi = pd.DataFrame(
        {Thing.d: [dt.datetime.now()], Thing.num: [1], Thing.c: ["a"], Thing.ind: [0]}
    )
    dfa = pd.DataFrame({Thang.ti.ind: [0], Thang.tio.ind: [0]})
    scrutable.replace_all(dfi)
    thang_table.replace_all(dfa)
