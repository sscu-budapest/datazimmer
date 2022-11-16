import datetime as dt

import datazimmer as dz


class Thing(dz.AbstractEntity):
    d = dt.datetime
    num = float
    c = str

    ind = dz.Index & int


scrutable = dz.ScruTable(Thing, partitioning_cols=[Thing.c])


@dz.register
def template_proc():
    pass
