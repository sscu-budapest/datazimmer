import datetime as dt

import datazimmer as dz


class Thing(dz.AbstractEntity):
    d = dt.datetime
    num = float

    ind = dz.Index & int


scrutable = dz.ScruTable(Thing)


@dz.register
def template_proc():
    pass
