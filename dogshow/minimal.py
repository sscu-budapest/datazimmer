import datetime as dt

import datazimmer as dz


class TableFeatures(dz.TableFeaturesBase):
    d = dt.datetime
    num = float


class ThingIndex(dz.IndexBase):
    ind = int


scrutable = dz.ScruTable(TableFeatures, index=ThingIndex)


@dz.register
def template_proc():
    pass
