import metazimmer.dogracebase.core as doglast

import datazimmer as dz

from ..success import Status

# TODO: this seems a bit too much hassle


class CountBase(dz.AbstractEntity):
    elem_count = int


class SizeCount(CountBase):
    size = dz.Index & doglast.DogSize


class StatusCount(CountBase):
    status = dz.Index & Status


size_count_table = dz.ScruTable(SizeCount)
status_count_table = dz.ScruTable(StatusCount)
