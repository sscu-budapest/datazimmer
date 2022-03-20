import metazimmer.dogracebase.core as doglast

import datazimmer as dz

from ..success import status_table

# TODO: this seems a bit too much hassle


class CountBase(dz.TableFeaturesBase):
    elem_count = int


class SizeCountFeatures(CountBase):
    pass


class StatusCountFeatures(CountBase):
    pass


class SizeCountIndex(dz.IndexBase):
    size = doglast.DogSizeIndex


class StatusCountIndex(dz.IndexBase):
    status = status_table.index


size_count_table = dz.ScruTable(
    SizeCountFeatures,
    index=SizeCountIndex,
    subject_of_records=doglast.DogSize,
)

status_count_table = dz.ScruTable(
    StatusCountFeatures, StatusCountIndex, subject_of_records=status_table.subject
)
