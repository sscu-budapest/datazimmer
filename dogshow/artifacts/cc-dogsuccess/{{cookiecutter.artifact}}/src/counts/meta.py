import metazimmer.dogracebase.core as doglast

from sscutils import IndexBase, ScruTable, TableFeaturesBase

from ..success import status_table

# TODO: this seems a bit too much hassle


class CountBase(TableFeaturesBase):
    elem_count = int


class SizeCountFeatures(CountBase):
    pass


class StatusCountFeatures(CountBase):
    pass


class SizeCountIndex(IndexBase):
    size = doglast.DogSizeIndex


class StatusCountIndex(IndexBase):
    status = status_table.index


size_count_table = ScruTable(
    SizeCountFeatures,
    index=SizeCountIndex,
    subject_of_records=doglast.DogSize,
)

status_count_table = ScruTable(
    StatusCountFeatures, StatusCountIndex, subject_of_records=status_table.subject
)
