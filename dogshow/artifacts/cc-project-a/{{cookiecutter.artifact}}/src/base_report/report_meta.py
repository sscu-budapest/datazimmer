from sscutils import TableFeaturesBase, ScruTable, IndexBase

from ..imported_namespaces import doglast

# TODO: this seems a bit too much hassle


class SizeCountFeatures(TableFeaturesBase):
    count = int

class SizeCountIndex(IndexBase):
    size = doglast.DogSizeIndex  # TODO: this knows the other col


size_count_table = ScruTable(
    SizeCountFeatures,
    index=SizeCountIndex,
    subject_of_records=doglast.DogSize,
)
