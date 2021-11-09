from sscutils import TableFeaturesBase, ScruTable

from ..imported_namespaces import doglast

# TODO: this seems a bit too much hassle


class SizeCountFeatures(TableFeaturesBase):
    count = int


size_count_table = ScruTable(
    SizeCountFeatures,
    index=doglast.DogSizeIndex,  # TODO: this to sql properly
    subject_of_records=doglast.DogSize,
)
