from datetime import datetime
from sscutils import CompositeTypeBase, IndexBase, ScruTable, TableFeaturesBase, Nullable

from .imported_namespaces import dogbase

# name here ^^ matches the prefix in the imported-namespaces.yaml
# code is written with import-namespaces invoke command


class DogSizeIndex(IndexBase):
    dogsize_name = str


class DogIndex(IndexBase):
    canine_id = str


class CompetitionIndex(IndexBase):
    competition_id = str


class DogCategory(CompositeTypeBase):
    pure = bool
    neutered = bool


class DogOfTheMonthIndex(IndexBase):
    dog_type = DogCategory
    year = int
    month = int


class IntLimitType(CompositeTypeBase):
    min = int
    max = int


class DogSizeFeatures(TableFeaturesBase):
    waist_limit = IntLimitType
    weight_limit = IntLimitType


class DogFeatures(TableFeaturesBase):
    name = str
    color = Nullable(str)
    size = DogSizeIndex


class CompetitionFeatures(TableFeaturesBase):
    held_date = datetime
    fastest_time = float
    champion = DogIndex


class DogOfTheMonthFeatures(TableFeaturesBase):
    winner = DogIndex


dog_size_table = ScruTable(DogSizeFeatures, DogSizeIndex)
dog_table = ScruTable(DogFeatures, DogIndex, subject_of_records=dogbase.Dog)
competition_table = ScruTable(CompetitionFeatures, CompetitionIndex)
dog_of_the_month_table = ScruTable(
    DogOfTheMonthFeatures, DogOfTheMonthIndex, max_partition_size=3
)
