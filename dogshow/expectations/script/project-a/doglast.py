from datetime import datetime  # noqa: F401

from sscutils import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    Nullable,
    TableFactory,
    TableFeaturesBase,
)

from . import dogfirst


class Competition(BaseEntity):
    pass


class DogOfTheMonth(BaseEntity):
    pass


class DogSize(BaseEntity):
    pass


class DogCategory(CompositeTypeBase):
    pure = bool
    neutered = bool


class IntLimitType(CompositeTypeBase):
    min = int
    max = int


class CompetitionIndex(IndexBase):
    competition_id = str


class DogOfTheMonthIndex(IndexBase):
    dog_type = DogCategory
    year = int
    month = int


class DogSizeFeatures(TableFeaturesBase):
    waist_limit = IntLimitType
    weight_limit = IntLimitType


class DogSizeIndex(IndexBase):
    dogsize_name = str


class DogFeatures(TableFeaturesBase):
    name = str
    color = Nullable(str)
    size = DogSizeIndex


class DogIndex(IndexBase):
    canine_id = str


class CompetitionFeatures(TableFeaturesBase):
    held_date = datetime
    fastest_time = float
    champion = DogIndex


class DogOfTheMonthFeatures(TableFeaturesBase):
    winner = DogIndex


table_factory = TableFactory("doglast")
competition_table = table_factory.create(
    features=CompetitionFeatures, subject_of_records=Competition, index=CompetitionIndex
)
dog_of_the_month_table = table_factory.create(
    features=DogOfTheMonthFeatures, subject_of_records=DogOfTheMonth, index=DogOfTheMonthIndex, max_partition_size=3
)
dog_size_table = table_factory.create(features=DogSizeFeatures, subject_of_records=DogSize, index=DogSizeIndex)
dog_table = table_factory.create(features=DogFeatures, subject_of_records=dogfirst.Dog, index=DogIndex)
