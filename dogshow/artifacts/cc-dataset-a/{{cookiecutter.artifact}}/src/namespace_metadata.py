import datetime as dt

from sscutils import (
    CompositeTypeBase,
    IndexBase,
    ScruTable,
    TableFeaturesBase,
    BaseEntity,
    Nullable
)


class Creature(BaseEntity):
    pass


class Pet(BaseEntity):
    """most likely owned by pet owners, but not necessarily a creature"""

    # ^ this goes to description
    pass


class Dog(Creature, Pet):
    pass


class Person(Creature):
    pass


class DogIndex(IndexBase):
    dog_id = str


class PersonIndex(IndexBase):
    person_id = str


class CompetitionIndex(IndexBase):
    competition_id = str


class RelationshipIndex(IndexBase):
    owner = PersonIndex
    dog = DogIndex


class PhotoIndex(IndexBase):
    photo_id = str


class ResultType(CompositeTypeBase):
    owner = PersonIndex
    pet = DogIndex
    prize = int


class BuildingInfoType(CompositeTypeBase):
    floor = int
    door = int


class AddressType(CompositeTypeBase):
    city = str
    zip = str
    street_address = str
    building = BuildingInfoType


class PersonFeatures(TableFeaturesBase):
    name = str
    date_of_birth = Nullable(dt.datetime)


class DogFeatures(TableFeaturesBase):
    name = str
    date_of_birth = dt.datetime
    waist = Nullable(float)
    sex = str


class RelationshipFeatures(TableFeaturesBase):
    since_birth = bool


class CompetitionFeatures(TableFeaturesBase):
    prize_pool = int
    winner = ResultType
    runner_up = ResultType
    special_mention = ResultType


class SpotFeatures(TableFeaturesBase):
    dog_1 = DogIndex
    dog_2 = DogIndex
    place = AddressType


class PhotoFeatures(TableFeaturesBase):
    cuteness = float
    rel = RelationshipIndex


person_table = ScruTable(
    PersonFeatures, PersonIndex, subject_of_records=Person
)
dog_table = ScruTable(
    DogFeatures,
    DogIndex,
    subject_of_records=Dog,
    partitioning_cols=[DogFeatures.sex],
)
relationship_table = ScruTable(RelationshipFeatures, RelationshipIndex)
competition_table = ScruTable(CompetitionFeatures, CompetitionIndex)
spot_table = ScruTable(SpotFeatures)
photo_table = ScruTable(PhotoFeatures, name="picture", index=PhotoIndex)
