from datetime import datetime  # noqa: F401

from sscutils import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    Nullable,
    TableFactory,
    TableFeaturesBase,
)


class Competition(BaseEntity):
    pass


class Picture(BaseEntity):
    pass


class Relationship(BaseEntity):
    pass


class Spot(BaseEntity):
    pass


class Creature(BaseEntity):
    pass


class Person(Creature):
    pass


class Pet(BaseEntity):
    pass


class BuildingInfoType(CompositeTypeBase):
    floor = int
    door = int


class CompetitionIndex(IndexBase):
    competition_id = str


class DogFeatures(TableFeaturesBase):
    name = str
    date_of_birth = datetime
    waist = Nullable(float)
    sex = str


class DogIndex(IndexBase):
    dog_id = str


class PersonFeatures(TableFeaturesBase):
    name = str
    date_of_birth = Nullable(datetime)


class PersonIndex(IndexBase):
    person_id = str


class PictureIndex(IndexBase):
    photo_id = str


class RelationshipFeatures(TableFeaturesBase):
    since_birth = bool


class RelationshipIndex(IndexBase):
    owner = PersonIndex
    dog = DogIndex


class Dog(Creature, Pet):
    pass


class AddressType(CompositeTypeBase):
    city = str
    zip = str
    street_address = str
    building = BuildingInfoType


class ResultType(CompositeTypeBase):
    owner = PersonIndex
    pet = DogIndex
    prize = int


class CompetitionFeatures(TableFeaturesBase):
    prize_pool = int
    winner = ResultType
    runner_up = ResultType
    special_mention = ResultType


class PictureFeatures(TableFeaturesBase):
    cuteness = float
    rel = RelationshipIndex


class SpotFeatures(TableFeaturesBase):
    dog_1 = DogIndex
    dog_2 = DogIndex
    place = AddressType


table_factory = TableFactory("dogfirst")
competition_table = table_factory.create(
    features=CompetitionFeatures, subject_of_records=Competition, index=CompetitionIndex
)
dog_table = table_factory.create(
    features=DogFeatures, subject_of_records=Dog, index=DogIndex, partitioning_cols=["sex"]
)
person_table = table_factory.create(features=PersonFeatures, subject_of_records=Person, index=PersonIndex)
picture_table = table_factory.create(features=PictureFeatures, subject_of_records=Picture, index=PictureIndex)
relationship_table = table_factory.create(
    features=RelationshipFeatures, subject_of_records=Relationship, index=RelationshipIndex
)
spot_table = table_factory.create(features=SpotFeatures, subject_of_records=Spot)
