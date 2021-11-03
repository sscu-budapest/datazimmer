from datetime import datetime  # noqa: F401

from sscutils import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    TableFeaturesBase,
)


class Creature(BaseEntity):
    pass


class Person(Creature):
    pass


class Pet(BaseEntity):
    pass


class Competition(BaseEntity):
    pass


class Picture(BaseEntity):
    pass


class Relationship(BaseEntity):
    pass


class Spot(BaseEntity):
    pass


class BuildingInfoType(CompositeTypeBase):
    floor = int
    door = int


class CompetitionIndex(IndexBase):
    competition_id = str


class DogFeatures(TableFeaturesBase):
    name = str
    date_of_birth = datetime
    waist = float
    sex = str


class DogIndex(IndexBase):
    dog_id = str


class PersonFeatures(TableFeaturesBase):
    name = str
    date_of_birth = datetime


class PersonIndex(IndexBase):
    person_id = str


class PictureIndex(IndexBase):
    photo_id = str


class RelationshipFeatures(TableFeaturesBase):
    since_birth = bool


class RelationshipIndex(IndexBase):
    owner = PersonIndex
    dog = DogIndex


class SpotFeatures(TableFeaturesBase):
    dog_1 = DogIndex
    dog_2 = DogIndex
    place = str


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
