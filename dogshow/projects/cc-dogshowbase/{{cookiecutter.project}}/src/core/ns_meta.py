import datetime as dt

import datazimmer as dz


class Creature(dz.AbstractEntity):
    cid = dz.Index & str

    name = str
    date_of_birth = dz.Nullable(dt.datetime)


class Pet(dz.AbstractEntity):
    """most likely owned by pet owners, but not necessarily a creature"""

    # ^ this goes to description
    sex = str


class Dog(Creature, Pet):
    waist = dz.Nullable(float)


class Person(Creature):
    pass


class Relationship(dz.AbstractEntity):
    owner = dz.Index & Person
    dog = dz.Index & Dog

    since_birth = bool


class ResultType(dz.CompositeTypeBase):
    owner = Person
    pet = Dog
    prize = int


class BuildingInfoType(dz.CompositeTypeBase):
    floor = int
    door = int


class AddressType(dz.CompositeTypeBase):
    city = str
    street_address = str
    building = BuildingInfoType


# add in v0.0:     zip = str


class Competition(dz.AbstractEntity):
    competition_id = dz.Index & str

    prize_pool = int
    winner = ResultType
    runner_up = ResultType
    special_mention = ResultType


class Spotting(dz.AbstractEntity):
    dog_1 = Dog
    dog_2 = Dog
    place = AddressType


class Photo(dz.AbstractEntity):

    photo_id = dz.Index & str
    cuteness = float
    rel = Relationship


person_table = dz.ScruTable(Person)
dog_table = dz.ScruTable(Dog, partitioning_cols=[Dog.sex])
relationship_table = dz.ScruTable(Relationship)
competition_table = dz.ScruTable(Competition)
spot_table = dz.ScruTable(Spotting)
photo_table = dz.ScruTable(Photo)


test_base_url = dz.SourceUrl("https://sscu-budapest.github.io/")
