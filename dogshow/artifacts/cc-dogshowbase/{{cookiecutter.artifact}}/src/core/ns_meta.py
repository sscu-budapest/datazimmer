import datetime as dt

import datazimmer as dz


class Creature(dz.BaseEntity):
    pass


class Pet(dz.BaseEntity):
    """most likely owned by pet owners, but not necessarily a creature"""

    # ^ this goes to description
    pass


class Dog(Creature, Pet):
    pass


class Person(Creature):
    pass


class DogIndex(dz.IndexBase):
    dog_id = str


class PersonIndex(dz.IndexBase):
    person_id = str


class CompetitionIndex(dz.IndexBase):
    competition_id = str


class RelationshipIndex(dz.IndexBase):
    owner = PersonIndex
    dog = DogIndex


class PhotoIndex(dz.IndexBase):
    photo_id = str


class ResultType(dz.CompositeTypeBase):
    owner = PersonIndex
    pet = DogIndex
    prize = int


class BuildingInfoType(dz.CompositeTypeBase):
    floor = int
    door = int


class AddressType(dz.CompositeTypeBase):
    city = str
    street_address = str
    building = BuildingInfoType


# add in v0.0:     zip = str


class PersonFeatures(dz.TableFeaturesBase):
    name = str
    date_of_birth = dz.Nullable(dt.datetime)


class DogFeatures(dz.TableFeaturesBase):
    name = str
    date_of_birth = dt.datetime
    waist = dz.Nullable(float)
    sex = str


class RelationshipFeatures(dz.TableFeaturesBase):
    since_birth = bool


class CompetitionFeatures(dz.TableFeaturesBase):
    prize_pool = int
    winner = ResultType
    runner_up = ResultType
    special_mention = ResultType


class SpotFeatures(dz.TableFeaturesBase):
    dog_1 = DogIndex
    dog_2 = DogIndex
    place = AddressType


class PhotoFeatures(dz.TableFeaturesBase):
    cuteness = float
    rel = RelationshipIndex


person_table = dz.ScruTable(PersonFeatures, PersonIndex, subject_of_records=Person)
dog_table = dz.ScruTable(
    DogFeatures,
    DogIndex,
    subject_of_records=Dog,
    partitioning_cols=[DogFeatures.sex],
)
relationship_table = dz.ScruTable(RelationshipFeatures, RelationshipIndex)
competition_table = dz.ScruTable(CompetitionFeatures, CompetitionIndex)
spot_table = dz.ScruTable(SpotFeatures)
photo_table = dz.ScruTable(PhotoFeatures, index=PhotoIndex)
