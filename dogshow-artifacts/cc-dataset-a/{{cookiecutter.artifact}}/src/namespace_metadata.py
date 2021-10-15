import datetime as dt

from sscutils import CompositeTypeBase, IndexBase, ScruTable, TableFeaturesBase


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
    date_of_birth = dt.datetime


class DogFeatures(TableFeaturesBase):
    name = str
    date_of_birth = dt.datetime
    waist = float
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
    place = str


class PhotoFeatures(TableFeaturesBase):
    cuteness = float
    rel = RelationshipIndex


persons_table = ScruTable(PersonFeatures, PersonIndex)
dogs_table = ScruTable(
    DogFeatures, DogIndex, partitioning_cols=[DogFeatures.sex]
)
relationships_table = ScruTable(RelationshipFeatures, RelationshipIndex)
competitions_table = ScruTable(CompetitionFeatures, CompetitionIndex)
spots_table = ScruTable(SpotFeatures)
photos_table = ScruTable(PhotoFeatures, name="pictures", index=PhotoIndex)
