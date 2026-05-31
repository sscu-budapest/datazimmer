"""The dogshow star schema.

Rich enough to exercise the whole metadata surface: a nullable scalar
(``Breed.origin``), a nullable foreign key (``Dog.kennel``), single-column
indexes, and two FK-only aggregate tables with no index of their own.
"""

from datetime import datetime
from typing import Annotated

from datazimmer import Entity, Index


class Breed(Entity):
    """A recognized dog breed."""

    breed_id: Annotated[str, Index]
    name: str
    group: str
    origin: str | None


class Kennel(Entity):
    """A breeder/owner organization."""

    kennel_id: Annotated[str, Index]
    name: str
    country: str


class Dog(Entity):
    """A competing dog."""

    dog_id: Annotated[str, Index]
    name: str
    breed: Breed
    kennel: Kennel | None
    weight_kg: float
    born_on: datetime


class Show(Entity):
    """A dog show event."""

    show_id: Annotated[str, Index]
    held_on: datetime
    city: str
    country: str


class Result(Entity):
    """A dog's placement at a show."""

    result_id: Annotated[str, Index]
    dog: Dog
    show: Show
    rank: int
    points: float


class BreedWins(Entity):
    """First-place finishes per breed (aggregate, FK-only)."""

    breed: Breed
    wins: int


class KennelStanding(Entity):
    """Total points per kennel (aggregate, FK-only)."""

    kennel: Kennel
    total_points: float
