"""The pipeline + the typed tables it produces.

Kept free of step definitions so the step modules (``parsing``, ``analytics``)
can import from here without a cycle; ``__init__`` imports those for their
``@pipeline.step`` side effects.
"""

from datazimmer import Pipeline, Table

from . import meta

pipeline = Pipeline()

breeds = Table(meta.Breed, "data/breeds")
kennels = Table(meta.Kennel, "data/kennels")
dogs = Table(meta.Dog, "data/dogs")
shows = Table(meta.Show, "data/shows")
# partitioned by show to exercise shackleton's hash-free column partitioning
results = Table(meta.Result, "data/results", partition_cols=["show__show_id"])
breed_wins = Table(meta.BreedWins, "data/breed_wins")
kennel_standing = Table(meta.KennelStanding, "data/kennel_standing")

ALL_TABLES = [breeds, kennels, dogs, shows, results, breed_wins, kennel_standing]
