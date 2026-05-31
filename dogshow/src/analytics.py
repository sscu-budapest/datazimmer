"""Aggregate steps — the FK-only tables, derived from the parsed entities.

These declare table inputs, so the pipeline orders them after ``parse`` and
re-runs them whenever the upstream results/dogs fingerprints change.
"""

import polars as pl

from .tables import breed_wins, dogs, kennel_standing, pipeline, results


@pipeline.step(inputs=[results, dogs], outputs=[breed_wins])
def aggregate_breed_wins():
    wins = (
        results.collect()
        .filter(pl.col("rank") == 1)
        .join(
            dogs.collect().select("dog_id", "breed__breed_id"),
            left_on="dog__dog_id",
            right_on="dog_id",
        )
        .group_by("breed__breed_id")
        .agg(pl.len().alias("wins"))
    )
    breed_wins.write(wins)


@pipeline.step(inputs=[results, dogs], outputs=[kennel_standing])
def aggregate_kennel_standing():
    standing = (
        results.collect()
        .join(
            dogs.collect().select("dog_id", "kennel__kennel_id"),
            left_on="dog__dog_id",
            right_on="dog_id",
        )
        .filter(pl.col("kennel__kennel_id").is_not_null())
        .group_by("kennel__kennel_id")
        .agg(pl.col("points").sum().alias("total_points"))
    )
    kennel_standing.write(standing)
