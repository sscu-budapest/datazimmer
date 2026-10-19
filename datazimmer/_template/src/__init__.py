"""Project pipeline. `dz run` imports `pipeline` from this package."""

from typing import Annotated

import polars as pl

from datazimmer import Entity, Index, Pipeline, Table

pipeline = Pipeline()


class Record(Entity):
    rid: Annotated[str, Index]
    value: float


records = Table(Record, "data/records")


@pipeline.step(outputs=[records])
def build():
    records.write(pl.DataFrame({"rid": ["a", "b"], "value": [1.0, 2.0]}))
