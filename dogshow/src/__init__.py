"""The dogshow — the canonical, self-contained showcase of the datazimmer
ecosystem and its cross-package end-to-end test suite.

One story drives every library: aswan scrapes a (locally served) kennel
federation into a versioned depot, a datazimmer pipeline parses that into a
typed star schema, and the result is validated, materialized to SQL + an ER
diagram, and published for URL-based consumption. See ``README.md``.

Importing this package builds the pipeline and registers every step (the
``parsing`` / ``analytics`` imports run their ``@pipeline.step`` decorators).
"""

from . import analytics, parsing  # noqa: F401 -- register pipeline steps
from .collection import get_collector
from .tables import (
    breed_wins,
    breeds,
    dogs,
    kennel_standing,
    kennels,
    pipeline,
    results,
    shows,
)

__all__ = [
    "breed_wins",
    "breeds",
    "dogs",
    "get_collector",
    "kennel_standing",
    "kennels",
    "pipeline",
    "results",
    "shows",
]
