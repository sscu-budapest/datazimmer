"""Pipeline steps that turn collected pages into typed entity tables.

``collect`` drives aswan (it has no table output, so it runs once then is
skipped until its source changes); ``parse`` reshapes the parsed events the
depot holds into the breed/kennel/dog/show/result tables. The raw events stay
in aswan's versioned, content-addressed depot — datazimmer owns only the typed
side of the boundary.
"""

from datetime import datetime

import polars as pl

from .collection import (
    BreedPageHandler,
    KennelHandler,
    ResultFeedHandler,
    ShowScheduleHandler,
    get_collector,
)
from .tables import breeds, dogs, kennels, pipeline, results, shows


@pipeline.step(outputs=[])
def collect():
    get_collector().run()


@pipeline.step(inputs=[collect], outputs=[breeds, kennels, dogs, shows, results])
def parse():
    col = get_collector()

    breed_events = [e.content for e in col.events(BreedPageHandler)]
    breeds.write(
        pl.DataFrame(
            [
                {k: ev[k] for k in ("breed_id", "name", "group", "origin")}
                for ev in breed_events
            ]
        )
    )
    dogs.write(
        pl.DataFrame(
            [
                {
                    "dog_id": d["dog_id"],
                    "name": d["name"],
                    "breed__breed_id": ev["breed_id"],
                    "kennel__kennel_id": d["kennel_id"],
                    "weight_kg": d["weight_kg"],
                    "born_on": datetime.fromisoformat(d["born_on"]),
                }
                for ev in breed_events
                for d in ev["dogs"]
            ]
        )
    )

    kennels.write(
        pl.DataFrame([k for e in col.events(KennelHandler) for k in e.content])
    )

    shows.write(
        pl.DataFrame(
            [
                {
                    "show_id": s["show_id"],
                    "held_on": datetime.fromisoformat(s["held_on"]),
                    "city": s["city"],
                    "country": s["country"],
                }
                for e in col.events(ShowScheduleHandler)
                for s in e.content
            ]
        )
    )

    results.write(
        pl.DataFrame(
            [
                {
                    "result_id": r["result_id"],
                    "dog__dog_id": r["dog_id"],
                    "show__show_id": r["show_id"],
                    "rank": r["rank"],
                    "points": r["points"],
                }
                for e in col.events(ResultFeedHandler)
                for r in e.content
            ]
        )
    )
