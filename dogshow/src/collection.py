"""aswan collection: scrape the kennel federation into the depot.

Five handlers cover the request/soup, request/json and relay paths plus a link
cascade; ``DogFederation`` is the :class:`datazimmer.DzAswan` collector that the
pipeline drives. ``url_root`` is read from ``DOGSHOW_SITE_URL`` so the test
suite can point every handler at the spawned fixture site with no code edits.
"""

import os

import aswan
from datazimmer import DzAswan

SITE_URL = os.environ.get("DOGSHOW_SITE_URL", "http://localhost:5005")


class BreedListHandler(aswan.RequestSoupHandler):
    """The registry index — cascades a page request per breed."""

    url_root = SITE_URL
    test_urls = ["/"]

    def parse(self, soup):
        hrefs = [a["href"] for a in soup.select("#registry a")]
        self.register_links_to_handler(hrefs, BreedPageHandler)
        return hrefs


class BreedPageHandler(aswan.RequestSoupHandler):
    """A breed page: breed attributes + the dogs (with their kennels)."""

    url_root = SITE_URL

    def parse(self, soup):
        dogs = [
            {
                "dog_id": tr["data-dog-id"],
                "name": tr.select_one(".dog-name").text,
                "kennel_id": tr["data-kennel-id"] or None,
                "weight_kg": float(tr["data-weight"]),
                "born_on": tr["data-born"],
            }
            for tr in soup.select("tr.dog")
        ]
        return {
            "breed_id": soup.find(id="breed-id").text,
            "name": soup.find(id="breed-name").text,
            "group": soup.find(id="breed-group").text,
            "origin": soup.find(id="breed-origin").text or None,
            "dogs": dogs,
        }


class KennelHandler(aswan.RequestJsonHandler):
    url_root = SITE_URL
    test_urls = ["/kennels.json"]

    def parse(self, obj):
        return obj["kennels"]


class ShowScheduleHandler(aswan.RequestJsonHandler):
    """The show schedule — registers each show's results feed to the relay."""

    url_root = SITE_URL
    test_urls = ["/shows.json"]

    def parse(self, obj):
        shows = obj["shows"]
        self.register_links_to_handler(
            [s["results_url"] for s in shows], ResultFeedHandler
        )
        return shows


class ResultFeedHandler(aswan.RelayHandler):
    """Results arrive via the relay bridge: an external worker fetches the feed
    and POSTs it back, exercising aswan's relay path end to end."""

    url_root = SITE_URL


class DogFederation(DzAswan):
    name = "dog-federation"
    starters = {
        BreedListHandler: ["/"],
        KennelHandler: ["/kennels.json"],
        ShowScheduleHandler: ["/shows.json"],
    }

    def prepare_run(self):
        self.project.max_cpu_use = 2


def get_collector() -> DogFederation:
    """A fresh collector handle.

    Not a module-level singleton: the depot root is resolved from
    ``ASWAN_DEPOT_ROOT`` when the handle is built, so a `DogFederation()` at
    import time would freeze it before the test fixture (or a deployment) gets
    to set it. Building on demand keeps the depot wherever the caller points.
    """
    return DogFederation()
