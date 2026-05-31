"""The fake kennel-federation website + its fixture data.

Routes (the surface aswan's handlers scrape):

* ``/``                     registry index, links to every breed page (Soup)
* ``/breed/<id>``           breed page: breed attrs + a table of its dogs (Soup)
* ``/kennels.json``         the kennel directory (Json)
* ``/shows.json``           the show schedule, each with a relayed results feed
* ``/results/<id>.json``    a show's results (fetched by the relay worker)

All data lives in ``DATA`` below — nothing is read from outside the process.
"""

federation_port = 5005


# --- fixture data ---------------------------------------------------------

DATA = {
    "breeds": [
        {
            "breed_id": "lab",
            "name": "Labrador",
            "group": "sporting",
            "origin": "Canada",
        },
        {"breed_id": "pug", "name": "Pug", "group": "toy", "origin": "China"},
        {
            "breed_id": "collie",
            "name": "Border Collie",
            "group": "herding",
            "origin": None,
        },
        {
            "breed_id": "dane",
            "name": "Great Dane",
            "group": "working",
            "origin": "Germany",
        },
    ],
    "kennels": [
        {"kennel_id": "k1", "name": "Lakeside", "country": "US"},
        {"kennel_id": "k2", "name": "Highland", "country": "UK"},
    ],
    # kennel=None exercises the nullable foreign key on Dog
    "dogs": [
        {
            "dog_id": "d1",
            "name": "Rex",
            "breed_id": "lab",
            "kennel_id": "k1",
            "weight_kg": 30.0,
            "born_on": "2019-03-01",
        },
        {
            "dog_id": "d2",
            "name": "Bella",
            "breed_id": "pug",
            "kennel_id": "k2",
            "weight_kg": 8.0,
            "born_on": "2020-06-15",
        },
        {
            "dog_id": "d3",
            "name": "Max",
            "breed_id": "lab",
            "kennel_id": None,
            "weight_kg": 28.5,
            "born_on": "2018-11-20",
        },
        {
            "dog_id": "d4",
            "name": "Luna",
            "breed_id": "collie",
            "kennel_id": "k1",
            "weight_kg": 20.0,
            "born_on": "2021-01-10",
        },
        {
            "dog_id": "d5",
            "name": "Zeus",
            "breed_id": "dane",
            "kennel_id": "k2",
            "weight_kg": 54.0,
            "born_on": "2017-07-07",
        },
    ],
    "shows": [
        {
            "show_id": "s1",
            "held_on": "2024-05-01T00:00:00",
            "city": "Budapest",
            "country": "HU",
        },
        {
            "show_id": "s2",
            "held_on": "2024-09-01T00:00:00",
            "city": "Vienna",
            "country": "AT",
        },
    ],
    "results": {
        "s1": [
            {
                "result_id": "r1",
                "show_id": "s1",
                "dog_id": "d1",
                "rank": 1,
                "points": 10.0,
            },
            {
                "result_id": "r2",
                "show_id": "s1",
                "dog_id": "d3",
                "rank": 2,
                "points": 6.0,
            },
            {
                "result_id": "r5",
                "show_id": "s1",
                "dog_id": "d5",
                "rank": 3,
                "points": 3.0,
            },
        ],
        "s2": [
            {
                "result_id": "r3",
                "show_id": "s2",
                "dog_id": "d2",
                "rank": 1,
                "points": 10.0,
            },
            {
                "result_id": "r4",
                "show_id": "s2",
                "dog_id": "d4",
                "rank": 2,
                "points": 6.0,
            },
        ],
    },
}


def dogs_for_breed(breed_id: str) -> list[dict]:
    return [d for d in DATA["dogs"] if d["breed_id"] == breed_id]


# --- flask app ------------------------------------------------------------


def federation_app_creator():  # pragma: no cover - runs in a spawned subprocess
    from flask import Flask, abort, jsonify

    app = Flask(__name__)

    @app.route("/")
    def index():
        items = "".join(
            f'<li><a href="/breed/{b["breed_id"]}">{b["name"]}</a></li>'
            for b in DATA["breeds"]
        )
        return f"<html><body><ul id='registry'>{items}</ul></body></html>"

    @app.route("/breed/<breed_id>")
    def breed_page(breed_id):
        breed = next((b for b in DATA["breeds"] if b["breed_id"] == breed_id), None)
        if breed is None:
            abort(404)
        rows = "".join(
            f'<tr class="dog" data-dog-id="{d["dog_id"]}" '
            f'data-kennel-id="{d["kennel_id"] or ""}" '
            f'data-weight="{d["weight_kg"]}" data-born="{d["born_on"]}">'
            f'<td class="dog-name">{d["name"]}</td></tr>'
            for d in dogs_for_breed(breed_id)
        )
        return (
            "<html><body>"
            f'<h1 id="breed-name">{breed["name"]}</h1>'
            f'<span id="breed-id">{breed["breed_id"]}</span>'
            f'<span id="breed-group">{breed["group"]}</span>'
            f'<span id="breed-origin">{breed["origin"] or ""}</span>'
            f'<table id="dogs">{rows}</table>'
            "</body></html>"
        )

    @app.route("/kennels.json")
    def kennels():
        return jsonify({"kennels": DATA["kennels"]})

    @app.route("/shows.json")
    def shows():
        payload = [
            {**s, "results_url": f"/results/{s['show_id']}.json"} for s in DATA["shows"]
        ]
        return jsonify({"shows": payload})

    @app.route("/results/<show_id>.json")
    def results(show_id):
        if show_id not in DATA["results"]:
            abort(404)
        return jsonify(DATA["results"][show_id])

    return app
