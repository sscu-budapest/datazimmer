"""The pipeline: produced tables, aggregates, and hash-based skipping."""


def test_produced_entities(built):
    assert built.breeds.n_rows() == 4
    assert built.kennels.n_rows() == 2
    assert built.dogs.n_rows() == 5
    assert built.shows.n_rows() == 2
    assert built.results.n_rows() == 5


def test_aggregates(built):
    wins = built.breed_wins.collect()
    # one rank-1 finish each for lab (d1@s1) and pug (d2@s2)
    assert dict(zip(wins["breed__breed_id"], wins["wins"])) == {"lab": 1, "pug": 1}

    standing = built.kennel_standing.collect()
    points = dict(zip(standing["kennel__kennel_id"], standing["total_points"]))
    # k1: d1(10)+d4(6)=16 ; k2: d5(3)+d2(10)=13 ; d3's null kennel is excluded
    assert points == {"k1": 16.0, "k2": 13.0}


def test_rerun_skips_everything(built):
    # nothing changed since the fixture ran the whole pipeline
    assert built.pipeline.run() == []
