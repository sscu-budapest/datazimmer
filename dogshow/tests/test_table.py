"""shackleton's TableRepo surface, through datazimmer's Table.

Partitioned reads come from the real pipeline output; the splitting / compaction
/ append paths are exercised on a standalone table to keep them explicit.
"""

import polars as pl

from datazimmer import Table

from src import meta


def test_results_partitioned_by_show(built):
    # one parquet file per show partition (HashPartitioner-free column split)
    assert built.results._repo.n_files == 2
    s1 = built.results.read_partition({"show__show_id": "s1"}).collect()
    assert s1.height == 3 and set(s1["show__show_id"].unique()) == {"s1"}


def test_max_records_split_append_compact(project, tmp_path):
    t = Table(meta.Breed, tmp_path / "breeds_repo", max_records=2)
    t.write(
        pl.DataFrame(
            {
                "breed_id": [f"b{i}" for i in range(5)],
                "name": ["n"] * 5,
                "group": ["g"] * 5,
                "origin": [None] * 5,
            }
        )
    )
    assert t._repo.n_files == 3  # 5 rows / 2 per file
    assert t.n_rows() == 5

    t.append(
        pl.DataFrame(
            {"breed_id": ["b9"], "name": ["n"], "group": ["g"], "origin": ["o"]}
        )
    )
    assert t.n_rows() == 6

    t.compact(target_rows=1000)
    assert t._repo.n_files == 1
    assert t.n_rows() == 6
