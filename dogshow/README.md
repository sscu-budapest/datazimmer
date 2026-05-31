# The Dogshow

The **canonical, self-contained showcase of the entire datazimmer ecosystem** —
and its end-to-end test suite. One small, coherent story drives every package:

> A fictional kennel federation runs a website of breed registries, kennels,
> show schedules and result feeds. The dogshow project **collects** that site
> with aswan, **parses** it into a typed star schema with datazimmer,
> **validates** foreign-key integrity, **materializes** a SQL database + ER
> diagram, and **publishes** a versioned dataset that downstream projects
> consume by URL.

No external data is ever read: the "federation website" is a Flask app the test
suite serves locally, so the full `collect → run → validate → publish → load`
loop runs from a clean checkout (or a single `docker compose up`).

## What it exercises

| Package | Role in the dogshow | Exercised by |
|---------|---------------------|--------------|
| [aswan](https://github.com/endremborza/aswan) | scrape the federation site (HTTP + relay handlers, proxy, retries, link cascades), versioned depot, push/pull | `collection.py`, `tests/test_collection.py` |
| [atqo](https://github.com/endremborza/atqo) | parallel scheduler + actor slots running collection in subprocess workers | (transitively, via aswan `Project.run`) |
| [dirark](https://github.com/endremborza/dirark) | content-addressed object store backing the depot; tar.zst ark + rsync dedup on push/pull | (transitively, via aswan depot) |
| [shackleton](https://github.com/endremborza/shackleton) | `TableRepo` parquet storage: schema-validated writes, hash partitioning, `max_records` splitting, compaction, batch writers | `tests/test_table.py` |
| [sqlmermaid](https://github.com/endremborza/sqlmermaid) | ER diagram from the materialized SQL metadata | `tests/test_sql.py` (`dz draw`) |
| **datazimmer** | the spine: `Entity` schema + FK metadata, `Table`, `Pipeline` (topo + hash-skip), `validate`, `to_sql`, `publish`/`pull`/`load`/`schema codegen`, `config`, `cli`, `DzAswan`, `zenodo` | every test module |

The explicit goal is **maximal measured coverage of all six packages from one
suite** — see [Coverage](#coverage).

## The schema

A star schema rich enough to exercise nullable foreign keys, composite-free
single-column indexes, FK-only aggregate tables, and partitioned tables:

```
Breed ─┐                       Kennel ─┐
       ├─< Dog >─── Result >── Show     │
Kennel ┘            │                   │
                    └──> BreedWins (agg, FK-only)
                    └──> KennelStanding (agg, FK-only)
```

| Entity | Index | Notable fields |
|--------|-------|----------------|
| `Breed` | `breed_id` | `origin: str \| None` (nullable) |
| `Kennel` | `kennel_id` | `country` |
| `Dog` | `dog_id` | `breed: Breed`, `kennel: Kennel \| None` (nullable FK) |
| `Show` | `show_id` | `held_on: datetime` |
| `Result` | `result_id` | `dog: Dog`, `show: Show`, `rank`, `points` — partitioned by show |
| `BreedWins` | *(none)* | `breed: Breed`, `wins` — FK-only aggregate |
| `KennelStanding` | *(none)* | `kennel: Kennel`, `total_points` — FK-only aggregate |

## Layout

```
dogshow/
  README.md            # this file
  PLAN.md              # the build plan (phased; tracked via git)
  pyproject.toml       # [tool.datazimmer] + editable sibling sources + coverage config
  sitecustomize.py     # starts coverage in aswan's spawned worker subprocesses
  src/
    __init__.py        # exposes `pipeline`, `get_collector`, the tables
    meta.py            # the Entity schemas above
    tables.py          # the pipeline + the typed tables (no step defs -> no cycle)
    collection.py      # aswan handlers (Soup/Json/Relay) + the DzAswan collector
    parsing.py         # @pipeline.step: collect, then collected events -> tables
    analytics.py       # @pipeline.step: aggregates (BreedWins, KennelStanding)
  tests/
    conftest.py        # boots the federation site; tmp depot/data/remote per test
    site/              # the self-contained Flask "federation" + the relay worker
    test_collection.py # aswan/atqo/dirark path
    test_table.py      # shackleton features (partitioning, splitting, compaction)
    test_pipeline.py   # produced entities, aggregates, hash-based skipping
    test_validate.py   # uniqueness + FK integrity (incl. nullable FK) + custom checks
    test_sql.py        # to_sql + sqlmermaid ER diagram
    test_publish.py    # publish/load/codegen over local|s3(moto)|http|ssh backends
    test_e2e.py        # the whole loop through the `dz` CLI, as a smoke test
  docker/
    Dockerfile         # python:3.12-slim + uv + rsync + openssh; ecosystem bind-mounted
    compose.yaml       # one self-contained runner (binds the monorepo at /code)
    run-coverage.sh    # ssh-to-self + uv sync + coverage run + combine + report
```

## Running

```bash
# locally, from the dogshow dir (in-process site + relay; s3 via moto, http via
# a throwaway server; the ssh backend test skips unless DOGSHOW_SSH_REMOTE is set)
cd dogshow && uv run pytest

# the full dockerized cross-package coverage run (adds the real rsync-over-ssh
# backend and prints the combined coverage of every package)
docker compose -f dogshow/docker/compose.yaml up --build --abort-on-container-exit

# the showcase itself, as a user would drive it (with the federation site up)
cd dogshow && dz run && dz validate && dz draw && dz publish
```

## Coverage

Collection runs handler code in **spawned subprocesses** (atqo `MultiProcAPI`)
and the relay server in a **worker thread**, so coverage is configured with
`concurrency = ["multiprocessing", "thread"]`, `parallel = true`, `sigterm =
true`, and a top-level `sitecustomize.py` that calls `coverage.process_startup()`
in each worker (gated on `COVERAGE_PROCESS_START`) — the same approach aswan uses
in its own suite. `source` lists all six editable packages, so one `coverage
combine` reports the whole ecosystem from a single suite.

The dockerized runner is self-contained: it `ssh`'s to itself for a real
rsync-over-ssh target (the `ssh` backend), serves published data over real HTTP
in-process (the `http` backend) and mocks the S3 client with `moto` (the `s3`
backend), so every remote backend's code is exercised without external services.
The selenium/browser handler and the authenticated-proxy path are out of scope
here — aswan's own suite covers them.

One suite genuinely measures all six packages. The full dockerized run (18/18,
ssh backend included) reports, in one combined coverage run:

| Package | Representative module coverage |
|---------|-------------------------------|
| datazimmer | `validate` 97% · `sql` 96% · `remote` 96% · `schema` 93% · `table` 87% · `pipeline` 85% · `aswan_integration` 83% · `cli` 80% |
| aswan | `models` 94% · `metadata_handling` 93% · `project` 79% · `url_handler` 68% · `security` 68% · `object_store` 58% · `connection_session` 27% |
| atqo | `exchange` 89% · `distributed_apis` 80% · `bases` 76% · `core` 75% |
| dirark | `storage` 70% · `blob` 59% |
| shackleton | `core` 63% |
| sqlmermaid | 65% |

That atqo's scheduler/exchange and aswan's `connection_session` show coverage at
all is the proof point — they run **only inside spawned workers**, so the
`sitecustomize` + `COVERAGE_PROCESS_START` capture is working. Worker code is
*under*-reported (e.g. the relay session's flask thread): atqo terminates
workers before coverage fully flushes, so the relay's executed lines don't all
land in the data file even though the relay tests pass. The remaining gaps are
genuinely unexercised features here — `zenodo`, the Dash `monitor_app`, the
`legacy` depot converter, browser/proxy handlers — covered by each library's own
suite.

The build plan and current status live in [PLAN.md](PLAN.md).
