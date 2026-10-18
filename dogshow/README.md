# The Dogshow

The **canonical, self-contained showcase of the entire datazimmer ecosystem** — and its end-to-end test suite. One small, coherent story drives every package:

> A fictional kennel federation runs a website of breed registries, kennels, show schedules and result feeds. The dogshow project **collects** that site with aswan, **parses** it into a typed star schema with datazimmer, **validates** foreign-key integrity, **materializes** a SQL database + ER diagram, and **publishes** a versioned dataset that downstream projects consume by URL.

No external data is ever read: the "federation website" is a Flask app the test suite serves locally, so the full `collect → run → validate → publish → load` loop runs from a clean checkout (or a single `docker compose up`).

## What it exercises

| Package | Role in the dogshow | Exercised by |
| --- | --- | --- |
| [aswan](https://github.com/endremborza/aswan) | scrape the federation site (HTTP + relay handlers, proxy, retries, link cascades), versioned depot, push/pull | `collection.py`, `tests/test_collection.py` |
| [atqo](https://github.com/endremborza/atqo) | parallel scheduler + actor slots running collection in subprocess workers | (transitively, via aswan `Project.run`) |
| [dirark](https://github.com/endremborza/dirark) | content-addressed object store backing the depot; tar.zst ark + rsync dedup on push/pull | (transitively, via aswan depot) |
| [shackleton](https://github.com/endremborza/shackleton) | `TableRepo` parquet storage: schema-validated writes, column partitioning, `max_records` splitting, append, compaction | `tests/test_table.py` |
| [sqlmermaid](https://github.com/endremborza/sqlmermaid) | ER diagram from the materialized SQL metadata | `tests/test_sql.py`, `tests/test_docs.py` (`dz draw`) |
| **datazimmer** | the spine: `Entity` schema + FK metadata, `Table`, `Pipeline` (topo + hash-skip), `validate`, `to_sql`, `publish`/`pull`/`load`/`load_dependency`/`schema codegen`, `config`, `cli`, `DzAswan` | every test module (reuse story in `tests/test_reuse.py`) |

The explicit goal is **maximal measured coverage of all six packages from one suite** — see [Coverage](#coverage).

## The schema

The single source of truth is [`src/meta.py`](src/meta.py): seven plain, annotated `Entity` classes. It is shaped to exercise the whole metadata surface — a nullable scalar (`Breed.origin`), a nullable foreign key (`Dog.kennel`), single-column indexes, partitioning (`Result` by show), and two FK-only aggregate tables with no index of their own (`BreedWins`, `KennelStanding`).

The entity-relationship diagram is **generated** from those classes by `dz draw` and committed at [`docs/schema.md`](docs/schema.md) (GitHub renders the Mermaid block). `tests/test_docs.py` regenerates it and fails if it has drifted, so the diagram can never go stale against the schema — run `make erd` after a schema change.

## Layout

```
dogshow/
  README.md            # this file
  PLAN.md              # open forward work (tracked via git)
  Makefile             # test / report / showcase / erd / serve / clean
  pyproject.toml       # [tool.datazimmer] + editable sibling sources + coverage config
  sitecustomize.py     # starts coverage in aswan's spawned worker subprocesses
  docs/
    schema.md          # generated ER diagram (`dz draw`; guarded by test_docs.py)
  src/
    __init__.py        # exposes `pipeline`, `get_collector`, the tables
    meta.py            # the Entity schemas (the schema's source of truth)
    tables.py          # the pipeline + the typed tables (no step defs -> no cycle)
    collection.py      # aswan handlers (Soup/Json/Relay) + the DzAswan collector
    parsing.py         # @pipeline.step: collect, then collected events -> tables
    analytics.py       # @pipeline.step: aggregates (BreedWins, KennelStanding)
  tests/
    conftest.py        # boots the federation site; collects once per session,
                       #   clones the built outputs into a tmp project per test
    site/              # the self-contained Flask "federation" + the relay worker
                       #   (`python -m tests.site` serves it for a manual run)
    test_collection.py # aswan/atqo/dirark path
    test_table.py      # shackleton features (partitioning, splitting, compaction)
    test_pipeline.py   # produced entities, aggregates, hash-based skipping
    test_validate.py   # uniqueness + FK integrity (incl. nullable FK) + custom checks
    test_sql.py        # to_sql + sqlmermaid ER diagram
    test_publish.py    # publish/load/codegen over local|s3(moto)|http|ssh backends
    test_reuse.py      # data + metadata reuse: codegen roundtrip, dependency consume
    test_docs.py       # the committed ER diagram stays in sync with the schema
    test_e2e.py        # the whole loop through the `dz` CLI, as a smoke test
  docker/
    Dockerfile         # python:3.12-slim + uv + rsync + openssh + gosu; bind-mounted
    compose.yaml       # one self-contained runner (binds the monorepo at /code)
    entrypoint.sh      # root: sshd + ssh-to-self, then drops to the host user
    run-coverage.sh    # (host user) uv sync + a hard-bounded `report.py`
  report.py            # runs the suite under coverage -> REPORT.md (runtime +
                       #   per-library coverage); the README's numbers source
  showcase.py          # one-command hand-driven run: site + relay -> the real
                       #   `dz` CLI loop -> teardown, bounded (make showcase)
  REPORT.md            # generated, tracked: the run report the README links to
```

## Running

The `Makefile` is the entrypoint; every target runs through `uv` against the live editable sibling tree.

```bash
cd dogshow

make test        # the full self-contained suite (in-process site + relay; s3 via
                 # moto, http via a throwaway server; ssh skips unless
                 # DOGSHOW_SSH_REMOTE is set)
make report      # the same suite under coverage -> REPORT.md + coverage/html
make showcase    # boot the site + run the real `dz` loop end to end (one command)
make erd         # regenerate docs/schema.md from src/meta.py

# the full dockerized cross-package coverage run (adds the real rsync-over-ssh
# backend; combines coverage of every package):
docker compose -f docker/compose.yaml up --build --abort-on-container-exit
```

`make showcase` (`showcase.py`) drives the showcase the way a user would, in one bounded command: it boots the federation site + relay worker, runs the real `dz` CLI through `run → validate → draw → sql → publish → pull`, and tears the site down — every artifact stays inside `dogshow/` and `make clean` wipes it. For a fully hand-driven session instead, `make serve` brings the federation up long-lived (open it in a browser, run ad-hoc `dz` commands against it).

## Coverage

`make report` (or the dockerized run) writes two artifacts and copies **no numbers into prose** so nothing here can go stale:

- **[`REPORT.md`](REPORT.md)** — the single source of truth: total test runtime, a per-library coverage table for all six packages, and the slowest tests, regenerated every run (tracked, so this link resolves on a fresh checkout).
- **`coverage/html/index.html`** — the filtered, browsable line-by-line report. The `omit`/`skip_empty` config drops every package's own tests, fixtures and import-only files, so what you browse is the measured library surface, nothing else.

What makes the measurement interesting is *what gets measured*. Collection runs handler code in **spawned subprocesses** (atqo `MultiProcAPI`) and the relay server in a **worker thread**, so coverage is configured with `concurrency = ["multiprocessing", "thread"]`, `parallel = true`, `sigterm = true`, and a top-level `sitecustomize.py` that calls `coverage.process_startup()` in each worker (gated on `COVERAGE_PROCESS_START`) — the same approach aswan uses in its own suite. `source` lists all six editable packages, so one `coverage combine` reports the whole ecosystem from a single suite; that atqo's scheduler and aswan's `connection_session` show coverage at all is the proof the subprocess capture works.

The dockerized runner is self-contained: it `ssh`'s to itself for a real rsync-over-ssh target (the `ssh` backend), serves published data over real HTTP in-process (the `http` backend) and mocks the S3 client with `moto` (the `s3` backend), so every remote backend's code is exercised without external services. It runs the suite **as the host user** (dropped from root after starting sshd), so a finished — or crashed — run leaves no root-owned files in your tree; `init: true` + a hard `timeout` guarantee the container always exits. Worker code is *under*-reported (atqo terminates workers before coverage fully flushes); the rest of the gap is features the dogshow deliberately leaves to each library's own suite (`zenodo`, the Dash `monitor_app`, the `legacy` depot converter, the browser handler, the authenticated-proxy path).

Nothing can hang the runner: each test is bounded by `--timeout`, the whole session by `--session-timeout`, and — because those are Python-level timers that a native GIL-holding deadlock could outlast — `report.py` runs pytest in its own process group under `DOGSHOW_SUITE_TIMEOUT` and SIGKILLs the group on expiry.

Open forward work lives in [PLAN.md](PLAN.md).
