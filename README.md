# datazimmer

[![pypi](https://img.shields.io/pypi/v/datazimmer.svg)](https://pypi.org/project/datazimmer/) [![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.7499121.svg)](https://doi.org/10.5281/zenodo.7499121)

Scientific data pipeline orchestrator. Define typed entity schemas, build reproducible polars pipelines, validate foreign-key integrity, and publish versioned datasets consumed by URL.

Part of the [sscu-budapest](https://github.com/sscu-budapest) ecosystem for open scientific data.

## Install

```bash
uv add datazimmer
uv add "datazimmer[collect]"   # + aswan web collection
uv add "datazimmer[s3]"        # + S3 publishing/consumption
uv add "datazimmer[zenodo]"    # + Zenodo deposition
```

## Concepts

A project is one importable package exposing a module-level `pipeline`, configured by a `[tool.datazimmer]` table in `pyproject.toml`. Everything is plain classes, type hints and polars frames.

### Entities — typed schemas

```python
from typing import Annotated, Literal
from datazimmer import Entity, Index

class Breed(Entity):
    breed_id: Annotated[str, Index]   # index = unique key, sort order
    name: str
    origin: str | None                # nullable

class Dog(Entity):
    dog_id: Annotated[str, Index]
    name: str
    breed: Breed                      # foreign key -> breed__breed_id
    weight_kg: float
    sex: Literal["m", "f"]            # categorical -> pl.Enum
```

The index and the foreign keys drive SQL materialization, ER diagrams and published manifests.

### Tables — schema-validated parquet (via shackleton)

```python
from datazimmer import Table
breeds = Table(Breed, "data/breeds")
breeds.write(df)          # validated + dtype-coerced
breeds.replace(df)        # upsert on the index, composite or not
lf = breeds.read()        # polars LazyFrame
```

Files stay sorted by the entity's index.

### Pipeline — explicit steps, hash-based skipping

```python
from datazimmer import Pipeline
pipeline = Pipeline()

@pipeline.step(outputs=[breeds, dogs])
def seed(): ...

@pipeline.step(inputs=[dogs], outputs=[breed_wins])
def aggregate(): ...
```

An input is a `Table`, a path, or another step. `dz run` runs the steps in dependency order and skips one whose source and inputs are unchanged since it last succeeded and whose outputs exist, so editing a step re-runs it and everything downstream. Fingerprints are kept in the project's state dir (`config.STATE_DIR`).

### Validate, publish, consume

```python
from datazimmer import Config, validate, publish, load, load_dependency
validate([breeds, dogs])                                  # uniqueness + FK integrity
publish(Config.load(), [breeds, dogs])                    # versioned upload + manifest
lf = load("s3://bucket/datasets/dogshow", "breeds")       # lazy scan (local/s3/http)
lf = load_dependency(Config.load(), "elections", "precincts")  # a [tool.datazimmer.dependencies] entry
```

A consumer needs only the URL: the manifest carries each table's schema and files. `dz schema codegen` turns it into typed `Entity` classes when you want them.

### Collection — aswan collectors

```python
from datazimmer import DzAswan

class Lichess(DzAswan):
    name = "lichess"                  # the aswan depot
    cadence = "1h"                    # count + unit of CADENCE_UNITS; unset = only when named
    starters = {GetTournaments: ["https://lichess.org/api/tournament"]}

@pipeline.step(outputs=[tournaments])
def parse():
    tournaments.write(pl.DataFrame([e.content for e in Lichess().events(GetTournaments)]))
```

`dz collect` imports the `collectors` modules and runs every collector whose cadence has passed since its last attempt, into the aswan depot root (`ASWAN_DEPOT_ROOT`); with `ASWAN_REMOTE` set, it pulls the depot first and pushes it after. A failed run counts as an attempt, so a broken collector retries once per cadence. Call it from a timer (cron, systemd) as often as the shortest cadence needs.

## Configuration (`pyproject.toml`)

```toml
[tool.datazimmer]
name = "dogshow"
version = "0.1.0"
package = "src"
collectors = ["src.collection"]   # modules whose DzAswan classes `dz collect` runs

[tool.datazimmer.remote]
kind = "s3"                       # local | ssh | s3
url = "s3://bucket/datasets"

[tool.datazimmer.dependencies.elections]
source = "s3://bucket/datasets/electiondata"
version = "0.2.0"                 # omit for the latest
```

Credentials are the standard env vars (`AWS_ACCESS_KEY_ID`, `ZENODO_TOKEN`, …). Zenodo deposition is Python-only: `datazimmer.zenodo.ZenodoDeposit` creates a draft, uploads files and optionally publishes it.

## CLI

```bash
dz init <name>            # scaffold a new project
dz run [--force]          # execute the pipeline, skipping unchanged steps
dz collect [names…]       # run due collectors, or the named ones [--force] [--status]
dz validate               # index uniqueness + foreign-key integrity
dz sql [--constr URL]     # load all tables into a relational database
dz draw [--out PATH]      # write a Mermaid ER diagram
dz publish                # upload tables + manifest to the remote
dz pull <source>          # download a published project into the cache
dz schema export          # JSON Schema per entity
dz schema codegen <src>   # Entity classes from a published manifest
```

`dz --help-all` prints every command's flags; `dz --complete <args…>` serves shell completion.

## The dogshow

[`dogshow/`](dogshow) is the canonical example and the cross-package end-to-end suite: aswan scrapes a locally served kennel federation into a depot, the pipeline parses it into a typed star schema, and the result is validated, materialized to SQL + an ER diagram, and published. It reads no external data.

```bash
cd dogshow && make test                                      # self-contained suite
docker compose -f dogshow/docker/compose.yaml up --build     # + the ssh backend, combined coverage of every package
```

## Ecosystem

| Library | Role |
| --- | --- |
| [shackleton](https://github.com/endremborza/shackleton) | Polars table storage |
| [aswan](https://github.com/endremborza/aswan) | Web data collection |
| [dirark](https://github.com/endremborza/dirark) | Content-addressed archival (aswan depot) |
| [atqo](https://github.com/endremborza/atqo) | Task queue orchestration (aswan workers) |
| [sqlmermaid](https://github.com/endremborza/sqlmermaid) | ER diagram generation |
| [protocli](https://github.com/endremborza/protocli) | The `dz` CLI dispatcher |

## License

See [LICENSE](LICENSE).
