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

A project is one importable package exposing a module-level `pipeline`, plus a `[tool.datazimmer]` table in `pyproject.toml`. No DVC, no metaclasses, no codegen DSL — plain classes, type hints and explicit polars DataFrames.

### Entities — typed schemas

```python
from datetime import datetime
from typing import Annotated
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
```

Entity-typed fields become foreign keys; the metadata (index/uniqueness, sort order, foreign keys) is what drives SQL materialization, ER diagrams and published manifests.

### Tables — schema-validated parquet (via shackleton)

```python
from datazimmer import Table
breeds = Table(Breed, "data/breeds")
breeds.write(df)          # validated + dtype-coerced on write
lf = breeds.read()        # polars LazyFrame
```

### Pipeline — explicit steps, hash-based skipping

```python
from datazimmer import Pipeline
pipeline = Pipeline()

@pipeline.step(outputs=[breeds, dogs])
def seed(): ...

@pipeline.step(inputs=[dogs], outputs=[breed_wins])
def aggregate(): ...
```

`dz run` topologically sorts the steps and skips any whose source and inputs are unchanged; each step's fingerprint is recorded as soon as it succeeds, under the project's state dir (`config.STATE_DIR`). Editing an upstream step re-runs everything downstream.

### Validate, publish, consume

```python
from datazimmer import Config, validate, publish, load
validate([breeds, dogs])                      # uniqueness + FK integrity
publish(Config.load(), [breeds, dogs])        # versioned upload + manifest
lf = load("s3://bucket/datasets/dogshow", "breeds")   # lazy, no download (local/s3/http)
```

Cross-project consumption is parquet-metadata-first; `dz schema codegen` optionally regenerates typed `Entity` classes from a published manifest.

## Configuration (`pyproject.toml`)

```toml
[tool.datazimmer]
name = "dogshow"
version = "0.1.0"
package = "src"

[tool.datazimmer.remote]
kind = "s3"                       # local | ssh | s3
url = "s3://bucket/datasets"

[tool.datazimmer.dependencies.elections]
source = "s3://bucket/datasets/electiondata"
version = "0.2.0"
```

Credentials use the standard env vars (`AWS_ACCESS_KEY_ID`, …); there is no custom auth layer.

## CLI

```bash
dz init <name>            # scaffold a new project from the template
dz run [--force]          # execute the pipeline (skips unchanged steps)
dz validate               # index uniqueness + foreign-key integrity
dz sql [--constr URL]     # load all tables into a relational database
dz draw [--out PATH]      # write a Mermaid ER diagram
dz publish                # upload tables + manifest to the remote
dz pull <source>          # download a published project into the cache
dz schema export          # JSON Schema per entity
dz schema codegen <src>   # Entity classes from a published manifest
```

`dz --help-all` prints every command's flags; `dz --complete <args…>` serves shell completion (dependency sources for `pull`, paths for `--out`/`--dest`).

## The dogshow

[`dogshow/`](dogshow) is the canonical example and the cross-package end-to-end suite. One story drives the whole ecosystem: aswan scrapes a (locally served) kennel federation into a versioned depot, this pipeline parses it into a typed star schema, and the result is validated, materialized to SQL + a Mermaid ER diagram, and published for URL consumption. It reads no external data and, under Docker, reports the combined coverage of all six packages:

```bash
cd dogshow && make test                                      # fast, self-contained
docker compose -f dogshow/docker/compose.yaml up --build     # combined coverage
```

## Ecosystem

| Library | Role |
| --- | --- |
| [shackleton](https://github.com/endremborza/shackleton) | Polars-based table storage |
| [aswan](https://github.com/endremborza/aswan) | Web data collection |
| [dirark](https://github.com/endremborza/dirark) | Content-addressed archival (aswan depot) |
| [sqlmermaid](https://github.com/endremborza/sqlmermaid) | ER diagram generation |
| [atqo](https://github.com/endremborza/atqo) | Task queue orchestration |

The remaining roadmap lives in [MODERNIZATION.md](MODERNIZATION.md).

## License

See [LICENSE](LICENSE).
