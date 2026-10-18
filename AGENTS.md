# datazimmer

Scientific data pipeline orchestrator: typed entity schemas → schema-validated polars tables → explicit pipelines → versioned publish/consume by URL. The hub of a forward-only rewrite of the sscu-budapest data ecosystem — see [MODERNIZATION.md](MODERNIZATION.md) for the living plan and remaining phases.

The rewrite dropped DVC, the colassigner metaclass DSL, pandas, typer, cookiecutter, zimmauth, metazimmer and parquetranger. Core deps are now polars, sqlalchemy, shackleton and sqlmermaid; everything else is an optional extra.

## Where to look

- `datazimmer/schema.py` — `Entity` + `Annotated[str, Index]`. Plain classes, no metaclass. `get_columns` flattens entity-typed fields into prefixed FK columns (`breed` → `breed__breed_id`); `entity_metadata` / `to_json_schema` / `get_polars_schema` are the exports everything else derives from. Only index fields are expanded when resolving an FK target, so self-references terminate; an unsupported field type or an FK to an index-less entity raises `SchemaError`.
- `datazimmer/table.py` — `Table` wrapping shackleton's `TableRepo`. Validates column set and coerces dtypes on every write; reads return `pl.LazyFrame`. `tables_by_name` keys tables by `Table.name` (also the SQL and manifest name) and rejects duplicates.
- `datazimmer/pipeline.py` — `Pipeline` + `@pipeline.step(inputs=, outputs=)`. Topo-sorts steps; a step is skipped when its blake2b fingerprint (its source plus its inputs' fingerprints) is unchanged and outputs still exist. Step names must be unique; a step's fingerprint is recorded (`HASH_FILE` under `config.STATE_DIR`) as soon as it succeeds.
- `datazimmer/remote.py` — `Manifest` + local/ssh/s3/http `Backend`s. `publish` writes `{project}/{version}/{manifest,tables}`, the manifest listing each table's schema, checksum and data files; `load` lazily scans those files by URL (only ssh downloads first; http needs a Range-capable host); `schema_codegen` regenerates `Entity` source from a manifest, stubbing FK targets it does not publish (opt-in typed consumption).
- `datazimmer/sql.py` — entities → SQLAlchemy `MetaData` (PK from index, FK from entity fields) → load rows; `draw` renders a Mermaid ER diagram via sqlmermaid.
- `datazimmer/validate.py` — index uniqueness + FK integrity + custom checks, all in polars, no DB round-trip.
- `datazimmer/config.py` — single source of truth: the `[tool.datazimmer]` table in `pyproject.toml` (`Config.load`).
- `datazimmer/cli.py` — argparse `dz` entrypoint: `init / run / validate / sql / draw / publish / pull / schema export|codegen`. Loads the project's module-level `pipeline` by importing `config.package`.
- `datazimmer/aswan_integration.py` — `DzAswan`, the bridge to an aswan collection project (`run` → pull/scrape/save/push, `events` → parsed rows). aswan is imported lazily so it stays the optional `[collect]` extra.
- `datazimmer/zenodo.py` — slim Zenodo REST deposition.
- `datazimmer/_template/` — project scaffold copied by `dz init`.
- `dogshow/` — the canonical example **and** the end-to-end test: a small star schema (Breed, Kennel, Dog, Show, Result, BreedWins, KennelStanding) exercising every feature. `cd dogshow && make showcase` — which boots the local federation site + relay, then runs the real `dz` CLI loop — is the integration smoke test.
- `datazimmer/tests/` — `test_core`, `test_schema`, `test_table`, `test_zenodo` (core unit suite). The cross-package end-to-end coverage lives in `dogshow/tests/`.

## Architecture in one sentence

`Entity` subclasses are pure schema descriptors → `schema.py` flattens them to columns/FKs/JSON-Schema → `Table` persists schema-validated parquet via shackleton → `Pipeline` runs `@step`s in topo order with fingerprint skipping → `validate` / `sql` / `remote` consume the same `entity_metadata` to check integrity, materialize a relational+ER view, and publish/consume versioned datasets by URL.

## The ecosystem (sibling libraries)

datazimmer sits on a stack of small, single-purpose libraries the same author co-develops. They are pinned as local editable installs in `pyproject.toml` under `[tool.uv.sources]`, so changes in a sibling are visible immediately. When something conceptually belongs in one of these libraries (a new primitive, an API ergonomics fix, a bug), fix it **there**, not by working around it in datazimmer.

| Library | Path | Role | How datazimmer uses it |
| --- | --- | --- | --- |
| **shackleton** | `../../libs/shackleton` | Persistent polars table store (Parquet/Arrow IPC): partitioned writes, sorted merge, lazy reads, schema evolution, thread-safe. Single dep: polars. | `Table` wraps its `TableRepo` / `HashPartitioner` — the storage layer for every pipeline output. |
| **sqlmermaid** | `../sqlmermaid` | Mermaid ER diagrams from any SQLAlchemy metadata. | `sql.draw` feeds it the metadata built from entity schemas. |
| **aswan** | `../../libs/aswan` | Parallel web data collection into a versioned, content-addressed depot (replay/merge/sync). | `DzAswan` bridges a collection project into a pipeline step. Optional `[collect]` extra. |
| **dirark** | `../../libs/dirark` | Content-addressed directory archival (tar.zst + SQLite index, blake2b, rsync dedup sync). | **Transitive** — backs aswan's object store; not imported by datazimmer directly. |
| **atqo** | `../../libs/atqo` | Async task-queue orchestrator with resource-aware scheduling. | **Transitive** — drives aswan's parallel workers; not imported by datazimmer directly. |

`dirark` and `atqo` are listed in `[tool.uv.sources]` only so aswan resolves them to local editable copies too; datazimmer itself touches them solely through aswan.

Downstream of all this sit the **data projects** (~20 under sscu-budapest; `electiondata` is the first migration target) — real datasets that define entities + a pipeline against this library and publish versioned catalogs.

## Documentation convention

The whole ecosystem documents itself two ways, and every repo should carry both:

- **`README.md` — functionality, for users.** What the library is, install, quick start, the public API and concepts. Task-oriented and example-driven. aswan's README is the reference depth.
- **`AGENTS.md` — orientation, for developers and AI agents.** A "where to look" map of the modules, the architecture in a sentence, the sibling-library relationships, conventions, sharp edges, and how to run the tests. Not a tutorial — a fast index into the source. This file is the reference shape.
- **`CLAUDE.md` → `AGENTS.md`** as a symlink, so the agent harness picks up the same file without duplication.

Data projects get a README describing the dataset and a short AGENTS.md noting their entities, pipeline shape, and remote.

## Conventions

- Python ≥ 3.12, type hints everywhere; `uv` for envs, `ruff` for format (line length from `[tool.ruff]`). Never pip.
- Minimal core dependencies — heavy/optional features (`aswan`, `boto3`, `requests`) live in extras and are imported lazily inside the functions that need them (see the `__getattr__` in `__init__.py` and the in-handler imports in `cli.py` / `remote.py`).
- No metaclasses, no runtime codegen on the hot path, no `&`-operator DSL, no pandas. Entities are plain annotated classes; codegen is opt-in and explicit.
- Fail loud and early with clear exception types (`SchemaError`, `ValidationError`, `ConfigError`, `PipelineError`).
- Credentials are standard env vars (`AWS_ACCESS_KEY_ID`, …) — no custom auth layer.

## Running

```bash
uv run pytest            # datazimmer core unit suite (datazimmer/tests, seconds)
```

CI runs the same suite as `uv run --no-sources pytest`, resolving the siblings from PyPI instead of the editable checkouts.

The `dogshow/` cross-package integration + e2e suite is a **separate uv project** with its own venv and `dev` dependency group (flask, moto, bs4, coverage), so it runs from its own directory — the root `uv run pytest` deliberately does not collect it:

```bash
cd dogshow && uv run pytest         # collect → parse → validate → sql/draw → publish
cd dogshow && make showcase         # boot the local site + relay, run the real dz CLI loop
```

The dogshow uses a `local` remote (`_remote/`), so `publish`/`pull` round-trips run with no external services.
