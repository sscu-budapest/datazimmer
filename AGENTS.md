# datazimmer

Scientific data pipeline orchestrator: typed entity schemas → schema-validated polars tables → explicit pipelines → versioned publish/consume by URL. Core deps are polars, sqlalchemy, shackleton, sqlmermaid and protocli; everything else is an optional extra.

## Where to look

- `datazimmer/schema.py` — `Entity` (plain annotated classes, never instantiated) + `Annotated[str, Index]`. `get_columns` flattens an entity-typed field into prefixed FK columns (`breed` → `breed__breed_id`), expanding only the target's index fields, so self-references terminate. A `Literal` of strings is a categorical column (`pl.Enum`, JSON-Schema `enum`, manifest `categories`). `entity_metadata` is what the other modules derive from. An unsupported type or an FK to an index-less entity raises `SchemaError`.
- `datazimmer/table.py` — `Table` wraps shackleton's `TableRepo` with the entity's whole index as `id_col`: files stay sorted by it and `replace` upserts on it. Every write checks the column set and coerces dtypes (unknown Enum categories fail); reads are `pl.LazyFrame`, empty and typed when unpopulated. `Table.name` (the path's last component) is also the SQL and manifest name; `tables_by_name` rejects duplicates.
- `datazimmer/pipeline.py` — `Pipeline` + `@pipeline.step(inputs=, outputs=)`; an input is a `Table`, a path or another step. Steps run in topo order; one is skipped when its blake2b fingerprint (source + inputs' fingerprints) matches `HASH_FILE` under the state dir and its outputs exist. The fingerprint is cleared before a step starts and recorded as soon as it succeeds.
- `datazimmer/_hashing.py` — the file/tree blake2b digest shared by pipeline fingerprints and manifest checksums.
- `datazimmer/remote.py` — `Manifest` + local/ssh/s3/http `Backend`s (http is read-only). `publish` writes `{project}/{version}/` (manifest + one dir per table) and `{project}/latest.json`; `load` scans the manifest's files lazily by URL (ssh downloads first; http needs Range support); `schema_codegen` regenerates `Entity` source from a manifest, stubbing FK targets it does not publish.
- `datazimmer/sql.py` — entities → SQLAlchemy `MetaData` (PK from index, FK from entity fields); `to_sql` drops, recreates and loads parents before children; `draw` renders Mermaid via sqlmermaid.
- `datazimmer/validate.py` — index uniqueness + FK integrity as lazy count queries collected in one `pl.collect_all`, then custom checks.
- `datazimmer/config.py` — `Config.load` reads `[tool.datazimmer]` from `pyproject.toml`, the only config source; `Config.import_module` imports a project module with the project root on `sys.path`.
- `datazimmer/cli.py` — `dz`: typed `_cmd_*` handlers dispatched by protocli, per-flag help from each handler's `Args:` docstring, `schema export|codegen` as a nested `Dispatcher`. Commands that need the pipeline import the module-level `pipeline` of `config.package`.
- `datazimmer/aswan_integration.py` — `DzAswan` (`run` pulls, scrapes, saves and pushes the depot; `events` reads parsed rows) and `dz collect`: `find_collectors` takes the named subclasses defined in the `collectors` modules, `cadence` makes one due, and attempts go to `ATTEMPTS_FILE` under the state dir, so a failing collector retries once per cadence.
- `datazimmer/zenodo.py` — `ZenodoDeposit`, a minimal Zenodo REST deposition; no CLI command yet.
- `datazimmer/_template/` — the scaffold `dz init` copies.
- `datazimmer/tests/` — the core unit suite.
- `dogshow/` — the canonical example and the cross-package end-to-end suite: a small star schema exercising every feature, against a locally served site (see its README).

## Architecture in one sentence

`Entity` subclasses are pure schema descriptors → `schema.py` flattens them to columns/FKs/JSON-Schema → `Table` persists schema-validated parquet via shackleton → `Pipeline` runs `@step`s in topo order with fingerprint skipping → `validate` / `sql` / `remote` consume the same `entity_metadata` to check integrity, materialize a relational + ER view, and publish/consume versioned datasets by URL.

## The ecosystem (sibling libraries)

datazimmer sits on small single-purpose libraries by the same author. `pyproject.toml` resolves them from PyPI, as CI and users do. To work against the local checkouts, run `uv sync --extra collect` (so aswan's own dependencies are installed), then `branb link ../../libs/shackleton ../sqlmermaid ../../libs/protocli ../../libs/aswan ../../libs/dirark ../../libs/atqo`; the link lasts until the venv is recreated, and `branb link` with no paths removes it. A fix that conceptually belongs in a sibling (a new primitive, an API ergonomics fix, a bug) goes **there**, not into a workaround here.

| Library | Role | Used by datazimmer as |
| --- | --- | --- |
| **shackleton** | Persistent polars table store: partitioned Parquet writes, sorted merge, lazy reads, schema evolution. | The storage under every `Table` (`TableRepo`, `HashPartitioner`). |
| **sqlmermaid** | Mermaid ER diagrams from SQLAlchemy metadata. | `sql.draw`. |
| **protocli** | Signature-driven argparse dispatcher: nested groups, `--help-all`, lazy shell completion. | The `dz` CLI. |
| **aswan** | Parallel web collection into a versioned, content-addressed depot. | `DzAswan`; the optional `[collect]` extra. |
| **dirark** | Content-addressed directory archival (tar.zst + SQLite index). | Transitive only: aswan's object store. |
| **atqo** | Async task queue with resource-aware scheduling. | Transitive only: aswan's parallel workers. |

Downstream, ~20 sscu-budapest data projects define entities + a pipeline against datazimmer and publish versioned datasets; `electiondata` is the first to migrate onto this stack.

## Documentation convention

Every repo in the ecosystem carries a `README.md` for users (what it is, install, quick start, API, concepts; example-driven, aswan's is the reference depth) and an `AGENTS.md` for developers and agents (module map, architecture, sibling relationships, conventions, sharp edges, how to test; an index into the source, not a tutorial, shaped like this file), with `CLAUDE.md` a symlink to `AGENTS.md`. Data projects get a README describing the dataset and a short AGENTS.md naming their entities, pipeline shape and remote.

## Conventions

- Python per `requires-python`, type hints throughout; `uv`, never pip; `ruff` with the line length from `[tool.ruff]` (CI runs `uvx ruff check datazimmer/`).
- Optional features (`aswan`, `boto3`, `requests`) are extras imported lazily where used: the `__getattr__` in `__init__.py`, the in-handler imports in `cli.py`, `S3Backend.client`, `ZenodoDeposit`. The core must import without them.
- No metaclasses, no `&`-operator DSL, no pandas, no runtime codegen on the hot path; `schema_codegen` is opt-in.
- Fail loud and early with the module exceptions: `SchemaError`, `ConfigError`, `PipelineError`, `ValidationError`, `CollectError`.
- Credentials are standard env vars (`AWS_ACCESS_KEY_ID`, `ZENODO_TOKEN`, …); no auth layer.

## Running

```bash
uv run pytest                  # core unit suite (datazimmer/tests)
cd dogshow && uv run pytest    # cross-package integration + e2e
cd dogshow && make showcase    # boots the local site + relay, runs the real dz CLI loop
```

CI runs the core suite against the siblings' PyPI releases, i.e. a venv without `branb link`. The dogshow is a separate uv project (own venv, a `dev` group with flask, moto, bs4, coverage) whose `[tool.uv.sources]` pins the sibling checkouts as editables; the root `uv run pytest` does not collect it. Its remote is `local` (`_remote/`), so publish/pull need no external services.
