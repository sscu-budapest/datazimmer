# Datazimmer Ecosystem Modernization Plan

*Living document — completed items are removed, tracked via git history*

---

## Vision

A lean, fast, explicit scientific data management platform:
- **Polars-native** (no pandas in core path)
- **Zero magic** (no metaclasses, no runtime code generation, no `&` operator DSL)
- **Minimal dependencies** (target: polars, sqlalchemy, and stdlib)
- **uv everywhere** (no pip, no flit, no setuptools in runtime)
- **AI-agent friendly** (JSON schemas, explicit configs, inspectable pipelines, clear error messages)
- **Self-documenting** (type hints, dataclasses, simple module structure)
- **Useful data catalog** (static site with downloadable CSVs, visualizations, and exploratory analysis)

---

## The Dogshow

The `dogshow/` directory in this repo serves two roles:

1. **Test suite** — end-to-end integration tests exercising *every* datazimmer feature
2. **Documentation/showcase** — readable example demonstrating how the system works, for both humans and AI agents

Features the dogshow must cover:
- Entity definitions: all field types (Index, FK, Nullable, composite)
- Table operations: create, write, read, append, replace, schema validation
- Pipeline: multi-step with declared dependencies and step-skipping on unchanged inputs
- Data collection: aswan handlers collecting from a mock API
- Scheduled collection: cron-triggered pipeline updates via CI
- Remote operations: publish → pull round-trip (tested via moto/local filesystem)
- Consumer flow: load another project's data from remote URL (cross-project import)
- Data catalog: generate static site with schema tables, sample rows, distributions, ER diagrams, CSV export
- JSON Schema export for all entities
- Schema evolution: adding/renaming columns handled gracefully
- Data quality checks: custom validation rules on pipeline outputs
- Incremental updates: append new records to existing tables
- Reproducibility: re-running pipeline skips unchanged steps

```
dogshow/
  pyproject.toml                    # [tool.datazimmer] config
  src/
    schema.py                       # Dog, Breed, Show, Judge, Result entities
    collect.py                      # Aswan handlers (mock dog show API)
    pipeline.py                     # collect → clean → aggregate → validate → publish
    quality.py                      # Custom validation rules
  tests/
    test_full_cycle.py              # The integration suite
```

---

## Dependency Consolidation Target

### Keep (modernize)
| Library | Role | Status |
|---------|------|--------|
| **shackleton** | Table storage (polars) | Done. Replaces parquetranger. |
| **aswan** | Data collection | In progress. Finish modernization, integrate dirark. |
| **dirark** | Object archival (tar.zst + SQLite) | Stable. Wire into aswan depot. |
| **sqlmermaid** | ER diagram generation | Done. Minimal. |
| **atqo** | Task scheduling | Done. structlog removed, type-based API. |
| **branthebuilder** | Dev tooling (branb CLI) | Done (v5). Dev tool only, not a package dep. |
| **partcsv** | Zero-dep CSV partitioning | Stable. Minimal. |

### Eliminate
| Library | Replacement |
|---------|------------|
| **parquetranger** | shackleton |
| **metazimmer** | Inline metadata as dataclasses + JSON schema files |
| **zimmauth** | Environment variables + standard secret management |
| **colassigner** | `datazimmer.schema` (plain annotations, done) |
| **cookiecutter** | Simple `dz init` that copies a template dir |
| **DVC** | `datazimmer.remote` — manifest-based publish/pull (see Phase 3) |

### Drop from datazimmer core
| Dependency | Reason |
|-----------|--------|
| pandas | polars via shackleton |
| pyarrow | polars handles parquet |
| structlog | stdlib logging |
| typer | argparse or click |
| pyyaml + toml | stdlib `tomllib` (3.11+) |
| DVC | `datazimmer.remote` module (~200 lines replaces entire DVC dep tree) |
| cookiecutter | template dir |
| cryptography | drop zimmauth |

---

## Phases

### Phase 0: Foundation
*Goal: modernize the libraries datazimmer depends on, and clean up all repos.*

**aswan 1.0** (finish modernization):
- Fix webext test
- Integrate dirark for object store

**dirark 0.2**:
- Stable API, sync protocol, tested with aswan

### Phase 1: Schema Revolution
*Goal: replace the metaclass-based entity/column DSL with plain Python.*

`datazimmer/schema.py` created with:
- `Entity` base class (plain marker, no metaclass)
- `Index` marker via `Annotated[str, Index]`
- FK inferred from Entity subclass references → expands to `prefix__index_col`
- `get_columns()`, `get_index_columns()`, `get_property_columns()`
- `get_polars_schema()` (lazy polars import)
- `to_json_schema()` — JSON Schema export

Remaining:
- Wire `schema.py` into the rest of datazimmer (Table, pipeline, catalog)
- Migration tool: convert old entity definitions to new format
- **Eliminate**: colassigner, ColAssigner, ColMeta, AbstractEntity, CompositeTypeBase, `Index & Type` syntax

**Syntax comparison:**
```python
# OLD (metaclass magic)
class Election(dz.AbstractEntity):
    eid = dz.Index & str
    start_date = dt.datetime
    is_individual = bool

# NEW (plain annotations)
class Election(Entity):
    eid: Annotated[str, Index]
    start_date: datetime
    is_individual: bool
```

### Phase 2: Table Layer Swap
*Goal: replace parquetranger/pandas with shackleton/polars.*

- New `datazimmer.table` module wrapping shackleton's `TableRepo`
- API: `Table(schema=Election, path="data/elections")` — explicit, no env context magic
- Methods: `read() -> pl.LazyFrame`, `write(df)`, `append(df)`, `replace(df)`
- Schema validation on write: polars dtypes derived from entity definition, FK integrity checks
- **Eliminate**: ScruTable, parquetranger, `env_ctx()` context managers, pandas as core dep

### Phase 3: Pipeline & Data Hosting
*Goal: replace DVC entirely — both pipeline orchestration and remote data sync.*

This is the critical phase. DVC currently handles pipeline execution, data tracking, remote sync, and cross-project imports. We replace all four with simpler, purpose-built modules.

#### 3a: Pipeline Runner (`datazimmer.pipeline`)

**Current** (implicit, DVC-backed):
```python
@dz.register_data_loader
def load_data():
    table.replace_all(df)
# → generates dvc.yaml stage, runs via `dvc repro`
```

**Target** (explicit):
```python
pipeline = Pipeline("hungarian")

@pipeline.step(inputs=["raw/"], outputs=[election_table, precinct_table])
def load_data():
    ...

# dz run → topological sort → execute steps
```

Step-skipping: hash inputs (source code + input files) → store in `.datazimmer/step_hashes.json` → skip if unchanged. This replaces `dvc.lock` with ~50 lines of code.

#### 3b: Data Hosting & Sync (`datazimmer.remote`)

**The key insight**: parquet files are immutable pipeline artifacts. We don't need content-addressed storage (DVC's CAS). We need versioned file publishing and URL-based access.

**Manifest** — committed to git, replaces `.dvc` files:
```json
{
  "project": "electiondata",
  "version": "0.2.0",
  "tables": {
    "elections": {"path": "elections/", "rows": 1234, "size_bytes": 56789, "checksum": "blake2b:abc..."},
    "precincts": {"path": "precincts/", "rows": 5678, "size_bytes": 123456, "checksum": "blake2b:def..."}
  }
}
```

**Remote backends** (~100 lines each):
| Backend | Producer (`dz publish`) | Consumer (`dz pull` or lazy read) |
|---------|------------------------|----------------------------------|
| **S3** | `boto3.upload_file()` | `pl.scan_parquet("s3://bucket/project/v0.2/elections/")` |
| **HTTP** | upload to any static host | `pl.scan_parquet("https://data.example.com/elections/")` |
| **GitHub Releases** | `gh release upload` | download via release URL |
| **Local/SSH** | `shutil.copytree` / rsync | direct path read |

Polars reads parquet from HTTP and S3 natively — consumers may not need to download at all.

**Producer flow:**
```
dz run          → execute pipeline, write parquet locally
dz publish      → upload parquet + manifest + JSON schemas to remote
                  (versions by git tag: project-v0.2.0)
dz catalog build → generate static site from local data
```

**Consumer flow:**
```python
# Lazy read from remote (no download needed):
elections = dz.load("sscu-budapest/electiondata", "elections")
# → pl.LazyFrame pointing at remote parquet

# Or download for offline work:
# dz pull sscu-budapest/electiondata
```

**Cross-project import** replaces `dvc import` (which cloned entire repos):
```toml
# pyproject.toml
[tool.datazimmer.dependencies]
elections = {source = "sscu-budapest/electiondata", version = ">=0.1"}
```
Resolution: check local cache → fetch manifest → lazy-read parquet from remote URL.

**Credentials**: standard env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`). Supported natively by boto3 and polars. No zimmauth needed.

**What we gain:**
- Remove DVC (massive dep tree: `dvc[s3,ssh]`)
- Remove zimmauth (custom encryption)
- Simpler mental model: versioned parquet on a remote, accessed by URL
- Native polars streaming for consumers
- Standard credential management

**What we lose (and why it's OK):**
- Content-addressed dedup across versions — parquet is already compact, disk is cheap
- `dvc repro` automatic stage skipping — replaced by hash-based step-skipping (~50 LOC)
- Incremental push — can compare manifests and skip unchanged files (add later if needed)

#### 3c: Versioning

- Git tags for data releases: `electiondata-v0.2.0`
- Manifest committed to git alongside code
- Each remote has a version directory structure: `{project}/{version}/{table}/`
- `dz.load("project@v0.1", "table")` loads a specific version

**Eliminate**: PipelineElement, @register decorators, ProjectRuntime module walker, DVC, dvc.yaml, dvc.lock, `.dvc/` directory, `__run_conf.yaml`

### Phase 4: Config & Auth Cleanup
*Goal: single config format, no encryption layer, standard tooling.*

- Single config: `pyproject.toml` `[tool.datazimmer]` section
- Auth via env vars only (standard `AWS_*`, `SSH_*`)
- CLI: `dz init`, `dz run`, `dz publish`, `dz pull`, `dz validate`, `dz catalog build`, `dz schema export`
- **Eliminate**: zimmer.yaml, `__run_conf.yaml`, `~/.config/datazimmer.yaml`, zimmauth, metazimmer, git-backed main-registry

### Phase 5: Data Catalog & Publication
*Goal: a useful, browsable data catalog and modern data sharing.*

Replaces the old explorer (Jupyter Book + pandas-profiling, removed 2023).

**Data Catalog** (`dz catalog build`):
- Static site generated from project metadata + data samples
- Good DX: defining an Entity + running the pipeline = catalog updates automatically
- Each dataset page shows:
  - Schema table (columns, types, constraints, foreign keys)
  - Sample rows (first N rows rendered as HTML table)
  - Column distributions / histograms (lightweight, no pandas-profiling)
  - Downloadable CSV export (generated from parquet at build time)
  - ER diagram (via sqlmermaid)
  - Data provenance (source URLs, collection dates, update schedule)
- Searchable index page: filter by domain, date, size, column types
- Built with lightweight static site approach (Jinja2 templates → HTML, or Svelte SPA consuming JSON metadata)
- No server required — deploy to GitHub Pages or any static host

**Publication**:
- Keep Zenodo integration (simplify API wrapper)
- Add: export to HuggingFace Datasets, simple HTTP hosting
- Schema published as JSON Schema alongside data

### Phase 6: Data Project Migration
*Goal: migrate existing projects to new format, one at a time.*

Priority order:
1. **electiondata** — first migration, proves the new system works
2. **chess** — active
3. **retail** — active
4. Remaining projects as needed

For each migration:
- Convert entity definitions to new schema format
- Convert pipeline to explicit steps
- Replace zimmer.yaml with pyproject.toml config
- Update aswan collectors to new depot format
- Set up remote backend (S3 or GitHub Releases)
- Test end-to-end: collect → process → validate → publish → catalog

---

## Features Checklist

Every feature must be exercised by the dogshow:

| Feature | Module | Status |
|---------|--------|--------|
| Entity schema definition | `schema.py` | Done |
| JSON Schema export | `schema.py` | Done |
| Polars schema derivation | `schema.py` | Done |
| Table CRUD (read/write/append/replace) | `table.py` | Phase 2 |
| Schema validation on write | `table.py` | Phase 2 |
| FK integrity checks | `table.py` | Phase 2 |
| Pipeline declaration & execution | `pipeline.py` | Phase 3 |
| Step-skipping (hash-based) | `pipeline.py` | Phase 3 |
| Data publishing (remote upload) | `remote.py` | Phase 3 |
| Data pulling (remote download) | `remote.py` | Phase 3 |
| Lazy remote reads (polars scan) | `remote.py` | Phase 3 |
| Cross-project data import | `remote.py` | Phase 3 |
| Manifest generation & versioning | `remote.py` | Phase 3 |
| Data quality checks | `validate.py` | Phase 4 |
| Schema evolution handling | `schema.py` | Phase 4 |
| Scheduled collection (aswan + CI) | aswan/CI | Phase 5 |
| Catalog static site generation | `catalog.py` | Phase 5 |
| ER diagram rendering | sqlmermaid | Phase 5 |
| Zenodo archival | `zenodo.py` | Phase 5 |
| Project scaffolding (`dz init`) | CLI | Phase 4 |

---

## Breaking Changes

### For data project authors:
1. Entity definitions: `eid = dz.Index & str` → `eid: Annotated[str, Index]`
2. `ScruTable` replaced by `Table` (polars, explicit path, no env magic)
3. `zimmer.yaml` replaced by `pyproject.toml [tool.datazimmer]`
4. `@dz.register` decorators replaced by explicit `Pipeline` + `@pipeline.step`
5. Pipeline outputs are polars DataFrames, not pandas
6. DVC removed entirely — `dz run` / `dz publish` / `dz pull` replace all DVC commands
7. No more metazimmer/zimmauth package imports
8. Remote credentials via standard env vars, not encrypted zimmauth

### Migration path:
- Provide `dz migrate` command that auto-converts entity definitions + config
- Clear deprecation warnings pointing to new APIs

---

## Success Criteria

- datazimmer core has <= 5 external dependencies (polars, sqlalchemy, boto3, and stdlib)
- A new data project can be understood by reading only pyproject.toml + src/
- An AI agent can create a new data project from a natural language description
- `dz run` works without DVC installed
- electiondata produces identical output on new stack
- All entity schemas exportable as JSON Schema
- No metaclass magic anywhere in the codebase
- Full test suite (including dogshow) runs in < 30 seconds
- `dz catalog build` produces a useful static site from any project
- The dogshow exercises every feature listed above
- Documentation auto-generated from type hints + docstrings

---

## Quick Reference: What Lives Where

```
datazimmer/
  schema.py       -- Entity, Index, get_columns, to_json_schema (done)
  table.py        -- Table wrapping shackleton (Phase 2)
  pipeline.py     -- Pipeline, Step, step-skipping (Phase 3)
  remote.py       -- publish, pull, manifest, S3/HTTP backends (Phase 3)
  validate.py     -- data quality checks (Phase 4)
  catalog.py      -- static site generation (Phase 5)
  zenodo.py       -- Zenodo archival (exists, simplify)
  cli.py          -- dz init/run/publish/pull/catalog/validate (Phase 4)

dogshow/          -- integration test suite + documentation showcase

/mnt/data/synced/code/
  libs/
    shackleton/      -- polars table storage (done)
    aswan/           -- data collection (modernizing)
    dirark/          -- archive storage (stable)
    atqo/            -- task scheduling (done)
    partcsv/         -- CSV partitioning (stable)
    parquetranger/   -- ARCHIVE
    colassigner/     -- ARCHIVE
    metazimmer/      -- ARCHIVE
    zimmauth/        -- ARCHIVE
  apps/
    datazimmer/      -- this repo
    sqlmermaid/      -- ER diagrams (stable)
```
