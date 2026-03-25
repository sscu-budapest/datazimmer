# Datazimmer Ecosystem Report

*Generated 2026-03-24 | Comprehensive audit of all sscu-budapest / endremborza tooling and data projects*

---

## Overview

The datazimmer ecosystem is a scientific data engineering platform built to create, manage, version, and publish reproducible tabular datasets. It was created by the Social Science Computing Unit Budapest (SSCU) and centers on the `datazimmer` orchestrator, supported by ~10 custom libraries and powering ~20 data projects.

**Core philosophy**: metadata-first, reproducible data pipelines with schema validation, DVC-backed versioning, and Zenodo publication.

**Current state**: functional but stale. Most repos have not been actively maintained since 2022-2023. The ecosystem carries heavy dependency baggage, excessive abstraction layers, and "magic" patterns that make it hostile to newcomers and AI agents.

---

## Tooling Repos

### Tier 1: Core Pipeline

| Repo | Purpose | Deps | State | Notes |
|------|---------|------|-------|-------|
| **datazimmer** | Central orchestrator: CLI, pipeline registration, metadata serialization, DVC integration, Zenodo publishing, SQL export | parquetranger, colassigner, metazimmer, zimmauth, sqlmermaid, pandas, sqlalchemy, structlog, typer, pyyaml, toml, cookiecutter, flit | Stale (last real commit: Feb 2024). 39 Python modules, ~28 CLI commands, heavy magic. | The hub. Everything flows through here. |
| **aswan** | Web data collection framework: parallel HTTP/browser scraping with versioned depot storage | atqo, structlog (being removed), optional: flask, selenium, beautifulsoup4 | Under active revision (uncommitted changes). 39 tests pass, modernization in progress. | Key improvements: dropped pyyaml/typer/fabric/structlog, flat status model, rsync sync. |
| **parquetranger** | Partitioned parquet dataset management (pandas/pyarrow) | pandas, pyarrow, atqo | Being archived in favor of shackleton. Has uncommitted revision changes. | ScruTable in datazimmer wraps this. |
| **shackleton** | Polars-based TableRepo (parquetranger replacement) | polars (only!) | v1.0.0 merged. Has uncommitted README + PERF.md. | 10-100x faster than parquetranger. Zero-magic. Hash partitioning via polars xxhash. |

### Tier 2: Supporting Libraries

| Repo | Purpose | Deps | State | Notes |
|------|---------|------|-------|-------|
| **colassigner** | Type-safe pandas column assignment DSL, metaclass-based schema introspection | pandas | Stable, v0.4.2. Clean git. | `AbstractEntity` in datazimmer inherits from `ColAssigner`. Heavy metaclass magic. |
| **atqo** | Async task queue orchestrator with resource-aware multiprocess scheduling | tblib | Stable, v0.4.2. Clean git. | Used by aswan for parallel workers. Type-based API (SyncAPI/MultiProcAPI). |
| **dirark** | Zero-dependency directory archival: tar.zst + SQLite index, BLAKE2b dedup | **none** | v0.1.0, clean. | Planned as aswan's object store backend. Excellent design. |
| **sqlmermaid** | Generate Mermaid ER diagrams from SQLAlchemy schemas | sqlalchemy | Stable, clean. | Simple, works. Single-file module. |
| **partcsv** | Zero-dependency CSV partitioning | **none** | Stable, clean. | Tiny utility. |
| **branthebuilder** | Python package scaffolding, CI boilerplate, test/doc orchestration | typer, ruff, cookiecutter, pyyaml, pytest, pytest-cov, chardet | Recently modernized (v5.0.1, hatchling build). | Used by almost every other repo for `[test]` and `[doc]` extras. |

### Tier 3: Auth & Meta (to be eliminated or heavily reworked)

| Repo | Purpose | Deps | State | Notes |
|------|---------|------|-------|-------|
| **zimmauth** | Symmetric encryption for credential storage (PBKDF2+Fernet), DVC remote config, SSH/S3 auth | cryptography, optional: toml, fabric, decorator | Stale. Tightly coupled to DVC. | Over-engineered for what it does. Replaced by rsync in aswan. |
| **metazimmer** | Empty shell package (`__version__ = "0.0.0"`), runtime-generated metadata namespace | **none** | 5 commits total, essentially a placeholder. | datazimmer generates `metazimmer-<project>` packages at build time. Pure magic. |

### Other Local Repos (not core ecosystem)

| Repo | Purpose | Ecosystem role |
|------|---------|----------------|
| **hallubib** | BibTeX parser/formatter with CLI | Used in academic projects, not datazimmer |
| **rankless** | Large-scale scholarly data platform (Svelte+Rust+Python) | Consumer of data pipelines |
| **ccl-science-data** | Science data utilities | Unclear, needs investigation |
| **chysame** | Browser-native PBKDF2+AES-GCM encryption (JS) | Not Python, not datazimmer |
| **uxarr** | Unknown | Not found locally |

---

## Data Projects (sscu-budapest)

All data projects follow the same pattern: `zimmer.yaml` config, `src/` with entity definitions (`AbstractEntity` subclasses) and pipeline steps (`@dz.register`), DVC stages, parquet output.

### Active / Recently Updated
| Project | Domain | Last updated | Notes |
|---------|--------|-------------|-------|
| **chess** | Chess data | 2026-03-23 | Active |
| **retail** | Retail data | 2026-03-23 | Active |
| **job-data** | Job market data | 2026-03-23 | Active |
| **real-estate-data** | Hungarian real estate | 2026-02-19 | Recent |
| **articledata** | Academic article data | 2025-11-02 | Recent |

### Established
| Project | Domain | Last updated |
|---------|--------|-------------|
| **electiondata** | Hungarian election results (parliament, municipal) | 2022-12-20 |
| **filmdata** | Film industry data | 2023-05-07 |
| **football-depth** | Football analytics | 2024-05-31 |
| **footballing-nations** | International football statistics | 2023-01-30 |
| **bbc-sentiment** | BBC article sentiment analysis | 2024-10-15 |
| **politician-fb-hun** | Hungarian politicians' Facebook data | 2024-03-09 |
| **mobility** | Mobility research data | 2024-03-01 |
| **mobility-pattern-data** | Mobility patterns | 2023-02-27 |
| **metascience** | Meta-science / bibliometrics | 2023-04-05 |
| **socialmedia** | Social media data | 2022-06-24 |
| **twitter-dataset** | Twitter geolocation + network | 2022-10-28 |
| **finance-forum-data** | Finance forum discussions | 2022-07-29 |
| **telekom-dataset** | Telecom data | 2023-10-09 |

### Smaller / One-off
| Project | Domain |
|---------|--------|
| **geobase** | Geographic base data |
| **gps-ping-data** | GPS ping data |
| **board-game-dataset** | Board game data |
| **dataset-oszkar** | Oszkar (ridesharing) data |
| **dabble** | Small public datasets for exploration |
| **softwaredata** | Software ecosystem data |
| **assisted-living-capacities-hun** | Hungarian assisted living capacities |
| **lead-contamination-hungary** | Lead contamination data |

### Infrastructure
| Project | Purpose |
|---------|---------|
| **main-registry** | Versioned metadata registry (git-backed package index) |
| **project-template** | Cookiecutter template for new data projects |
| **explorer** / **explorer-template** | Data exploration UI |
| **sscu-budapest.github.io** | Organization website |
| **dvc** | Forked DVC (custom patches) |
| **pandas-profiling** | Forked pandas-profiling |

---

## Electiondata: A Representative Data Project

**Path**: `/mnt/data/synced/composites/data-projects/electiondata`

This is a typical consumer of the ecosystem:

```
zimmer.yaml          -- project config (name: elections, aswan_projects: hun-election)
src/meta.py          -- entity definitions (Election, GeographicalUnit, VoteRecord, etc.)
src/hungarian/
  collection.py      -- aswan handlers (ListHandler, LetterHandler, LocHandler, VoteInstHandler)
  parsing.py         -- @dz.register_data_loader pipeline step, ScruTable writes
dvc.yaml             -- single stage: complete-hungarian
data/                -- parquet outputs
```

**Key observations**:
- Uses `dz.AbstractEntity` + `dz.Index & str` syntax for schema (magic `&` operator)
- Uses `dz.ScruTable(meta.Entity)` for typed parquet tables
- Uses `dz.SourceUrl` for provenance tracking
- Uses `dz.DzAswan` for web collection (scrapes valtor.valasztas.hu)
- Uses `colassigner.get_all_cols` for column introspection
- Pipeline reads from hardcoded `/home/borza/tmp/election-midway/` parquet files
- Entity relationships via typed references (e.g., `election = Election` on `ElectionPrecinct`)

---

## Dependency Graph

```
datazimmer (orchestrator)
 +-- parquetranger --> pandas, pyarrow, atqo --> structlog, tblib
 +-- colassigner ----> pandas
 +-- metazimmer -----> (empty)
 +-- zimmauth -------> cryptography, [toml, fabric]
 +-- sqlmermaid -----> sqlalchemy
 +-- sqlalchemy
 +-- pandas
 +-- structlog
 +-- typer
 +-- pyyaml
 +-- toml
 +-- cookiecutter
 +-- flit, wheel, pip, setuptools
 |
 +-- [collect] aswan -> atqo -> structlog, tblib
 |                      [webext] flask, flask-cors
 |                      [soup] beautifulsoup4
 |                      [browser] selenium
 +-- [zenodo] requests, markdown2
 +-- [test] branthebuilder -> typer, ruff, cookiecutter, pyyaml, pytest, pytest-cov, chardet
```

**Total unique dependencies** (direct + transitive, approximate): ~40+
**Self-made dependencies**: 8 (parquetranger, colassigner, atqo, metazimmer, zimmauth, sqlmermaid, branthebuilder, partcsv)

---

## Critical Pain Points

### 1. Too Much Magic
- `AbstractEntity` uses metaclass introspection + `&` operator overloading for index declaration
- `ColAssigner` metaclass tracks column definitions via class body attribute access
- `metazimmer` is a runtime-generated package — datazimmer serializes entity classes to YAML, builds them into installable packages, publishes to a git registry
- `ScruTable` wraps parquetranger with context managers for environment switching
- `KeyMeta` metaclass returns attribute names as strings (config key generation)
- `PipelineElement` introspects function dependencies via AST walking

### 2. Stale Dependencies
- **parquetranger**: being replaced by shackleton (polars), but datazimmer still hardcodes parquetranger
- **pandas**: shackleton uses polars; pandas remains required by datazimmer + colassigner
- **structlog**: being removed from aswan, still in datazimmer + atqo
- **pyarrow**: unnecessary once on polars/shackleton
- **DVC**: heavyweight orchestrator used for what could be simple shell scripts
- **flit/wheel/pip/setuptools**: build-time deps baked into runtime requirements
- **typer**: CLI framework, could be argparse or click
- **pyyaml + toml**: two config formats, pick one
- **cookiecutter**: project templating, heavyweight for what it does

### 3. Broken Abstractions
- zimmauth exists to encrypt DVC remote configs — rsync replaces this in modernized aswan
- metazimmer is a zero-code placeholder that exists only for the runtime code-generation pipeline
- The metadata registry (main-registry) is a git repo acting as a package index
- DVC integration adds 5 layers of indirection for running pipeline steps

### 4. Poor AI/Developer Experience
- No comprehensive documentation (Sphinx stubs exist but are incomplete)
- Entity schema uses Python class body as DSL — not inspectable without running Python
- Config spread across zimmer.yaml, __run_conf.yaml, ~/.config/datazimmer.yaml, env vars
- Pipeline steps discovered via module walking + decorator introspection
- No type stubs, no JSON schema for configs
- Error messages reference internal state (CompleteId, NamespaceMetadata) not user concepts
