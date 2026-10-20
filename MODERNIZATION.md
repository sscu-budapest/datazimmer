# Datazimmer Ecosystem Modernization Plan

*Living document — completed items are removed; history is in git.*

---

## Vision

A lean, fast, explicit scientific data management platform:
- **Polars-native** (no pandas in the core path)
- **Zero magic** (no metaclasses, no runtime codegen, no `&`-operator DSL)
- **Minimal dependencies** (core: polars, sqlalchemy, shackleton, sqlmermaid, protocli)
- **uv everywhere**, **AI-agent friendly** (JSON schemas, explicit configs)
- **Useful data catalog** (static site with CSVs, distributions, ER diagrams)

## Remaining work

### Phase 5 — Data catalog & publication

- **`catalog.py` / `dz catalog build`**: static site from project metadata + data samples. Per dataset: schema table (from `entity_metadata`), sample rows, lightweight column distributions, downloadable CSV (parquet→csv at build), ER diagram (reuse `sql.draw`), provenance (aswan source URLs / dates). Searchable index page. Deploy to any static host. Jinja2 → HTML.
- **Zenodo polish**: wire `dz zenodo` command around `ZenodoDeposit`; optional CITATION.cff regeneration.

### Phase 6 — Data project migration

Migrate real projects onto the new stack, one at a time:

1. **electiondata** — first migration; the live, full-aswan-integration showcase (no external/midway data). Migration guide committed at `electiondata/MODERNIZATION.md`.
2. **chess**, **real-estate-data**, **articledata** — as needed.

Each migration: entities → new `Entity` schema; pipeline → explicit `@pipeline.step`; `zimmer.yaml`/`dvc.yaml` → `[tool.datazimmer]`; collectors → new `DzAswan`; set a remote; verify `collect → run → validate → publish`.

### Nice-to-haves

- **Incremental publish**: diff manifests, upload only changed table dirs.
- **Postgres FK validation**: optional `dz validate --constr postgres://…` path that enforces FKs in-DB (deferred constraints) in addition to the polars checks.
- **dirark index-first sync**: pull remote index, exclude already-present hashes from the upload pack (today `push_ark` rsyncs whole tars).

---

## Success criteria

- `dz catalog build` produces a useful static site from any project.
- electiondata produces equivalent output on the new stack.
