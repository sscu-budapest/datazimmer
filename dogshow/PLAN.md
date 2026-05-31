# Building the ultimate dogshow

*Living document — completed phases are removed; history is in git.*

The self-contained, cross-package showcase + coverage suite is built and green
(`uv run pytest` → 17 passed, 1 skipped; the skip is the ssh backend, which the
dockerized run exercises). What it is and how it maps to each package lives in
[README.md](README.md).

## Stretch (only if a real gap shows up)

- The `http`/`s3` backends can fetch manifests + parquet *metadata* over the
  wire but `load()` of a multi-file table still localizes first for ssh/http;
  direct `pl.scan_parquet` over a custom S3 endpoint (minio) needs
  `storage_options` plumbing in `remote.py`. Add it in datazimmer only if a
  consumer actually needs endpoint-addressed lazy reads — otherwise the moto +
  in-process-HTTP coverage of the client code is sufficient.

## Done

- [x] `uv run pytest` is green and self-contained on a clean checkout
      (17 passed, 1 skipped — ssh).
- [x] `docker compose up` runs the full suite incl. the ssh backend (18/18) and
      emits a combined coverage report covering all six packages.
- [x] The measured per-package coverage is recorded in README.md.
- [x] `dz run/validate/draw/sql/publish/pull` all work against the fixture site,
      reading no external data.
- [x] Gaps found while building were fixed at the source: the pipeline gained
      first-class step-ordering dependencies (`datazimmer/pipeline.py`) so a
      table-less collect step still orders + fingerprints its dependents; the
      docker image installs `zstd` (dirark's ark needs it) — a clean-environment
      dependency the host happened to satisfy.

Everything in the original plan is built and verified. The only open item is the
optional stretch above; close this doc out (delete it) once that's decided.
