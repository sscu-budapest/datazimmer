# Building the ultimate dogshow

*Living document — completed items are removed; history is in git.*

## Forward work

- **Gate the showcase in CI.** The repo workflow runs only `datazimmer/tests/` (siblings from PyPI); the dogshow proves the *cross-package* contract against the live sibling trees and nothing keeps it green. A CI job must first check out every sibling repo (shackleton, sqlmermaid, aswan, atqo, dirark) into the `../../../libs/...` layout its editable sources expect, then run `cd dogshow && make report` and upload `REPORT.md` + `coverage/html`. Largest real gap.
- **Run provenance manifest.** A machine-readable record of a run (per-step fingerprints, input/output checksums, package versions, wall time) would let a third party verify they reproduced the same outputs — the dogshow as the first consumer of the Phase 5 catalog in [../MODERNIZATION.md](../MODERNIZATION.md).
