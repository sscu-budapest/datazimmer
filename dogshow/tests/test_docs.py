"""The committed ER diagram is generated, never hand-edited.

``docs/schema.md`` is produced by ``dz draw`` (``make erd``) from the entity
schema in ``src/meta.py``. This guard keeps the single-source-of-truth diagram
the README points at from silently drifting from the schema. No server, no data
— ``draw`` needs only the entity metadata.
"""

from pathlib import Path

from datazimmer import draw

SCHEMA_DOC = Path(__file__).resolve().parents[1] / "docs" / "schema.md"


def test_erd_doc_is_up_to_date(tmp_path):
    import src

    fresh = draw(src.pipeline.tables, tmp_path / "schema.md").read_text()
    assert SCHEMA_DOC.read_text() == fresh, "docs/schema.md is stale — run `make erd`"
