"""Relational materialization (SQLAlchemy) + the sqlmermaid ER diagram."""

import sqlalchemy as sa
from datazimmer import draw, to_sql


def test_to_sql_loads_with_fks(built, tmp_path):
    engine = to_sql(built.pipeline.tables, f"sqlite:///{tmp_path / 'dog.db'}")
    with engine.connect() as conn:
        assert conn.execute(sa.text("select count(*) from dogs")).scalar() == 5
        assert conn.execute(sa.text("select count(*) from results")).scalar() == 5

    fks = sa.inspect(engine).get_foreign_keys("dogs")
    referred = {fk["referred_table"] for fk in fks}
    assert {"breeds", "kennels"} <= referred  # incl. the nullable kennel FK


def test_er_diagram(built, tmp_path):
    out = draw(built.ALL_TABLES, tmp_path / "erd.md")
    text = out.read_text()
    assert text.startswith("```mermaid")
    for table in ("breeds", "kennels", "dogs", "shows", "results"):
        assert table in text
