import pandas as pd
import pytest
import sqlalchemy as sa

from datazimmer import ScruTable
from datazimmer.config_loading import RunConfig
from datazimmer.exceptions import ProjectSetupException
from datazimmer.get_runtime import get_runtime
from datazimmer.naming import DEFAULT_ENV_NAME
from datazimmer.sql.loader import SqlLoader, tmp_constr


def test_scrutable_parsing(running_template):

    df = pd.DataFrame(
        {
            "ind": [5, 20],
            "d": ["2020-01-01", "1999-04-04"],
            "num": ["3", "4"],
            "c": ["A", "B"],
        }
    )

    from src.core import Thing, scrutable

    scrutable: ScruTable
    scrutable.replace_all(df)

    parsed_df = scrutable.get_full_df()
    assert not parsed_df.equals(df)
    pd.testing.assert_frame_equal(
        df.astype({"d": "datetime64", "num": float}).set_index("ind"),
        parsed_df.sort_index().reindex(df.columns[1:], axis=1),
    )
    for fp in scrutable.paths:
        assert DEFAULT_ENV_NAME in fp.as_posix()

    assert next(scrutable.get_partition_paths(Thing.c))
    scrutable.purge()
    assert scrutable.get_full_df().empty

    with pytest.raises(KeyError):
        scrutable.replace_all(pd.DataFrame({"ind": [1]}))
    assert "thing" in scrutable.__repr__()


def test_run_scrutable(in_template):

    from src.core import scrutable

    with pytest.raises(ProjectSetupException):
        scrutable.replace_all(...)


def test_sql(in_template):
    runtime = get_runtime()
    with RunConfig(False, write_env=DEFAULT_ENV_NAME, read_env=DEFAULT_ENV_NAME):
        runtime.run_step("core", DEFAULT_ENV_NAME)

    with tmp_constr() as constr:
        loader = SqlLoader(constr)
        sa.MetaData.reflect(loader.sql_meta)
        loader.load_data(DEFAULT_ENV_NAME)
        loader.validate_data(DEFAULT_ENV_NAME)
