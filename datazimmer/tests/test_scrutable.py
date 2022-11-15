import pandas as pd
import pytest

from datazimmer import ScruTable
from datazimmer.exceptions import ProjectSetupException
from datazimmer.naming import DEFAULT_ENV_NAME


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
