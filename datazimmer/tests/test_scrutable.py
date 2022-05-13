import pandas as pd
import pytest

from datazimmer.exceptions import ProjectSetupException
from datazimmer.naming import DEFAULT_ENV_NAME


def test_scrutable_parsing(running_template):

    df = pd.DataFrame(
        {"ind": [5, 20], "d": ["2020-01-01", "1999-04-04"], "num": ["3", "4"]}
    )

    from src.core import scrutable

    scrutable.replace_all(df)

    parsed_df = scrutable.get_full_df()
    assert not parsed_df.equals(df)
    pd.testing.assert_frame_equal(
        df.astype({"d": "datetime64", "num": float}).set_index("ind"),
        parsed_df,
    )
    for fp in scrutable.paths:
        assert DEFAULT_ENV_NAME in fp
    scrutable.purge()
    assert scrutable.get_full_df().empty

    with pytest.raises(KeyError):
        scrutable.replace_all(pd.DataFrame({"ind": [1]}))


def test_run_scrutable(in_template):

    from src.core import scrutable

    with pytest.raises(ProjectSetupException):
        scrutable.replace_all(...)
