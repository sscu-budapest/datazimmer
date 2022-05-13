import pandas as pd
import pytest

from datazimmer.exceptions import ProjectSetupException


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


def test_run_scrutable(in_template):

    from src.core import scrutable

    with pytest.raises(ProjectSetupException):
        scrutable.replace_all(...)
