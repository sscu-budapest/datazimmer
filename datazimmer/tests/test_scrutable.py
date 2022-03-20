import pandas as pd
import pytest

from datazimmer import IndexBase, ScruTable, TableFeaturesBase
from datazimmer.exceptions import ArtifactRuntimeException
from datazimmer.naming import MAIN_MODULE_NAME, META_MODULE_NAME

_IN_MODULE = f"{MAIN_MODULE_NAME}.core"
_EXT_MODULE = f"{META_MODULE_NAME}.ext.core"


def test_scrutable_init(running_template):
    class TableFeatures(TableFeaturesBase):
        __module__ = _IN_MODULE

    class ExtFeatures(TableFeaturesBase):
        __module__ = _EXT_MODULE

    class ThingIndex(IndexBase):
        __module__ = _IN_MODULE

    class WrongName(TableFeaturesBase):
        __module__ = _IN_MODULE

    scrutable = ScruTable(TableFeatures)
    assert scrutable.name == "table"

    sc2 = ScruTable(index=ThingIndex)
    assert sc2.name == "thing"

    sc3 = ScruTable(ExtFeatures)

    with pytest.raises(ArtifactRuntimeException):
        sc3.replace_all(...)

    with pytest.raises(NameError):
        ScruTable(WrongName)


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
