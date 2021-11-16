import datetime as dt

import pandas as pd
import pytest

from sscutils import IndexBase, ScruTable, TableFeaturesBase
from sscutils.metadata.datascript.to_bedrock import (
    DatascriptToBedrockConverter,
)
from sscutils.naming import ns_metadata_abs_module
from sscutils.tests.create_dogshow import dataset_template_repo
from sscutils.utils import cd_into


def test_scrutable_io():

    df = pd.DataFrame({"a": [10, 20], "b": ["xy", "zv"]})

    class TableFeatures(TableFeaturesBase):
        a = int
        b = str

    class ThingIndex(IndexBase):
        pass

    class WrongName(TableFeaturesBase):
        pass

    with cd_into(dataset_template_repo, force_clone=True):
        scrutable = ScruTable(TableFeatures, namespace="test")
        scrutable.replace_all(df)
        assert scrutable.get_full_ddf().compute().equals(df)
        assert scrutable.name == "table"

        sc2 = ScruTable(index=ThingIndex, namespace="test")
        assert sc2.name == "thing"

        with pytest.raises(NameError):
            ScruTable(WrongName, namespace="test")


def test_scrutable_parsing():

    df = pd.DataFrame(
        {"ind": [5, 20], "d": ["2020-01-01", "1999-04-04"], "num": ["3", "4"]}
    )

    class TableFeatures(TableFeaturesBase):
        d = dt.datetime
        num = float
        __module__ = ns_metadata_abs_module

    class ThingIndex(IndexBase):
        ind = int
        __module__ = ns_metadata_abs_module

    with cd_into(dataset_template_repo, force_clone=True):
        scrutable = ScruTable(TableFeatures, index=ThingIndex, namespace="")

        # not serialized
        scrutable.replace_all(df)
        assert scrutable.get_full_ddf().compute().equals(df)

        conv = DatascriptToBedrockConverter(ns_metadata_abs_module)
        conv._add_scrutable(scrutable)
        conv.to_ns_metadata().dump()

        scrutable.replace_all(df)
        parsed_df = scrutable.get_full_ddf().compute()
        assert not parsed_df.equals(df)
        scrutable.trepo.purge()

        scrutable.replace_all(df, False)
        assert scrutable.get_full_ddf().compute().equals(df)

    pd.testing.assert_frame_equal(
        df.astype({"d": "datetime64", "num": float}).set_index("ind"),
        parsed_df,
    )
