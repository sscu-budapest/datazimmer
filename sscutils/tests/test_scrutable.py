import datetime as dt

import pandas as pd
import pytest

from sscutils import ScruTable, TableFeaturesBase
from sscutils.tests.create_dogshow import dataset_template_repo
from sscutils.utils import cd_into


def test_scrutable_io(tmp_path):

    df = pd.DataFrame({"a": ["10", "20"], "b": ["2020-01-01", "1999-04-04"]})

    class TableFeatures(TableFeaturesBase):
        a = int
        b = dt.datetime

    class WrongName(TableFeaturesBase):
        pass

    with cd_into(dataset_template_repo):
        scrutable = ScruTable(TableFeatures, namespace="test")
        scrutable.trepo.replace_all(df)
        assert scrutable.get_full_ddf().compute().equals(df)

        with pytest.raises(NameError):
            ScruTable(WrongName, namespace="test")
