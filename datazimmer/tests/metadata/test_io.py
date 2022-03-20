import pytest

from datazimmer.metadata.bedrock.atoms import CompositeType


def test_feature_parse():
    # TODO: extend this

    with pytest.raises(TypeError):
        CompositeType("tct", [{"not": "good"}])
