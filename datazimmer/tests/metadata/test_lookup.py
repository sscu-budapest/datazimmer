import pytest

from datazimmer.metadata import ArtifactMetadata
from datazimmer.metadata.bedrock.atoms import Table
from datazimmer.metadata.bedrock.complete_id import CompleteId
from datazimmer.metadata.bedrock.namespace_metadata import NamespaceMetadata


def test_base_lookup():

    tab1 = Table("t1", "EC", [])
    t1_id = CompleteId("a1", "ns2", "t1")

    ns_empty_meta = NamespaceMetadata([], [], [], "")
    ns_meta = NamespaceMetadata([], [], [tab1], "ns2")
    a_meta = ArtifactMetadata("uri", [], {"": ns_empty_meta, "ns2": ns_meta})

    assert tab1 == a_meta.get_atom(t1_id)

    with pytest.raises(KeyError):
        ns_empty_meta.get("t1")
