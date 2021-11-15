import pytest

from sscutils.metadata import ArtifactMetadata
from sscutils.metadata.bedrock.atoms import Table
from sscutils.metadata.bedrock.namespace_metadata import NamespaceMetadata
from sscutils.metadata.bedrock.namespaced_id import NamespacedId


def test_base_lookup():

    tab1 = Table("t1", "EC", [])
    t1_id = NamespacedId("ns2", "t1")

    ns_empty_meta = NamespaceMetadata([], [], [], "")
    ns_meta = NamespaceMetadata([], [], [tab1], "ns2")
    a_meta = ArtifactMetadata([], {"": ns_empty_meta, "ns2": ns_meta})

    assert tab1 == a_meta.get_atom(t1_id)

    with pytest.raises(KeyError):
        ns_empty_meta.get("t1")
