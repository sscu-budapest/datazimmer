import pytest

from sscutils.helpers import get_top_module_name
from sscutils.naming import imported_namespaces_abs_module


def test_top_module():
    with pytest.raises(ValueError):
        get_top_module_name("not_src.something")

    assert "sg" == get_top_module_name(imported_namespaces_abs_module + ".sg")
