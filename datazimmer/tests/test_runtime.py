import pytest

from datazimmer.get_runtime import get_runtime


def test_runtime_basics(running_template):
    runtime = get_runtime()
    assert not any(runtime.registry.get_info().values())
    assert runtime.registry.get_project_meta_base("nonexistent", "0.0") is None

    with pytest.raises(KeyError):
        runtime.run_step("core", "no-env")
