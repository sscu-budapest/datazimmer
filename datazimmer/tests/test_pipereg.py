from pathlib import Path

from pytest import raises

from datazimmer.naming import DEFAULT_ENV_NAME
from datazimmer.pipeline_registry import PipelineRegistry
from datazimmer.utils import reset_src_module


def test_pipereg_basics(running_template):

    pipereg = PipelineRegistry()

    def step1(a=5):
        return a * 2

    step1.__module__ = "src.step_one"
    pipereg.register(step1)  # usually works with decorator @register
    # but here module change is needed for the test
    assert pipereg.get_step(f"{DEFAULT_ENV_NAME}-step_one").runner(8) == 16

    def step2():
        return 10

    step2.__module__ = "src.step_two.deeper"
    pipereg.register(dependencies=["some_string"])(step2)

    assert pipereg.get_step(f"{DEFAULT_ENV_NAME}-step_two").run() == 10


def test_pipereg_parse_elems(in_template):

    reset_src_module()
    from src import core

    _env = DEFAULT_ENV_NAME
    pipereg = PipelineRegistry()
    _module_file_path = Path("src", "core.py").as_posix()

    maps = [
        (["sss", Path("sss")], "sss"),
        ([core, core.template_proc], _module_file_path),
    ]
    for _ss, target in maps:
        for _s in _ss:
            assert pipereg._parse_elem(_s, _env) == [target]

    with raises(TypeError):
        pipereg._parse_elem(20, _env)
