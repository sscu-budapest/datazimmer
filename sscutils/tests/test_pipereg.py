from pathlib import Path

from pytest import raises

import sscutils.pipeline_registry as pipereg_module
from sscutils import PipelineRegistry
from sscutils.naming import ProjectConfigPaths
from sscutils.pipeline_registry import _type_or_fun_elem

PARAMS_PATH = ProjectConfigPaths.PARAMS


def test_pipereg_basics():
    pipereg = PipelineRegistry()

    def step1(a=5):
        return a * 2

    step1.__module__ = "src.step_one"
    pipereg.register(step1)  # usually works with decorator @pipereg.register
    # but here module change is needed for the test
    assert pipereg.get_step("step_one").runner(8) == 16

    def step2(b=3):
        return b - 1

    step2.__module__ = "src.step_two.deeper"
    pipereg.register(dependencies=["some_string"])(step2)

    assert (
        pipereg.get_step("step_two").run({"step_two": {"b": 10}}, False) == 9
    )
    assert pipereg.get_step("step_two").run({"b": 5}, False) == 4


def test_pipereg_parse_elems(tmp_path):

    pipereg = PipelineRegistry()

    def step1():
        pass  # pragma: no cover

    step1.__module__ = "src.step_one"
    step1_pe = pipereg.register(outputs=["s1_out"])(step1)

    _module_file_path = (
        Path(pipereg_module.__file__).relative_to(Path.cwd()).as_posix()
    )

    assert pipereg._parse_elem("sss") == ["sss"]
    assert pipereg._parse_elem(PARAMS_PATH) == [PARAMS_PATH.as_posix()]
    assert pipereg._parse_elem(PipelineRegistry) == [_module_file_path]
    assert pipereg._parse_elem(_type_or_fun_elem) == [_module_file_path]
    assert pipereg._parse_elem(step1) == [
        Path(__file__).relative_to(Path.cwd()).as_posix()
    ]
    assert pipereg._parse_elem(step1_pe) == ["s1_out"]

    with raises(TypeError):
        pipereg._parse_elem(20)
