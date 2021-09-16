import os
from pathlib import Path

from invoke import Collection, MockContext
from parquetranger import TableRepo
from pytest import raises

import sscutils.core.pipeline_registry as pipereg_module
from sscutils import PipelineRegistry
from sscutils.constants import PARAMS_PATH
from sscutils.core.pipeline_registry import _type_or_fun_elem


def test_pipereg_basics():
    pipereg = PipelineRegistry()

    @pipereg.register
    def step1(a=5):
        return a * 2

    assert pipereg.get_step("step1").runner(8) == 16

    @pipereg.register(dependencies=["some_string"])
    def step2(b=3):
        return b - 1

    coll = pipereg.get_collection()
    assert isinstance(coll, Collection)
    assert [*coll.task_names] == ["step1", "step2"]
    coll.tasks["step1"](MockContext(run="dvc run *"))

    assert pipereg.get_step("step2").run({"step2": {"b": 10}}) == 9
    assert pipereg.get_step("step2").run({"b": 5}) == 4


def test_pipereg_params(tmp_path):
    cwd = Path.cwd()
    os.chdir(tmp_path)
    PARAMS_PATH.write_text("a: 10")
    pipereg = PipelineRegistry()
    assert pipereg.all_params["a"] == 10
    os.chdir(cwd)


def test_pipereg_parse_elems(tmp_path):

    trepo = TableRepo(tmp_path / "fing")
    pipereg = PipelineRegistry()

    @pipereg.register(outputs=["s1_out"])
    def step1():
        pass  # pragma: no cover

    _module_file_path = (
        Path(pipereg_module.__file__).relative_to(Path.cwd()).as_posix()
    )

    assert pipereg._parse_elem("sss") == ["sss"]
    assert pipereg._parse_elem(PARAMS_PATH) == [PARAMS_PATH.as_posix()]
    assert pipereg._parse_elem(trepo) == [trepo.full_path]
    assert pipereg._parse_elem(PipelineRegistry) == [_module_file_path]
    assert pipereg._parse_elem(_type_or_fun_elem) == [_module_file_path]
    assert pipereg._parse_elem(step1) == ["s1_out"]

    with raises(TypeError):
        pipereg._parse_elem(20)
