import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Union

import yaml
from invoke import Collection, task
from parquetranger import TableRepo
from structlog import get_logger

from ..constants import PARAMS_PATH

logger = get_logger()


class PipelineRegistry:
    def __init__(self):
        self._steps = {}
        try:
            self.all_params = yaml.safe_load(PARAMS_PATH.read_text())
        except FileNotFoundError:
            self.all_params = {}

    def register(
        self,
        procfun=None,
        *,
        dependencies: Optional[list] = None,
        outputs: Optional[list] = None,
        outputs_nocache: Optional[list] = None,
    ):
        """the names of parameters will matter
        and will be looked up in params.yaml"""

        def f(fun):
            relpath = inspect.getfile(fun)
            lineno = inspect.findsource(fun)[1]
            parsed_deps = self._parse_list(dependencies)

            pe = PipelineElement(
                runner=fun,
                outputs=self._parse_list(outputs),
                param_list=inspect.getfullargspec(fun).args,
                dependencies=[relpath] + parsed_deps,
                out_nocache=self._parse_list(outputs_nocache),
                lineno=lineno,
            )
            self._steps[pe.name] = pe
            return fun

        if procfun is None:
            return f
        return f(procfun)

    def get_step(self, name: str) -> "PipelineElement":
        return self._steps[name]

    def get_collection(self):
        tasks = [pe.get_invoke_task() for pe in self.steps]
        return Collection("pipeline", *tasks)

    @property
    def steps(self) -> Iterable["PipelineElement"]:
        return self._steps.values()

    def _parse_elem(
        self,
        elem: Union[str, Path, TableRepo, type],
    ) -> str:
        if isinstance(elem, str):
            return [elem]
        if isinstance(elem, Path):
            return [elem.as_posix()]
        if isinstance(elem, TableRepo):
            return [elem.full_path]
        if isinstance(elem, type):
            return _type_or_fun_elem(elem)
        if callable(elem):
            pe = self._steps.get(elem.__name__)
            if pe is None:
                return _type_or_fun_elem(elem)
            return pe.outputs + pe.out_nocache
        raise TypeError(
            f"{type(elem)} was given as parameter for pipeline element"
        )

    def _parse_list(self, elemlist):
        return sum([self._parse_elem(e) for e in elemlist or []], [])


@dataclass
class PipelineElement:

    runner: callable
    outputs: list
    param_list: list
    dependencies: list
    out_nocache: list
    lineno: int

    def run(self, params: dict = None):

        loaded_params = params or yaml.safe_load(PARAMS_PATH.read_text()) or {}

        parsed_params = {}
        _level_params = loaded_params.get(self.name, {})
        for k in self.param_list:
            try:
                parsed_params[k] = _level_params[k]
            except KeyError:
                logger.warn(
                    "couldn't find key in step level params, looking globally",
                    key=k,
                    step_keys=_level_params.keys(),
                )
                parsed_params[k] = loaded_params[k]

        return self.runner(**parsed_params)

    def get_invoke_task(self):
        param_str = ",".join(
            [
                (".".join([self.name, p]) if p != "seed" else p)
                for p in self.param_list
            ]
        )
        if param_str:
            param_str = "-p " + param_str
        command = " ".join(
            [
                f"dvc run -n {self.name}",
                "{}",  # for --force
                param_str,
                _get_comm(self.dependencies, "d"),
                _get_comm(self.outputs, "o"),
                _get_comm(self.out_nocache, "O"),
                f"python -m src {self.name}",
            ]
        )

        @task(name=self.name)
        def _task(c, force=True):
            c.run(command.format(" --force" if force else ""))

        return _task

    @property
    def name(self):
        return self.runner.__name__


def _get_comm(entries, prefix):
    return " ".join([f"-{prefix} {e}" for e in entries])


def _type_or_fun_elem(elem):
    return [Path(inspect.getfile(elem)).relative_to(Path.cwd()).as_posix()]
