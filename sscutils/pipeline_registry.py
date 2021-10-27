import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Union

import yaml
from invoke import Collection, task
from structlog import get_logger

from sscutils.metadata.inscript_converters import (
    load_metadata_from_child_module,
)
from sscutils.metadata.io import dump_to_yaml

from .naming import SRC_PATH, ProjectConfigPaths, get_top_module_name
from .scrutable_class import ScruTable

logger = get_logger()


class PipelineRegistry:
    """register pipeline elements in a project"""

    def __init__(self):
        self._steps = {}
        try:
            self.all_params = yaml.safe_load(
                ProjectConfigPaths.PARAMS.read_text()
            )
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
        f"""registers a function to the pipeline

        only one function per module of {SRC_PATH}

        the names of parameters will matter
        and will be looked up in params.yaml"""

        def f(fun):
            relpath = inspect.getfile(fun)
            lineno = inspect.findsource(fun)[1]
            parsed_deps = self._parse_list(dependencies)

            for out in outputs or []:
                if isinstance(out, ScruTable):
                    out.register_as_step_output(_get_name(fun))

            pe = PipelineElement(
                runner=fun,
                outputs=self._parse_list(outputs),
                param_list=inspect.getfullargspec(fun).args,
                dependencies=[relpath] + parsed_deps,
                out_nocache=self._parse_list(outputs_nocache),
                lineno=lineno,
            )
            self._steps[pe.name] = pe

            return pe

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
        elem: Union[str, Path, ScruTable, type],
    ) -> str:
        if isinstance(elem, str):
            return [elem]
        if isinstance(elem, Path):
            return [elem.as_posix()]
        if isinstance(elem, ScruTable):
            return [elem.trepo.full_path]
        if isinstance(elem, type):
            return _type_or_fun_elem(elem)
        if isinstance(elem, PipelineElement):
            return elem.outputs + elem.out_nocache
        if callable(elem):
            return _type_or_fun_elem(elem)

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

    def run(self, params: dict = None, log_metadata=True):

        loaded_params = (
            params
            or yaml.safe_load(ProjectConfigPaths.PARAMS.read_text())
            or {}
        )

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
        if log_metadata:
            self._log_metadata()
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
        # TODO: to in-script dvc
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
        return _get_name(self.runner)

    def _log_metadata(self):
        dump_to_yaml(
            load_metadata_from_child_module(self.runner.__module__), self.name
        )


def _get_comm(entries, prefix):
    return " ".join([f"-{prefix} {e}" for e in entries])


def _type_or_fun_elem(elem):
    return [Path(inspect.getfile(elem)).relative_to(Path.cwd()).as_posix()]


def _get_name(caller):
    return get_top_module_name(caller.__module__)
