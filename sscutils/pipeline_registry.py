import inspect
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Union

import yaml
from pyinstrument import Profiler
from structlog import get_logger

from .config_loading import RunConfig
from .helpers import get_associated_step
from .metadata.datascript.conversion import child_module_datascript_to_bedrock
from .metadata.datascript.scrutable import ScruTable
from .naming import PROFILES_PATH, SRC_PATH, ProjectConfigPaths

logger = get_logger()


class PipelineRegistry:
    """register pipeline elements in a project"""

    def __init__(self):
        self._steps = {}

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

    @property
    def steps(self) -> Iterable["PipelineElement"]:
        return self._steps.values()

    def _parse_elem(
        self,
        elem: Union[str, Path, ScruTable, "PipelineElement", type],
    ) -> str:
        if isinstance(elem, str):
            return [elem]
        if isinstance(elem, Path):
            return [elem.as_posix()]
        if isinstance(elem, ScruTable):
            return [elem.trepo.full_path]
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
        _, kwargs = self._get_params(params)
        if log_metadata:
            self._log_metadata()
        conf = RunConfig.load()
        with _profile(conf.profile, self.name):
            return self.runner(**kwargs)

    def add_as_stage(self, dvc_repo):
        param_ids, _ = self._get_params()
        dvc_repo.stage.add(
            cmd=f"python -m src {self.name}",
            name=self.name,
            outs_no_cache=self.out_nocache,
            outs=self.outputs,
            deps=self.dependencies,
            params=param_ids,
            force=True,
        )

    @property
    def name(self):
        return get_associated_step(self.runner)

    @property
    def child_module(self):
        return self.runner.__module__

    def _log_metadata(self):
        child_module_datascript_to_bedrock(self.child_module)

    def _get_params(self, override=None):
        loaded_params = override or _load_params()
        parsed_params = {}
        _level_params = loaded_params.get(self.name, {})
        param_ids = []
        for k in self.param_list:
            try:
                parsed_params[k] = _level_params[k]
                param_id = f"{self.name}.{k}"
            except KeyError:
                logger.warn(
                    "couldn't find key in step level params, looking globally",
                    key=k,
                    step_keys=_level_params.keys(),
                )
                parsed_params[k] = loaded_params[k]
                param_id = k
            param_ids.append(param_id)
        return param_ids, parsed_params


def _type_or_fun_elem(elem):
    return [Path(inspect.getfile(elem)).relative_to(Path.cwd()).as_posix()]


def _load_params():
    return yaml.safe_load(ProjectConfigPaths.PARAMS.read_text()) or {}


@contextmanager
def _profile(run: bool, name: str):
    if not run:
        yield
        return
    profiler = Profiler()
    profiler.start()
    yield
    profiler.stop()
    path = PROFILES_PATH / f"{name}.html"
    path.parent.mkdir(exist_ok=True)
    path.write_text(profiler.output_html())
    all_profiles = PROFILES_PATH.glob("*.html")
    lis = [
        f'<li><a href="./{_p.name}">{_p.name[:-5]}</a></li>'
        for _p in all_profiles
    ]
    li_html = "".join(lis)
    full_html = f"<html><body><ul>{li_html}</ul></body></html>"
    (PROFILES_PATH / "index.html").write_text(full_html)
