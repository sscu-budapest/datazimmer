import inspect
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Dict, Iterable, Optional

from pyinstrument import Profiler
from structlog import get_logger

from .config_loading import Config, RunConfig
from .exceptions import ProjectSetupException
from .metadata.complete_id import CompleteIdBase
from .metadata.scrutable import ScruTable
from .naming import BASE_CONF_PATH, CLI, MAIN_MODULE_NAME, PROFILES_PATH, get_data_path
from .reporting import ReportFile

logger = get_logger()
_GLOBAL_PIPEREG = None


class PipelineRegistry:
    """register pipeline elements in a project"""

    def __init__(self):
        self._steps: Dict[str, PipelineElement] = {}
        self._external_crons = defaultdict(dict)
        self._conf = Config.load()

    def register(
        self,
        procfun=None,
        *,
        dependencies: Optional[list] = None,
        outputs: Optional[list] = None,
        outputs_nocache: Optional[list] = None,
        outputs_persist: Optional[list] = None,
        write_env: Optional[str] = None,
        read_env: Optional[str] = None,
        cron="",
    ):
        """registers a function to the pipeline

        the names of parameters will matter
        and will be looked up in conf/envs.yaml params"""

        def f(fun):
            id_base = CompleteIdBase.from_cls(fun)
            if id_base.project:
                return self._add_external_cron(id_base, cron)

            relpath = inspect.getfile(fun)
            lineno = inspect.findsource(fun)[1]
            iter_envs = [write_env] if write_env else self._all_env_names
            for _env in iter_envs:
                _parser = partial(self._parse_list, env=_env)
                _true_read_env = read_env or _env
                parsed_deps = self._parse_list(dependencies, _true_read_env)
                pe = PipelineElement(
                    runner=fun,
                    env=_env,
                    read_env=_true_read_env,
                    outputs=_parser(outputs),
                    dependencies=[relpath] + parsed_deps,
                    out_nocache=_parser(outputs_nocache),
                    out_persist=_parser(outputs_persist),
                    lineno=lineno,
                    cron=cron,
                )
                if write_env or (pe.name not in self._steps.keys()):
                    self._steps[pe.name] = pe
                if cron:
                    self._conf.init_cron_bump(pe.name)

            return fun

        if procfun is None:
            return f
        return f(procfun)

    def register_env_creator(self, fun, extras=None, cron=""):
        """Convenience functions to use register with typical parameters"""
        for _env in self._conf.envs:
            if _env.name == self._conf.default_env:
                continue
            deps = self._get_data_dirs(fun, self._conf.default_env) + (extras or [])
            self.register(
                write_env=_env.name,
                read_env=self._conf.default_env,
                outputs=self._get_data_dirs(fun, _env.name),
                dependencies=deps,
                cron=cron,
            )(fun)
        return fun

    def register_data_loader(self, fun, extras=None, cron=""):
        """Convenience functions to use register with typical parameters"""
        return self.register(
            write_env=self._conf.default_env,
            outputs_persist=self._get_data_dirs(fun, self._conf.default_env),
            dependencies=extras or [],
            cron=cron,
        )(fun)

    def get_step(self, name: str) -> "PipelineElement":
        return self._steps[name]

    def get_external_cron(self, project, ns) -> str:
        return self._external_crons[project].get(ns)

    def step_names_of_env(self, env: str):
        return [k for k, v in self._steps.items() if v.env == env]

    @property
    def steps(self) -> Iterable["PipelineElement"]:
        return self._steps.values()

    def _parse_elem(self, elem, env: str) -> str:
        if isinstance(elem, str):
            return [elem]
        if isinstance(elem, Path):
            return [elem.as_posix()]
        if isinstance(elem, ReportFile):
            return [elem.env_posix(env)]
        if isinstance(elem, ScruTable):
            with elem.env_ctx(env):
                return [elem.trepo.full_path]
        if inspect.ismodule(elem):
            try:
                abs_paths = elem.__path__
            except AttributeError:
                abs_paths = [elem.__file__]
            return _parse_abs_paths(abs_paths)
        if callable(elem):
            return _type_or_fun_elem(elem)

        raise TypeError(f"{type(elem)} was given as parameter for pipeline element")

    def _parse_list(self, elemlist, env):
        return sum([self._parse_elem(e, env) for e in elemlist or []], [])

    def _get_data_dirs(self, fun, env):
        id_ = CompleteIdBase.from_cls(fun, self._conf.name)
        return [get_data_path(id_.project, id_.namespace, env)]

    def _add_external_cron(self, id_base: CompleteIdBase, cron: str):
        self._external_crons[id_base.project][id_base.namespace] = cron

    @property
    def _all_env_names(self):
        return [e.name for e in self._conf.envs]


@dataclass
class PipelineElement:

    runner: callable
    env: str
    read_env: str
    outputs: list
    dependencies: list
    out_nocache: list
    out_persist: list
    lineno: int
    cron: str

    def run(self):
        conf = RunConfig.load()
        conf.read_env = self.read_env
        conf.write_env = self.env
        conf.dump()
        _, kwargs = self._get_params()
        with _profile(conf.profile, self.name):
            return self.runner(**kwargs)

    def add_as_stage(self, dvc_repo):
        param_ids, _ = self._get_params()
        if self.cron:
            param_ids.append(f"cron_bumps.{self.name}")
        dvc_repo.stage.add(
            cmd=f"{CLI} run-step {self.name}",
            name=self.name,
            outs_no_cache=self.out_nocache,
            outs=self.outputs,
            outs_persist=self.out_persist,
            deps=self.dependencies,
            params=[{BASE_CONF_PATH.as_posix(): param_ids}] if param_ids else None,
            force=True,
        )

    @property
    def name(self):
        return f"{self.env}-{self.ns}"

    @property
    def ns(self):
        return CompleteIdBase.from_cls(self.runner).namespace

    def _get_params(self):
        kwarg_keys = inspect.getfullargspec(self.runner).args
        parsed_params = {}
        param_ids = []
        for k in kwarg_keys:
            param_id, val = self._get_param_id_val(self.ns, k, self.env)
            parsed_params[k] = val
            param_ids.append(param_id)
        return param_ids, parsed_params

    def _get_param_id_val(self, namespace, key, env):
        conf = Config.load()
        _envconf = conf.get_env(env)
        _level_params = _envconf.params.get(namespace, {})
        if key in _level_params.keys():
            _suff, val = [namespace, key], _level_params[key]
        elif key in _envconf.params.keys():
            _suff, val = [key], _envconf.params[key]
        else:
            if _envconf.parent is None:
                raise ProjectSetupException(f"no {namespace}.{key} in {env}")
            return self._get_param_id_val(namespace, key, _envconf.parent)
        # WET: 2 keys here in str literal
        return ".".join(["envs", env, "params", *_suff]), val


def get_global_pipereg(reset=False) -> PipelineRegistry:
    global _GLOBAL_PIPEREG
    if (_GLOBAL_PIPEREG is None) or reset:
        _GLOBAL_PIPEREG = PipelineRegistry()
    return _GLOBAL_PIPEREG


def register(
    procfun=None,
    *,
    dependencies: Optional[list] = None,
    outputs: Optional[list] = None,
    outputs_nocache: Optional[list] = None,
    outputs_persist: Optional[list] = None,
    write_env: Optional[str] = None,
    read_env: Optional[str] = None,
    cron="",
):
    return get_global_pipereg().register(
        procfun,
        dependencies=dependencies,
        outputs=outputs,
        outputs_nocache=outputs_nocache,
        outputs_persist=outputs_persist,
        write_env=write_env,
        read_env=read_env,
        cron=cron,
    )


def register_env_creator(fun=None, *, extra_deps=None, cron=""):
    return _wrap(get_global_pipereg().register_env_creator, fun, extra_deps, cron)


def register_data_loader(fun=None, *, extra_deps=None, cron=""):
    return _wrap(get_global_pipereg().register_data_loader, fun, extra_deps, cron)


def _wrap(base, decorated, *args):
    if decorated is None:

        def f(_fun):
            return base(_fun, *args)

        return f
    return base(decorated)


def _type_or_fun_elem(elem):
    return _parse_abs_paths([inspect.getfile(elem)])


def _parse_abs_paths(abs_paths):
    return [*map(_parse_abs_path, abs_paths)]


def _parse_abs_path(some_path):
    _parts = Path(some_path).parts
    _srcind = _parts.index(MAIN_MODULE_NAME)
    return Path(*_parts[_srcind:]).as_posix()


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
    lis = [f'<li><a href="./{_p.name}">{_p.name[:-5]}</a></li>' for _p in all_profiles]
    li_html = "".join(lis)
    full_html = f"<html><body><ul>{li_html}</ul></body></html>"
    (PROFILES_PATH / "index.html").write_text(full_html)
