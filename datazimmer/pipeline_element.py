import inspect
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Optional

from structlog import get_logger

from .aswan_integration import DzAswan
from .config_loading import (
    CONF_KEYS,
    ENV_KEYS,
    Config,
    RunConfig,
    get_aswan_leaf_param_id,
)
from .exceptions import ProjectSetupException
from .metadata.complete_id import CompleteIdBase
from .metadata.scrutable import ScruTable
from .naming import (
    BASE_CONF_PATH,
    MAIN_MODULE_NAME,
    PROFILES_PATH,
    cli_run,
    get_data_path,
    get_stage_name,
)
from .persistent_state import PersistentState
from .reporting import ReportFile

logger = get_logger()


@dataclass
class PipelineElement:

    runner: callable
    dependencies: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    outputs_nocache: list = field(default_factory=list)
    outputs_persist: list = field(default_factory=list)
    write_envs: list = field(default_factory=list)
    read_env: Optional[str] = None
    is_env_creator: bool = False
    is_data_loader: bool = False

    def __post_init__(self):
        _conf = Config.load()
        cbase = CompleteIdBase.from_cls(self.runner, _conf.name)
        if not self.write_envs:
            self.write_envs = _conf.env_names
        if self.is_data_loader:
            self.outputs_persist.append(cbase)
            self.write_envs = [_conf.default_env]
        if self.is_env_creator:
            self.outputs.append(cbase)
            self.dependencies.append(cbase)
            self.read_env = _conf.default_env
            created_envs = list(filter(_conf.default_env.__ne__, _conf.env_names))
            self.write_envs = created_envs

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.runner(*args, **kwds)

    def run(self, env):
        conf = RunConfig.load()
        conf.read_env = self.read_env or env
        conf.write_env = env
        conf.dump()
        _, kwargs = self._get_params(env)
        with _profile(conf.profile, self.stage_name(env)):
            return self.runner(**kwargs)

    def add_stages(self, dvc_repo):
        from . import typer_commands as tc

        # relpath = inspect.getfile(self.runner)
        # lineno = inspect.findsource(self.runner)[1]

        for write_env in self.write_envs:
            _true_read_env = self.read_env or write_env
            _parser = partial(_parse_list, env=write_env)
            param_ids, _ = self._get_params(write_env)

            dvc_repo.stage.add(
                cmd=cli_run((tc.run_step, self.ns, write_env)),
                name=self.stage_name(write_env),
                outs_no_cache=_parser(self.outputs_nocache),
                outs=_parser(self.outputs),
                outs_persist=_parser(self.outputs_persist),
                deps=_parse_list([self.runner, *self.dependencies], _true_read_env),
                params=[{BASE_CONF_PATH.as_posix(): param_ids}] if param_ids else None,
                force=True,
            )
            yield self.stage_name(write_env)

    def stage_name(self, env):
        return get_stage_name(self.ns, env)

    @property
    def ns(self):
        return CompleteIdBase.from_cls(self.runner).namespace

    @property
    def aswan_dependencies(self):
        for dep in self.dependencies:
            if isinstance(dep, type) and (DzAswan in dep.mro()):
                yield dep.name

    @property
    def __module__(self):
        return self.runner.__module__

    def _get_params(self, env):
        conf = Config.load()
        kwarg_keys = inspect.getfullargspec(self.runner).args
        parsed_params = {}
        param_ids = []
        for k in kwarg_keys:
            param_id, val = self._get_param_id_val(self.ns, k, env, conf)
            parsed_params[k] = val
            param_ids.append(param_id)
        for a_name in self.aswan_dependencies:
            param_ids.append(get_aswan_leaf_param_id(a_name))
        for pstate_id in self._get_persistent_state_dependencies(conf):
            param_ids.append(pstate_id)
        return param_ids, parsed_params

    def _get_param_id_val(self, namespace, key, env, conf: Config):
        _envconf = conf.get_env(env)
        _level_params = _envconf.params.get(namespace, {})
        if key in _level_params.keys():
            _suff, val = [namespace, key], _level_params[key]
        elif key in _envconf.params.keys():
            _suff, val = [key], _envconf.params[key]
        else:
            if _envconf.parent is None:
                raise ProjectSetupException(f"no {namespace}.{key} in {env}")
            return self._get_param_id_val(namespace, key, _envconf.parent, conf)
        return ".".join([CONF_KEYS.envs, env, ENV_KEYS.params, *_suff]), val

    def _get_persistent_state_dependencies(self, conf: Config):
        for dep in self.dependencies:
            if isinstance(dep, type) and (PersistentState in dep.mro()):
                base = [CONF_KEYS.persistent_states, dep.get_full_name()]
                for k in conf.persistent_states.get(base[-1], {}).keys():
                    yield ".".join([*base, k])


def register(
    procfun=None,
    *,
    dependencies: Optional[list] = None,
    outputs: Optional[list] = None,
    outputs_nocache: Optional[list] = None,
    outputs_persist: Optional[list] = None,
):
    """registers a function to the pipeline
    the names of parameters will matter
    and will be looked up in conf/envs.yaml params"""

    return _wrap_pe(
        procfun,
        dependencies=dependencies or [],
        outputs=outputs or [],
        outputs_nocache=outputs_nocache or [],
        outputs_persist=outputs_persist or [],
    )


def register_data_loader(fun=None, *, extra_deps=None):
    """Convenience functions to use register with typical parameters"""
    return _wrap_pe(fun, dependencies=extra_deps or [], is_data_loader=True)


def register_env_creator(fun=None, *, extra_deps=None):
    """Convenience functions to use register with typical parameters"""
    return _wrap_pe(fun, dependencies=extra_deps or [], is_env_creator=True)


def _wrap_pe(fun, **kwargs):
    wrap = partial(PipelineElement, **kwargs)
    if fun is None:
        return wrap
    return wrap(fun)


def _parse_list(elemlist, env):
    return sorted(set(sum([_parse_elem(e, env) for e in elemlist or []], [])))


def _parse_elem(elem, env: str) -> str:
    if isinstance(elem, ReportFile):
        return [elem.env_posix(env)]
    if isinstance(elem, ScruTable):
        with elem.env_ctx(env):
            return [elem.trepo.vc_path.as_posix()]
    if isinstance(elem, CompleteIdBase):
        # TODO: bit hacky, means all data from a ns
        return [get_data_path(elem.project, elem.namespace, env).as_posix()]
    if inspect.ismodule(elem):
        try:
            abs_paths = elem.__path__
        except AttributeError:
            abs_paths = [elem.__file__]
        return _parse_abs_paths(abs_paths)
    if callable(elem):
        return _type_or_fun_elem(elem)

    raise TypeError(f"{type(elem)} was given as parameter for pipeline element")


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
    from pyinstrument import Profiler

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
