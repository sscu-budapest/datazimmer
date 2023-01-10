import re
from contextlib import contextmanager
from pathlib import Path

from structlog import get_logger

from .config_loading import Config
from .get_runtime import get_runtime
from .naming import (
    CONSTR,
    DEFAULT_REGISTRY,
    MAIN_MODULE_NAME,
    SANDBOX_DIR,
    SANDBOX_NAME,
    TEMPLATE_REPO,
)
from .registry import Registry
from .sql.draw import dump_graph
from .sql.loader import SqlLoader
from .utils import cd_into, git_run

logger = get_logger(ctx="validation")


def validate(con: str = CONSTR, env: str = "", draw: bool = False, batch: int = 20000):
    """asserts a few things about a dataset

    - configuration files are present
    - metadata is properly exported from datascript
    - metadata fits what is in the data files
    - is properly uploaded -> can be imported to a project

    Raises
    ------
    ProjectSetupException
        explains what is wrong
    """

    _log = logger.info
    _log("getting runtime ctx")
    ctx = get_runtime()
    _log("config files and naming")
    env_names = set()
    for _env in ctx.config.envs:
        is_underscored_name(_env.name)
        env_names.add(_env.name)
    if _env in ctx.config.envs:
        if _env.parent:
            assert _env.parent in env_names

    venv = env or ctx.config.default_env
    _log("reading data to sql db", env=venv)
    sql_validation(con, venv, draw, batch_size=batch)


def sql_validation(constr, env, draw=False, batch_size=2000):
    # TODO: check if postgres validates FKs, but sqlite does not
    loader = SqlLoader(constr, echo=False, batch_size=batch_size)
    _log = logger.new(step="sql", constr=constr, batch_size=batch_size, env=env).info
    try:
        _log("schema setup")
        loader.setup_schema()
        draw and dump_graph(constr)
        _log("loading to db")
        loader.load_data(env)
        _log("validating")
        loader.validate_data(env)
    finally:
        loader.purge()


def is_underscored_name(s):
    _check_match("_", s)


def is_repo_name(s):
    _check_match("-", s, False)


def is_step_name(s):
    _check_match("_", s, False)


@contextmanager
def sandbox_project(registry=DEFAULT_REGISTRY):
    if not SANDBOX_DIR.exists():
        git_run(clone=(TEMPLATE_REPO, SANDBOX_DIR.as_posix()))
        with cd_into(SANDBOX_DIR):
            conf = Config(SANDBOX_NAME, "v0.0", registry=registry)
            conf.dump()
            Registry(conf, reset=True).full_build()
    with cd_into(SANDBOX_DIR):
        yield Path(MAIN_MODULE_NAME, "core.py")


def _check_match(bc, s, nums_ok=True):
    ok_chr = "a-z|0-9" if nums_ok else "a-z"
    rex = r"[a-z]+((?!{bc}{bc})[{okc}|\{bc}])*[{okc}]+".format(bc=bc, okc=ok_chr)
    if re.compile(rex).fullmatch(s) is None:
        raise NameError(
            f"{s} does not fit the expected format of "
            f"lower case letters and non-duplicated {bc}"
        )
