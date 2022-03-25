import re
from contextlib import contextmanager
from subprocess import check_call
from typing import TYPE_CHECKING

from structlog import get_logger

from .config_loading import ArtifactEnv, Config, ImportedArtifact
from .exceptions import ArtifactSetupException
from .get_runtime import get_runtime
from .metadata.bedrock.atoms import NS_ATOM_TYPE
from .metadata.datascript.to_bedrock import DatascriptToBedrockConverter
from .naming import CONSTR, SANDBOX_DIR, SANDBOX_NAME, TEMPLATE_REPO
from .registry import Registry
from .sql.draw import dump_graph
from .sql.loader import SqlLoader
from .utils import cd_into

if TYPE_CHECKING:
    from .artifact_context import ArtifactContext


logger = get_logger(ctx="validation")


def validate_artifact(constr=CONSTR, draw=False, batch_size=2000):
    """asserts a few things about a dataset

    - configuration files are present
    - metadata is properly exported from datascript
    - metadata fits what is in the data files
    - is properly uploaded -> can be imported to a project

    Raises
    ------
    ArtifactSetupException
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

    _log("packed metadata fits conventions")
    serialized_meta = ctx.metadata
    converter = DatascriptToBedrockConverter(ctx.name)
    for ns_from_datascript in converter.get_namespaces():
        ns_meta = serialized_meta.namespaces[ns_from_datascript.name]
        for table in ns_meta.tables:
            is_underscored_name(table.name)
            for feat in table.features_w_ind:
                is_underscored_name(feat.prime_id)
        _log("packed metadata matching datascript")
        ds_atom_n = 0
        for ds_atom in ns_from_datascript.atoms:
            try:
                ser_atom = ns_meta.get(ds_atom.name)
            except KeyError as e:
                raise ArtifactSetupException(f"{ds_atom} not packed: {e}")
            _nondesc_eq(ser_atom, ds_atom)
            ds_atom_n += 1
        assert ds_atom_n == len(ns_meta.atoms)

    for env in ctx.config.validation_envs:
        _log("reading data to sql db", env=env)
        sql_validation(constr, env, draw, batch_size=batch_size)


def sql_validation(constr, env, draw=False, batch_size=2000):
    # TODO: check if postgres validates FKs, but sqlite does not
    loader = SqlLoader(constr, env, echo=False, batch_size=batch_size)
    _log = logger.new(step="sql", constr=constr, batch_size=batch_size).info
    try:
        _log("schema setup")
        loader.setup_schema()
        draw and dump_graph(loader.sql_meta, loader.engine)
        _log("loading to db")
        loader.load_data()
        _log("validating")
        loader.validate_data()
    finally:
        loader.purge()


def validate_importable(actx: "ArtifactContext"):
    aname = actx.config.name
    envs = actx.config.validation_envs
    with sandbox_artifact():
        test_conf = Config(
            name=SANDBOX_NAME,
            version="v0.0",
            imported_artifacts=[ImportedArtifact(aname)],
            envs=[ArtifactEnv(f"test_{env}", import_envs={aname: env}) for env in envs],
        )
        test_reg = Registry(test_conf)
        infp = test_reg.paths.info_yaml_of(aname, actx.config.version)
        infp.write_text(actx.registry.paths.info_yaml.read_text())
        test_conf.dump()
        type(actx)().load_all_data()
        # TODO: assert this data matches local


def is_underscored_name(s):
    _check_match("_", s)


def is_dashed_name(s):
    _check_match("-", s)


def is_repo_name(s):
    _check_match("-", s, False)


def is_step_name(s):
    _check_match("_", s, False)


@contextmanager
def sandbox_artifact():
    if not SANDBOX_DIR.exists():
        check_call(["git", "clone", TEMPLATE_REPO, SANDBOX_DIR.as_posix()])
        with cd_into(SANDBOX_DIR):
            conf = Config(SANDBOX_NAME, "v0.0")
            conf.dump()
            Registry(conf, reset=True).full_build()
    with cd_into(SANDBOX_DIR):
        yield


def _check_match(bc, s, nums_ok=True):
    ok_chr = "a-z|0-9" if nums_ok else "a-z"
    rex = r"[a-z]+((?!{bc}{bc})[{okc}|\{bc}])*[{okc}]+".format(bc=bc, okc=ok_chr)
    if re.compile(rex).fullmatch(s) is None:
        raise NameError(
            f"{s} does not fit the expected format of "
            f"lower case letters and non-duplicated {bc}"
        )


def _nondesc_eq(serialized: NS_ATOM_TYPE, datascript: NS_ATOM_TYPE):
    if _dropdesc(serialized) != _dropdesc(datascript):
        raise ArtifactSetupException(
            "inconsistent metadata: "
            f"serialized: {serialized}\ndatascript: {datascript}"
        )


def _dropdesc(obj: NS_ATOM_TYPE):
    return {k: v for k, v in obj.to_dict().items() if k != "description"}
