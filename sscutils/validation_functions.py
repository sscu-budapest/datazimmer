import re
from functools import partial
from pathlib import Path

from dvc.repo import Repo
from structlog import get_logger

from .artifact_context import ArtifactContext
from .config_loading import DataEnvSpecification, DatasetConfig, ProjectConfig
from .exceptions import DatasetSetupException
from .helpers import import_env_creator_function, import_update_data_function
from .metadata import ArtifactMetadata
from .metadata.bedrock.atoms import NS_ATOM_TYPE
from .metadata.bedrock.imported_namespace import ImportedNamespace
from .metadata.datascript.conversion import imported_bedrock_to_datascript
from .metadata.datascript.to_bedrock import DatascriptToBedrockConverter
from .naming import (
    COMPLETE_ENV_NAME,
    ns_metadata_abs_module,
    project_template_repo,
)
from .sql.draw import dump_graph
from .sql.loader import SqlLoader
from .utils import cd_into

logger = get_logger()


def log(msg, artifact_type):
    logger.info(f"validating {artifact_type} - {msg}")


def sql_validation(constr, env=None, draw=False, batch_size=2000):
    loader = SqlLoader(constr, echo=False, batch_size=batch_size)
    loader.setup_schema()
    if draw:
        dump_graph(loader.sql_meta, loader.engine)
    try:
        loader.load_data(env)
        loader.validate_data(env)
    finally:
        loader.purge()


def validate_project():
    """asserts a few things about a dataset

    - all prefixes in envs have imported namespaces
    - configuration files are present
    - metadata is same across all branches
    - metadata fits what is in the data files
    - one step per module

    Raises
    ------
    ProjectSetupException
        explains what is wrong
    """
    _ = ProjectConfig()


def validate_dataset(
    constr="sqlite:///:memory:", env=None, draw=False, batch_size=2000
):
    """asserts a few things about a dataset

    - configuration files are present
    - standard functions can be imported
    - metadata is properly exported from datascript
    - metadata fits what is in the data files
    - is properly uploaded -> can be imported to a project

    Raises
    ------
    DatasetSetupException
        explains what is wrong
    """

    _log = partial(log, artifact_type="dataset")

    _log("full context")
    ctx = ArtifactContext()

    _log("config files and naming")
    conf = DatasetConfig()
    ctx.branch_remote_pairs
    for _env in [*conf.created_environments, conf.default_env]:
        is_underscored_name(_env.name)
        is_dashed_name(_env.branch)

    _log("function imports")
    import_env_creator_function()
    import_update_data_function()

    _log("serialized metadata fits conventions")
    root_serialized_ns = ArtifactMetadata.load_serialized().root_ns
    for table in root_serialized_ns.tables:
        is_underscored_name(table.name)
        for feat in table.features_w_ind:
            is_underscored_name(feat.prime_id)

    _log("serialized metadata matching datascript")
    root_datascript_ns = DatascriptToBedrockConverter(
        ns_metadata_abs_module
    ).to_ns_metadata()
    ds_atom_n = 0
    for ds_atom in root_datascript_ns.atoms:
        try:
            ser_atom = root_serialized_ns.get(ds_atom.name)
        except KeyError as e:
            raise DatasetSetupException(f"{ds_atom} not serialized: {e}")

        _nondesc_eq(ser_atom, ds_atom)
        ds_atom_n += 1
    assert ds_atom_n == len(root_serialized_ns.atoms)

    _log("data can be read to sql db")
    sql_validation(constr, env, draw, batch_size=batch_size)

    _log("data can be imported to a project via dvc")
    validate_ds_importable(env or COMPLETE_ENV_NAME)


def validate_ds_importable(env):
    artifact_dir = Path.cwd().as_posix()
    test_prefix = "test_dataset"

    with cd_into(project_template_repo, force_clone=True):
        ctx = ArtifactContext()
        ctx.config.data_envs.append(DataEnvSpecification(test_prefix, env))
        ctx.metadata.imported_namespaces.append(
            ImportedNamespace(test_prefix, artifact_dir)
        )
        ctx.serialize()
        ctx.import_namespaces()
        imported_bedrock_to_datascript()
        _denv = ArtifactContext().data_envs[0]
        _denv.out_path.parent.mkdir(exist_ok=True)
        _denv.load_data(Repo())
        # TODO: assert this data matches local


def is_underscored_name(s):
    _check_match("_", s)


def is_dashed_name(s):
    _check_match("-", s)


def is_repo_name(s):
    _check_match("-", s, False)


def is_step_name(s):
    _check_match("_", s, False)


def _check_match(bc, s, nums_ok=True):
    ok_chr = "a-z|0-9" if nums_ok else "a-z"
    rex = r"[a-z]+((?!{bc}{bc})[{okc}|\{bc}])*[{okc}]+".format(
        bc=bc, okc=ok_chr
    )
    if re.compile(rex).fullmatch(s) is None:
        raise NameError(
            f"{s} does not fit the expected format of "
            f"lower case letters and non-duplicated {bc}"
        )


def _nondesc_eq(serialized: NS_ATOM_TYPE, datascript: NS_ATOM_TYPE):
    if _dropdesc(serialized) != _dropdesc(datascript):
        raise DatasetSetupException(
            "inconsistent metadata: "
            f"serialized: {serialized} datascript: {datascript}"
        )


def _dropdesc(obj: NS_ATOM_TYPE):
    return {k: v for k, v in obj.to_dict().items() if k != "description"}
