import re

from .config_loading import DatasetConfig, ProjectConfig
from .exceptions import DatasetSetupException
from .helpers import import_env_creator_function, import_update_data_function
from .metadata import ArtifactMetadata
from .metadata.bedrock.atoms import NS_ATOM_TYPE
from .metadata.datascript.to_bedrock import DatascriptToBedrockConverter
from .naming import ns_metadata_abs_module
from .sql.draw import dump_graph
from .sql.loader import SqlLoader


def sql_validation(constr, env=None):
    loader = SqlLoader(constr, echo=False)
    loader.setup_schema()
    dump_graph(loader.sql_meta, loader.engine)
    try:
        loader.load_data(env)
        loader.validate_data(env)
    finally:
        loader.purge()


def validate_project_env():
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


def validate_dataset_setup(env=None):
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
    DatasetConfig()
    import_env_creator_function()
    import_update_data_function()

    root_serialized_ns = ArtifactMetadata.load_serialized().root_ns
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
    if ds_atom_n != len(root_serialized_ns.atoms):
        raise DatasetSetupException("")
    sql_validation("sqlite:///:memory:", env)
    validate_ds_importable(env)


def validate_ds_importable(env):
    pass


def validate_step_name(s):
    _check_match("_", s)


def validate_repo_name(s):
    _check_match("-", s)


def _check_match(bc, s):
    rex = r"[a-z]+((?!{bc}{bc})[a-z|\{bc}])*[a-z]+".format(bc=bc)
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
