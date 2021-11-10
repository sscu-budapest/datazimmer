import re

from .config_loading import DatasetConfig, ProjectConfig
from .exceptions import DatasetSetupException
from .helpers import import_env_creator_function, import_update_data_function
from .metadata import ArtifactMetadata
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


def validate_dataset_setup():
    """asserts a few things about a dataset

    - metadata is properly exported
    - configuration files are present
    - metadata is same across all branches
    - metadata fits what is in the data files

    Raises
    ------
    DatasetSetupException
        explains what is wrong
    """
    _ = DatasetConfig()
    a_meta = ArtifactMetadata.load_serialized()
    root_ns = DatascriptToBedrockConverter(
        ns_metadata_abs_module
    ).to_ns_metadata()
    for table in root_ns.tables:
        if table.name not in [t.name for t in a_meta.namespaces[""].tables]:
            raise DatasetSetupException(f"{table.name} table not serialized")
    import_env_creator_function()
    import_update_data_function()


def _check_match(bc, s):
    rex = r"[a-z]+((?!{bc}{bc})[a-z|\{bc}])*[a-z]+".format(bc=bc)
    if re.compile(rex).fullmatch(s) is None:
        raise NameError(
            f"{s} does not fit the expected format of "
            f"lower case letters and non-duplicated {bc}"
        )


def validate_step_name(s):
    _check_match("_", s)


def validate_repo_name(s):
    _check_match("-", s)
