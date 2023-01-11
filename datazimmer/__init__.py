"""sscu-budapest utilities for scientific data engineering"""
# flake8: noqa
from .aswan_integration import DzAswan
from .metadata.atoms import EntityClass
from .metadata.datascript import (
    AbstractEntity,
    CompositeTypeBase,
    Index,
    Nullable,
    SourceUrl,
)
from .metadata.scrutable import ScruTable
from .persistent_state import PersistentState
from .pipeline_element import register, register_data_loader, register_env_creator
from .project_runtime import dump_dfs_to_tables
from .raw_data import get_raw_data_path
from .reporting import ReportFile
from .typer_commands import app

__version__ = "0.4.8"
