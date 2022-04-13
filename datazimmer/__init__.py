"""sscu-budapest utilities for scientific data engineering"""
# flake8: noqa
from ._version import __version__
from .metadata.datascript import (
    BaseEntity,
    CompositeTypeBase,
    IndexBase,
    Nullable,
    TableFeaturesBase,
)
from .metadata.datascript.scrutable import ScruTable
from .pipeline_registry import register, register_data_loader, register_env_creator
from .project_runtime import dump_dfs_to_tables
from .reporting import ReportFile
from .typer_commands import app

__version__ = "0.2.3"
