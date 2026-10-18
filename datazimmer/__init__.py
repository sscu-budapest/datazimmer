"""Scientific data pipeline orchestrator.

Define typed entity schemas, build reproducible polars pipelines, validate
foreign-key integrity, and publish versioned datasets to a remote.
"""

from .config import Config, ConfigError, Dependency, RemoteConfig
from .pipeline import Pipeline, PipelineError, Step
from .remote import (
    Manifest,
    fetch_manifest,
    load,
    load_dependency,
    publish,
    pull,
    schema_codegen,
)
from .schema import (
    ColumnInfo,
    Entity,
    ForeignKey,
    Index,
    SchemaError,
    entity_metadata,
    get_columns,
    get_foreign_keys,
    get_index_columns,
    get_polars_schema,
    get_property_columns,
    to_json_schema,
)
from .sql import build_metadata, draw, to_sql
from .table import Table
from .validate import ValidationError, validate

__version__ = "0.5.5"

__all__ = [
    "ColumnInfo",
    "Config",
    "ConfigError",
    "Dependency",
    "Entity",
    "ForeignKey",
    "Index",
    "Manifest",
    "Pipeline",
    "PipelineError",
    "RemoteConfig",
    "SchemaError",
    "Step",
    "Table",
    "ValidationError",
    "build_metadata",
    "draw",
    "entity_metadata",
    "fetch_manifest",
    "get_columns",
    "get_foreign_keys",
    "get_index_columns",
    "get_polars_schema",
    "get_property_columns",
    "load",
    "load_dependency",
    "publish",
    "pull",
    "schema_codegen",
    "to_json_schema",
    "to_sql",
    "validate",
]


def __getattr__(name: str):
    # DzAswan needs aswan (the optional [collect] extra); keep it lazy so the
    # core package imports without aswan installed.
    if name == "DzAswan":
        from .aswan_integration import DzAswan

        return DzAswan
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
