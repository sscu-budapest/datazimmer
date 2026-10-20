"""``dz`` command line: typed handlers dispatched by protocli.

``dz --help-all`` prints every command's usage; ``dz --complete`` serves shell
completion.
"""

import json
import shutil
import sys
from pathlib import Path
from typing import Annotated

from protocli import FILES, Complete, Dispatcher

from .config import Config, ConfigError
from .pipeline import Pipeline
from .sql import DEFAULT_DB, DEFAULT_ERD


def _dependency_sources() -> list[str]:
    try:
        return [dep.source for dep in Config.load().dependencies]
    except ConfigError:
        return []


Source = Annotated[str, Complete(_dependency_sources)]
OutPath = Annotated[str, FILES]


def _cmd_run(*, force: bool = False) -> None:
    """Execute the pipeline, skipping unchanged steps.

    Args:
        force: re-run every step
    """
    config = Config.load()
    ran = _load_pipeline(config).run(force=force, state_dir=config.state_dir)
    print(f"ran: {ran}" if ran else "nothing to run (all steps up to date)")


def _cmd_collect(*names: str, force: bool = False, status: bool = False) -> int | None:
    """Run the due collectors of the modules listed under ``collectors``.

    Args:
        names: depots to collect now, whatever their cadence
        force: run every collector with a cadence, due or not
        status: print each collector's cadence, last attempt and due state; run none
    """
    from .aswan_integration import collect, collector_states

    config = Config.load()
    if status:
        for state in collector_states(config):
            print(state)
        return None
    ran, failed = collect(config, names, force=force)
    print(f"ran: {ran}" if ran else "nothing due")
    if failed:
        print(f"failed: {failed}", file=sys.stderr)
        return 1
    return None


def _cmd_publish() -> None:
    """Upload tables + manifest to the remote."""
    from .remote import publish

    config = Config.load()
    manifest = publish(config, _load_pipeline(config).output_tables)
    print(f"published {config.name} v{config.version}: {list(manifest.tables)}")


def _cmd_pull(
    source: Source,
    *,
    version: str | None = None,
    dest: Annotated[str | None, FILES] = None,
) -> None:
    """Download a published project.

    Args:
        source: project URL or path
        version: published version, the latest when omitted
        dest: target directory, the pull cache when omitted
    """
    from .remote import pull

    print(f"pulled to {pull(source, version, dest)}")


def _cmd_validate() -> None:
    """Check index uniqueness + foreign-key integrity."""
    from .validate import validate

    validate(_load_pipeline(Config.load()).tables)
    print("ok")


def _cmd_sql(*, constr: str = DEFAULT_DB) -> None:
    """Load all tables into a relational database.

    Args:
        constr: SQLAlchemy connection string
    """
    from .sql import to_sql

    to_sql(_load_pipeline(Config.load()).tables, constr)
    print(f"loaded into {constr}")


def _cmd_draw(*, out: OutPath = DEFAULT_ERD) -> None:
    """Write a Mermaid ER diagram.

    Args:
        out: markdown file to write
    """
    from .sql import draw

    print(f"wrote {draw(_load_pipeline(Config.load()).tables, out)}")


def _cmd_schema_export(*, out: OutPath = "schemas") -> None:
    """Write a JSON Schema per entity.

    Args:
        out: directory to write into
    """
    from .schema import to_json_schema

    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for table in _load_pipeline(Config.load()).tables:
        schema = to_json_schema(table.entity)
        (out_dir / f"{table.name}.json").write_text(json.dumps(schema, indent=2))
    print(f"wrote schemas to {out_dir}")


def _cmd_schema_codegen(
    source: Source, *, version: str | None = None, out: OutPath = "-"
) -> None:
    """Generate Entity classes from a published manifest.

    Args:
        source: project URL or path
        version: published version, the latest when omitted
        out: file to write, stdout for -
    """
    from .remote import fetch_manifest, schema_codegen

    code = schema_codegen(fetch_manifest(source, version))
    if out == "-":
        print(code)
    else:
        Path(out).write_text(code)
        print(f"wrote {out}")


def _cmd_init(name: str) -> int | None:
    """Scaffold a new project.

    Args:
        name: project name, also the directory created
    """
    template = Path(__file__).parent / "_template"
    dest = Path(name)
    if dest.exists():
        print(f"{dest} already exists", file=sys.stderr)
        return 1
    shutil.copytree(template, dest)
    pyproj = dest / "pyproject.toml"
    pyproj.write_text(pyproj.read_text().replace("{{name}}", name))
    print(f"created {dest}")


def _load_pipeline(config: Config) -> Pipeline:
    module = config.import_module(config.package)
    pipeline = getattr(module, "pipeline", None)
    if not isinstance(pipeline, Pipeline):
        raise SystemExit(
            f"package {config.package!r} must expose a module-level `pipeline = Pipeline()`"
        )
    return pipeline


main = Dispatcher(
    "dz",
    {
        "run": _cmd_run,
        "collect": _cmd_collect,
        "publish": _cmd_publish,
        "pull": _cmd_pull,
        "validate": _cmd_validate,
        "sql": _cmd_sql,
        "draw": _cmd_draw,
        "schema": Dispatcher(
            "dz schema",
            {"export": _cmd_schema_export, "codegen": _cmd_schema_codegen},
            doc="JSON Schema export / Entity codegen",
        ),
        "init": _cmd_init,
    },
).run
