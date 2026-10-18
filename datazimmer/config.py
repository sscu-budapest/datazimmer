"""Project configuration, sourced entirely from ``pyproject.toml``.

A datazimmer project is configured under ``[tool.datazimmer]``::

    [tool.datazimmer]
    name = "electiondata"
    version = "0.2.0"

    [tool.datazimmer.remote]          # the publish target
    kind = "s3"                       # local | ssh | s3
    url = "s3://my-bucket/datasets"

    [tool.datazimmer.dependencies.elections]
    source = "s3://my-bucket/datasets/elections"   # a published project's URL or path
    version = "0.1.0"                              # exact; omit for the latest
"""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

TOOL_TABLE = "datazimmer"
STATE_DIR = ".datazimmer"
PYPROJECT = "pyproject.toml"


@dataclass(frozen=True)
class RemoteConfig:
    kind: str
    url: str


@dataclass(frozen=True)
class Dependency:
    name: str
    source: str
    version: str = ""


@dataclass
class Config:
    name: str
    version: str
    package: str = "src"
    root: Path = field(default_factory=Path.cwd)
    remote: RemoteConfig | None = None
    dependencies: list[Dependency] = field(default_factory=list)

    @classmethod
    def load(cls, root: Path | str = ".") -> "Config":
        root = Path(root)
        raw = _read_tool_table(root / PYPROJECT)
        try:
            name, version = raw["name"], str(raw["version"])
        except KeyError as e:
            raise ConfigError(f"[tool.{TOOL_TABLE}] missing required key {e}") from e

        remote = None
        if (rdic := raw.get("remote")) is not None:
            remote = RemoteConfig(kind=rdic["kind"], url=rdic["url"])

        deps = [
            Dependency(
                name=dep_name,
                source=dep["source"],
                version=dep.get("version", Dependency.version),
            )
            for dep_name, dep in raw.get("dependencies", {}).items()
        ]
        return cls(
            name=name,
            version=version,
            package=raw.get("package", cls.package),
            root=root,
            remote=remote,
            dependencies=deps,
        )

    @property
    def state_dir(self) -> Path:
        return self.root / STATE_DIR

    def get_dependency(self, name: str) -> Dependency:
        for dep in self.dependencies:
            if dep.name == name:
                return dep
        raise KeyError(f"no dependency {name!r} in [tool.{TOOL_TABLE}.dependencies]")


class ConfigError(Exception):
    pass


def _read_tool_table(pyproject: Path) -> dict:
    if not pyproject.exists():
        raise ConfigError(f"{pyproject} not found")
    data = tomllib.loads(pyproject.read_text())
    try:
        return data["tool"][TOOL_TABLE]
    except KeyError:
        raise ConfigError(f"no [tool.{TOOL_TABLE}] table in {pyproject}") from None
