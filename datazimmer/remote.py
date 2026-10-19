"""Versioned data publishing and URL-based consumption.

Pipeline outputs are immutable parquet artifacts, so there is no
content-addressed store — just versioned files on a remote, described by a
manifest (per table: schema, row count, checksum and data files) and read back
by URL.

Remote layout, relative to the configured base::

    {project}/latest.json                     -> {"version": "0.2.0"}
    {project}/{version}/manifest.json
    {project}/{version}/{table}/**/*.parquet

Producer::

    publish(config, [elections, precincts])

Consumer (lazy, no download needed for local/s3/http)::

    lf = load("s3://bucket/datasets/electiondata", "elections")
    # or, resolving a [tool.datazimmer.dependencies] entry:
    lf = load_dependency(config, "elections", "elections")
"""

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse
from urllib.request import urlopen

import polars as pl

from ._hashing import digest_path, iter_files
from .config import Config, ConfigError, RemoteConfig
from .schema import PREFIX_SEP, entity_metadata
from .table import Table, tables_by_name

MANIFEST = "manifest.json"
LATEST = "latest.json"
CACHE_ENV_VAR = "DATAZIMMER_CACHE"
TIMEOUT = 60  # seconds a network connection or read may stall before failing
_SSH = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={TIMEOUT}"]


def cache_root() -> Path:
    """Where ``pull`` localizes datasets; ``CACHE_ENV_VAR`` overrides the default.
    Resolved per call so an env var set after import still takes effect."""
    env = os.environ.get(CACHE_ENV_VAR)
    return Path(env) if env else Path.home() / ".cache" / "datazimmer"


@dataclass
class TableManifest:
    name: str
    rows: int
    size_bytes: int
    checksum: str
    schema: dict
    files: list[str]  # data files, relative to the table dir


@dataclass
class Manifest:
    project: str
    version: str
    tables: dict[str, TableManifest] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, text: str | bytes) -> "Manifest":
        d = json.loads(text)
        tables = {k: TableManifest(**v) for k, v in d["tables"].items()}
        return cls(project=d["project"], version=d["version"], tables=tables)


# --- backends -------------------------------------------------------------


class Backend:
    """A storage target addressed by string keys relative to a base."""

    def put_file(self, local: Path, key: str) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support writes")

    def put_dir(self, local: Path, key: str) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support writes")

    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError(f"{type(self).__name__} does not support reads")

    def uri(self, key: str) -> str | None:
        """polars-scannable URI for a key, or None if it must be downloaded."""
        return None

    def get_files(self, key: str, files: list[str], dest: Path) -> None:
        """Download ``files`` (relative to ``key``) into ``dest``."""
        for f in files:
            out = dest / f
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(self.get_bytes(f"{key}/{f}"))


class LocalBackend(Backend):
    def __init__(self, base: str) -> None:
        self.base = Path(base).expanduser()

    def put_file(self, local: Path, key: str) -> None:
        dest = self.base / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local, dest)

    def put_dir(self, local: Path, key: str) -> None:
        dest = self.base / key
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(local, dest)

    def get_bytes(self, key: str) -> bytes:
        return (self.base / key).read_bytes()

    def uri(self, key: str) -> str:
        return str(self.base / key)


class SshBackend(Backend):
    """rsync target, e.g. ``user@host:/srv/datasets``."""

    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")

    def put_file(self, local: Path, key: str) -> None:
        host, _, path = f"{self.base}/{key}".partition(":")
        parent = str(PurePosixPath(path).parent)
        subprocess.run(
            [*_SSH, host, "mkdir", "-p", parent], check=True, timeout=TIMEOUT
        )
        _rsync(str(local), f"{self.base}/{key}")

    def put_dir(self, local: Path, key: str) -> None:
        _rsync("--delete", f"{local}/", f"{self.base}/{key}/")

    def get_bytes(self, key: str) -> bytes:
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "f"
            _rsync(f"{self.base}/{key}", str(dest))
            return dest.read_bytes()

    def get_files(self, key: str, files: list[str], dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        _rsync(f"{self.base}/{key}/", f"{dest}/")


class S3Backend(Backend):
    def __init__(self, base: str) -> None:
        parsed = urlparse(base)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3")
        return self._client

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_file(self, local: Path, key: str) -> None:
        self.client.upload_file(str(local), self.bucket, self._key(key))

    def put_dir(self, local: Path, key: str) -> None:
        for f in iter_files(local):
            rel = f.relative_to(local).as_posix()
            self.client.upload_file(str(f), self.bucket, self._key(f"{key}/{rel}"))

    def get_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=self._key(key))[
            "Body"
        ].read()

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._key(key)}"


class HttpBackend(Backend):
    """Read-only HTTP(S) backend for consumers; lazy scans need Range support."""

    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")

    def get_bytes(self, key: str) -> bytes:
        with urlopen(self.uri(key), timeout=TIMEOUT) as r:
            return r.read()

    def uri(self, key: str) -> str:
        return f"{self.base}/{key}"


def backend_from_config(remote: RemoteConfig) -> Backend:
    if remote.kind not in _BACKENDS:
        raise ConfigError(f"remote kind {remote.kind!r} not one of {sorted(_BACKENDS)}")
    return _BACKENDS[remote.kind](remote.url)


def backend_from_source(source: str | Path) -> Backend:
    source = str(source)  # accept a Path for local sources
    if source.startswith("s3://"):
        return S3Backend(source)
    if source.startswith(("http://", "https://")):
        return HttpBackend(source)
    if _is_ssh(source):
        return SshBackend(source)
    return LocalBackend(source)


_BACKENDS = {
    "local": LocalBackend,
    "ssh": SshBackend,
    "s3": S3Backend,
    "http": HttpBackend,
}


# --- publish / pull / load ------------------------------------------------


def build_manifest(config: Config, tables: list[Table]) -> Manifest:
    manifest = Manifest(project=config.name, version=config.version)
    for name, table in tables_by_name(tables).items():
        files = [f for f in iter_files(table.path) if f.suffix == ".parquet"]
        manifest.tables[name] = TableManifest(
            name=name,
            rows=table.n_rows(),
            size_bytes=sum(f.stat().st_size for f in files),
            checksum=f"blake2b:{digest_path(table.path)}",
            schema=entity_metadata(table.entity),
            files=[f.relative_to(table.path).as_posix() for f in files],
        )
    return manifest


def publish(
    config: Config, tables: list[Table], backend: Backend | None = None
) -> Manifest:
    if backend is None:
        if config.remote is None:
            raise ValueError(
                "no [tool.datazimmer.remote] configured and no backend passed"
            )
        backend = backend_from_config(config.remote)
    manifest = build_manifest(config, tables)
    prefix = f"{config.name}/{config.version}"
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / MANIFEST).write_text(manifest.to_json())
        backend.put_file(tmp / MANIFEST, f"{prefix}/{MANIFEST}")
        for table in tables:
            backend.put_dir(Path(table.path), f"{prefix}/{table.name}")
        (tmp / LATEST).write_text(json.dumps({"version": config.version}))
        backend.put_file(tmp / LATEST, f"{config.name}/{LATEST}")
    return manifest


def fetch_manifest(source: str | Path, version: str | None = None) -> Manifest:
    return _open(source, version)[1]


def pull(source: str, version: str | None = None, dest: Path | None = None) -> Path:
    """Download a published project version into a local cache; return its path."""
    backend, manifest = _open(source, version)
    dest = Path(dest) if dest else cache_root() / manifest.project / manifest.version
    dest.mkdir(parents=True, exist_ok=True)
    (dest / MANIFEST).write_text(manifest.to_json())
    for name, tm in manifest.tables.items():
        backend.get_files(f"{manifest.version}/{name}", tm.files, dest / name)
    return dest


def load(source: str, table: str, version: str | None = None) -> pl.LazyFrame:
    """Lazily read a published table by URL; only unscannable (ssh) sources are
    downloaded first."""
    backend, manifest = _open(source, version)
    files = manifest.tables[table].files
    uris = [backend.uri(f"{manifest.version}/{table}/{f}") for f in files]
    if None in uris:
        local = pull(source, manifest.version) / table
        uris = [str(local / f) for f in files]
    return pl.scan_parquet(uris)


def load_dependency(config: Config, dep_name: str, table: str) -> pl.LazyFrame:
    dep = config.get_dependency(dep_name)
    return load(dep.source, table, version=dep.version or None)


# --- opt-in codegen -------------------------------------------------------


def schema_codegen(manifest: Manifest) -> str:
    """Generate Entity class source from a manifest (opt-in type-safe consumption).

    An FK target the manifest does not publish gets an index-only stub class."""
    entities = {tm.schema["name"]: tm.schema for tm in manifest.tables.values()}
    for schema in list(entities.values()):
        for fk in schema["foreign_keys"]:
            entities.setdefault(fk["target_entity"], _stub_schema(schema, fk))
    lines = [
        "# Generated by `dz schema codegen` — do not edit.",
        "from datetime import datetime",
        "from typing import Annotated, Literal",
        "",
        "from datazimmer import Entity, Index",
        "",
    ]
    for name in _entity_order(entities):
        lines += _entity_source(entities[name])
        lines.append("")
    return "\n".join(lines)


def _stub_schema(schema: dict, fk: dict) -> dict:
    local = {c["name"]: c for c in schema["columns"]}
    columns = [
        {**local[col], "name": target, "nullable": False, "is_index": True}
        for col, target in zip(fk["columns"], fk["target_columns"])
    ]
    return {"name": fk["target_entity"], "columns": columns, "foreign_keys": []}


def _entity_source(schema: dict) -> list[str]:
    columns = {c["name"]: c for c in schema["columns"]}
    fk_columns = {c for fk in schema["foreign_keys"] for c in fk["columns"]}
    body = [
        f"    {c['name']}: {_annotation(_type_expr(c), c['nullable'], c['is_index'])}"
        for c in schema["columns"]
        if c["name"] not in fk_columns
    ]
    for fk in schema["foreign_keys"]:
        is_index = columns[fk["columns"][0]]["is_index"]
        annotation = _annotation(fk["target_entity"], fk["nullable"], is_index)
        body.append(f"    {_fk_field_name(fk)}: {annotation}")
    return [f"class {schema['name']}(Entity):", *(body or ["    pass"])]


def _type_expr(column: dict) -> str:
    categories = column.get("categories")
    if categories:
        return f"Literal[{', '.join(map(repr, categories))}]"
    return column["dtype"]


def _annotation(type_name: str, nullable: bool, is_index: bool) -> str:
    if is_index:
        return f"Annotated[{type_name}, Index]"
    return f"{type_name} | None" if nullable else type_name


def _fk_field_name(fk: dict) -> str:
    return fk["columns"][0].removesuffix(f"{PREFIX_SEP}{fk['target_columns'][0]}")


def _entity_order(entities: dict[str, dict]) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()

    def visit(name: str):
        if name in seen or name not in entities:
            return
        seen.add(name)
        for fk in entities[name]["foreign_keys"]:
            visit(fk["target_entity"])
        order.append(name)

    for name in entities:
        visit(name)
    return order


# --- helpers --------------------------------------------------------------


def _open(source: str | Path, version: str | None) -> tuple[Backend, Manifest]:
    backend = backend_from_source(source)
    version = version or json.loads(backend.get_bytes(LATEST))["version"]
    return backend, Manifest.from_json(backend.get_bytes(f"{version}/{MANIFEST}"))


def _is_ssh(source: str) -> bool:
    head = source.split("/", 1)[0]
    return "@" in head and ":" in head


def _rsync(*args: str) -> None:
    rsync = ["rsync", "-az", f"--timeout={TIMEOUT}", "-e", " ".join(_SSH)]
    subprocess.run([*rsync, *args], check=True)
