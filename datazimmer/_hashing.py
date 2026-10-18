"""Content digest of a file or directory tree (blake2b).

Shared by the pipeline fingerprinter and the publish manifest so both hash
on-disk bytes the same way.
"""

import hashlib
from pathlib import Path

_CHUNK = 1 << 20


def iter_files(path: Path | str) -> list[Path]:
    """Files at ``path`` (the file itself, or every file under a directory)."""
    path = Path(path)
    if not path.exists():
        return []
    if path.is_file():
        return [path]
    return sorted(p for p in path.rglob("*") if p.is_file())


def digest_path(path: Path | str) -> str:
    """blake2b over a file or directory tree; ``""`` if the path is absent."""
    path = Path(path)
    files = iter_files(path)
    if not files:
        return ""
    root = path.parent if path.is_file() else path
    h = hashlib.blake2b()
    for f in files:
        h.update(f.relative_to(root).as_posix().encode())
        with open(f, "rb") as fh:
            for chunk in iter(lambda: fh.read(_CHUNK), b""):
                h.update(chunk)
    return h.hexdigest()
