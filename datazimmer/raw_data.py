from pathlib import Path
from typing import Optional

from .exceptions import ProjectSetupException
from .naming import BASE_CONF_PATH

RAW_DATA_DIR = Path("raw-data")
IMPORTED_RAW_DATA_DIR = Path("imported-raw-data")
RAW_ENV_NAME = "#raw"
MAX_LEVELS = 5


def get_raw_data_path(filename: str, project: Optional[str] = None):
    """if project is None, raw data output path is given, otherwise imported"""
    loc = Path.cwd()
    for _ in range(MAX_LEVELS):
        if is_dz_project(loc):
            if project is None:
                r_dir = loc / RAW_DATA_DIR
            else:
                r_dir = loc / IMPORTED_RAW_DATA_DIR / project
            r_dir.mkdir(exist_ok=True, parents=True)
            return r_dir / filename
        loc = loc.parent
    raise ProjectSetupException(f"could not find a project going up {MAX_LEVELS}")


def is_dz_project(dirpath: Path):
    return (dirpath / BASE_CONF_PATH).exists()
