from pathlib import Path
from typing import Optional

from ..constants import DATA_PATH


def get_subset_path(subset_name: str, prefix: Optional[str] = None) -> Path:
    return DATA_PATH / (prefix or "") / subset_name
