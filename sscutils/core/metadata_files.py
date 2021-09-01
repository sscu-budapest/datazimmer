import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import requests

from ..constants import (
    DATA_MANAGEMENT_SRC_PATH,
    RAW_COLS_PYFILENAME,
    SRC_PATH,
    TREPOS_PYFILENAME,
)


@dataclass
class _MetadataFile(ABC):
    repo: str
    tag: str
    prefix: str

    @abstractmethod
    def adjust_content(self, content: str) -> str:
        return content

    @property
    @abstractmethod
    def filename(self) -> str:
        pass

    def get_blob(self):
        if self.repo.startswith("git@github.com:"):
            rel_repo = self.repo.replace("git@github.com:", "")
            return requests.get(
                f"https://raw.githubusercontent.com"
                f"{rel_repo}/{self.tag}/{SRC_PATH}/{self.filename}"
            ).content
        subprocess.check_call(["git", "checkout", self.tag], cwd=self.repo)
        return Path(self.repo, SRC_PATH, self.filename).read_bytes()

    def dump_to_local(self):
        meta_str = self.get_blob().decode("utf-8")
        loc_filename = "_".join(filter(None, [self.prefix, self.filename]))
        loc_file_path = Path(DATA_MANAGEMENT_SRC_PATH, loc_filename)
        loc_file_path.write_text(self.adjust_content(meta_str))


class RawColsMetadata(_MetadataFile):
    @property
    def filename(self) -> str:
        return RAW_COLS_PYFILENAME

    def adjust_content(self, content: str) -> str:
        return re.compile("(\nclass )").sub(
            r"\1" + self.prefix.title(), content
        )


class TreposMetadata(_MetadataFile):
    @property
    def filename(self) -> str:
        return TREPOS_PYFILENAME

    def adjust_content(self, content: str) -> str:
        return re.compile(r"\)\n").sub(f', prefix="{self.prefix}")\n', content)


def copy_all_metadata(repo, tag, prefix):
    for mdata_file_kls in [RawColsMetadata, TreposMetadata]:
        mdata_file_kls(repo, tag, prefix).dump_to_local()
