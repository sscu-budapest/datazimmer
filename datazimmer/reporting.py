from dataclasses import dataclass
from pathlib import Path

from .config_loading import RunConfig

REPORT_DIR = Path("reports")

report_file = REPORT_DIR / "report.html"


@dataclass
class ReportFile:
    filename: str

    def env_path(self, env):
        _path = REPORT_DIR / env / self.filename
        _path.parent.mkdir(exist_ok=True, parents=True)
        return _path

    def env_posix(self, env):
        return self.env_path(env).as_posix()

    def write_text(self, text):
        self.current_path.write_text(text)

    def write_bytes(self, blob):
        self.current_path.write_bytes(blob)

    @property
    def current_path(self) -> Path:
        return self.env_path(RunConfig.load().write_env)
