from dataclasses import dataclass
from pathlib import Path

from .config_loading import Config, RunConfig
from .metadata import ArtifactMetadata

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
        self.env_path(RunConfig.load().write_env).write_text(text)


def profile_tables(ns, env):
    # slow import...
    from pandas_profiling import ProfileReport

    conf = Config.load()
    profile_dir = REPORT_DIR / "profiles" / ns
    profile_dir.mkdir(exist_ok=True, parents=True)
    lis = []
    for tab in ArtifactMetadata.load_installed().namespaces[ns].tables:
        trepo = conf.table_to_trepo(tab, ns, env)
        profile = ProfileReport(
            trepo.get_full_df(), title=f"{tab.name} Profile", minimal=True
        )
        profile_path = profile_dir / f"{tab.name}.html"
        profile.to_file(profile_path)
        lis.append(
            f'<li><a href="{profile_path.relative_to(REPORT_DIR).as_posix()}">'
            f"{tab.name}</a></li>"
        )
    report_file.write_text(
        f"""
<!DOCTYPE html>
<html>
<body>

<h1>Table profiles</h1>

<ul>
    {''.join(lis)}
</ul>

</body>
</html>

    """
    )
