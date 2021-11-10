from pathlib import Path
from shutil import rmtree

from yaml import safe_load

from .create_dogshow import DogshowContextCreator, dogshow_root


def setup_dogshow(mode, tmp_path: Path) -> DogshowContextCreator:
    if mode == "test":
        conf_root = tmp_path / "dogshow-root"
        conf_root.mkdir()
        return DogshowContextCreator(conf_root)
    else:  # pragma: no cover
        conf_dic = safe_load((dogshow_root / "confs-live.yaml").read_text())[
            mode
        ]
        conf_root = Path(conf_dic.pop("local_output_root"))
        rmtree(conf_root, ignore_errors=True)
        conf_root.mkdir()
        return DogshowContextCreator(local_output_root=conf_root, **conf_dic)
