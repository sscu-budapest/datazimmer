from importlib import import_module
from pathlib import Path
from pkgutil import walk_packages

from .exceptions import ArtifactSetupException
from .naming import MAIN_MODULE_NAME, META_MODULE_NAME, RegistryPaths
from .utils import reset_src_module


class ModuleTree:
    def __init__(self) -> None:
        reset_src_module()
        self.all_modules = []
        self.local_namespaces = set()
        try:
            src = import_module(MAIN_MODULE_NAME)
        except ModuleNotFoundError:
            raise ArtifactSetupException(f"{MAIN_MODULE_NAME} module missing")
        src_dir = Path(src.__file__).parent.as_posix()
        for _info in walk_packages([src_dir], f"{MAIN_MODULE_NAME}."):
            _m = import_module(_info.name)
            self.all_modules.append(_m)
            self.local_namespaces.add(_info.name.split(".")[1])


class InstalledPaths:
    def __init__(self, to_load: str, loading_from: str) -> None:
        mod = import_module(f"{META_MODULE_NAME}.{to_load}")
        meta_path = Path(mod.__file__).parent
        self.ns_paths = [p for p in meta_path.iterdir() if not p.name.startswith("__")]
        rpaths = RegistryPaths(loading_from, "")
        self.info_yaml = rpaths.info_yaml_of(to_load, mod.__version__)
