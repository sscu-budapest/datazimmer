import sys
from importlib import import_module
from pathlib import Path
from pkgutil import walk_packages
from typing import TYPE_CHECKING

from .exceptions import ProjectSetupException
from .metaprogramming import table_var_name
from .naming import MAIN_MODULE_NAME, META_MODULE_NAME, RegistryPaths
from .utils import reset_src_module

if TYPE_CHECKING:  # pragma: no cover
    from .metadata.datascript.scrutable import ScruTable


class ModuleTree:
    def __init__(self) -> None:
        reset_src_module()
        self.all_modules = []
        self.local_namespaces = set()
        sys.path.insert(0, Path.cwd().as_posix())
        try:
            src = import_module(MAIN_MODULE_NAME)
        except ModuleNotFoundError:
            raise ProjectSetupException(f"{MAIN_MODULE_NAME} module missing")
        src_dir = Path(src.__file__).parent.as_posix()
        for _info in walk_packages([src_dir], f"{MAIN_MODULE_NAME}."):
            _m = import_module(_info.name)
            self.all_modules.append(_m)
            self.local_namespaces.add(_info.name.split(".")[1])
        sys.path.pop(0)


class InstalledPaths:
    def __init__(self, to_load: str, loading_from: str) -> None:
        mod = import_module(f"{META_MODULE_NAME}.{to_load}")
        meta_path = Path(mod.__file__).parent
        self.ns_paths = [p for p in meta_path.iterdir() if not p.name.startswith("__")]
        rpaths = RegistryPaths(loading_from, "")
        self.info_yaml = rpaths.info_yaml_of(to_load, mod.__version__)


def load_scrutable(project, ns_name, table_name) -> "ScruTable":
    mod = import_module(f"{META_MODULE_NAME}.{project}.{ns_name}")
    return getattr(mod, table_var_name(table_name))
