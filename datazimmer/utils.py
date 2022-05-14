import os
import stat
import sys
from contextlib import contextmanager
from inspect import ismodule
from itertools import chain
from pathlib import Path
from shutil import rmtree
from subprocess import check_call, check_output
from typing import List, Type, TypeVar, Union

from colassigner.util import camel_to_snake  # noqa: F401
from sqlalchemy.dialects.postgresql import dialect as postgres_dialect

from .naming import MAIN_MODULE_NAME, META_MODULE_NAME

LINE_LEN = 119
PRIMITIVE_MODULES = ["builtins", "datetime"]
T = TypeVar("T")
package_root = Path(__file__).parent.parent


def get_cls_defined_in_module(module, parent):
    out = {}
    for poss_cls_name in dir(module):
        cls = getattr(module, poss_cls_name)
        try:
            if parent in cls.mro()[1:]:
                out[poss_cls_name] = cls
        except (AttributeError, TypeError):
            pass
    return out


def get_instances_from_module(module, cls):
    out = {}
    for obj_name in dir(module):
        obj = getattr(module, obj_name)
        if isinstance(obj, cls):
            out[obj_name] = obj
    return out


def get_modules_from_module(module):
    for obj_name in dir(module):
        obj = getattr(module, obj_name)
        if ismodule(obj):
            yield obj


def git_run(add=(), msg=None, pull=False, push=False, wd=None):
    for k, git_cmd in [
        (add, ["add", *add]),
        (msg, ["commit", "-m", msg]),
        (pull, ["pull"]),
        (push, ["push"]),
    ]:
        if k:
            check_call(["git", *git_cmd], cwd=wd)


def get_git_diffs(staged=False):
    comm = ["git", "diff", "--name-only"]
    if staged:
        comm.append("--cached")
    diffs = check_output(comm)
    return [*filter(None, diffs.decode("utf-8").strip().split("\n"))]


@contextmanager
def cd_into(dirpath: Union[str, Path]):
    wd = os.getcwd()
    os.chdir(dirpath)
    # sys.path.insert(0, str(dirpath))
    reset_src_module()
    try:
        yield
    finally:
        os.chdir(wd)
        # sys.path.pop(0)


def named_dict_to_list(
    named_dict: dict,
    cls: Type[T],
    key_name="name",
) -> List[T]:
    out = []
    for k, kwargs in named_dict.items():
        out.append(cls(**{key_name: k, **kwargs}))
    return out


def reset_src_module():
    _reset_modules(MAIN_MODULE_NAME)


def gen_rmtree(path: Path):
    if Path(path).exists():
        try:
            rmtree(path, onerror=_onerror)
        except PermissionError:  # pragma: no cover
            pass  # stupid windows


def _onerror(func, path, exc_info):  # pragma: no cover
    if not os.access(path, os.W_OK):
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise  # still stupid windows


def reset_meta_module(name=None):
    mod = META_MODULE_NAME if name is None else f"{META_MODULE_NAME}.{name}"
    _reset_modules(mod)


def chainmap(fun, iterable) -> list:
    return [*chain(*map(fun, iterable))]


def is_postgres(engine):
    return isinstance(engine.dialect, postgres_dialect)


def get_simplified_mro(cls: Type):
    return _simplify_mro(cls.mro()[1:])


def _simplify_mro(parent_list: List[Type]):
    out = []
    for cls in parent_list:
        if any(map(lambda added_cls: cls in added_cls.mro(), out)):
            continue
        out.append(cls)
    return out


def _reset_modules(root_module):
    for m_id in [*sys.modules.keys()]:
        if m_id.startswith(f"{root_module}.") or (m_id == root_module):
            sys.modules.pop(m_id)
