from dataclasses import dataclass
from typing import Type, Union

from ..naming import (
    IMPORTED_NAMESPACES_MODULE_NAME,
    SRC_PATH,
    get_top_module_name,
)
from ..utils import OWN_NAME, PRIMITIVE_MODULES
from .schema import PrimitiveType

NAMESPACE_PREFIX_SEPARATOR = ":"

imported_namespaces_abs_module = (
    f"{SRC_PATH}.{IMPORTED_NAMESPACES_MODULE_NAME}"
)


@dataclass
class NamespacedId:
    ns_prefix: Union[str, None]
    obj_id: str

    @property
    def py_obj_accessor(self):
        return self._joiner(".")

    @property
    def conf_obj_id(self):
        return self._joiner(NAMESPACE_PREFIX_SEPARATOR)

    @property
    def is_primitive(self):
        return self.obj_id in PrimitiveType.__members__

    @property
    def is_local(self):
        return self.ns_prefix is None

    @classmethod
    def from_conf_obj_id(cls, id_: str) -> "NamespacedId":
        split_id = id_.split(NAMESPACE_PREFIX_SEPARATOR)
        obj_id = split_id[-1]
        if len(split_id) > 1:
            ns_id = split_id[-2]
        else:
            ns_id = None
        return cls(ns_id, obj_id)

    @classmethod
    def from_py_cls(
        cls, py_cls: Type, current_top_module: str
    ) -> "NamespacedId":
        _mod = py_cls.__module__
        if (_mod in PRIMITIVE_MODULES) or _mod.startswith(OWN_NAME):
            ns_id = None
        elif _mod.startswith(imported_namespaces_abs_module):
            ns_id = _mod.replace(imported_namespaces_abs_module + ".", "")
        else:
            top_module = get_top_module_name(_mod)
            ns_id = None if top_module == current_top_module else top_module
        return cls(ns_id, py_cls.__name__)

    def _joiner(self, join_str):
        return join_str.join(filter(None, [self.ns_prefix, self.obj_id]))
