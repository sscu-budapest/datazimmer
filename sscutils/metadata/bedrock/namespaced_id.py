from dataclasses import dataclass
from typing import Generic, Type, Union

from ...helpers import get_top_module_name
from ...naming import (
    NAMESPACE_PREFIX_SEPARATOR,
    imported_namespaces_abs_module,
)
from ...utils import PRIMITIVE_MODULES, T


@dataclass
class NamespacedId:
    """bedrock id with a namespace prefix

    ns_prefix can be "" and None as well
    "" means artifact level id
    None means local id
    """

    ns_prefix: Union[str, None]
    obj_id: str

    @property
    def datascript_obj_accessor(self):
        return self._joiner(".")

    @property
    def serialized_id(self):
        return self._joiner(NAMESPACE_PREFIX_SEPARATOR)

    @property
    def is_local(self):
        return self.ns_prefix is None

    @classmethod
    def from_serialized_id(cls, id_: str) -> "NamespacedId":
        split_id = id_.split(NAMESPACE_PREFIX_SEPARATOR)
        obj_id = split_id[-1]
        if len(split_id) > 1:
            ns_id = split_id[-2]
        else:
            ns_id = None
        return cls(ns_id, obj_id)

    @classmethod
    def from_datascript_cls(
        cls, py_cls: Type, current_top_module: str
    ) -> "NamespacedId":
        _mod = py_cls.__module__
        if _mod in PRIMITIVE_MODULES:
            ns_id = None
        elif _mod.startswith(imported_namespaces_abs_module):
            ns_id = _mod.replace(imported_namespaces_abs_module + ".", "")
        else:
            top_module = get_top_module_name(_mod)
            ns_id = None if top_module == current_top_module else top_module
        return cls(ns_id, py_cls.__name__)

    def _joiner(self, join_str):
        return join_str.join(filter(None, [self.ns_prefix, self.obj_id]))


class NamespacedIdOf(Generic[T], NamespacedId):
    pass
