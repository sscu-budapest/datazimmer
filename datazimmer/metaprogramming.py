from typing import List, Type

from colassigner.util import camel_to_snake  # noqa: F401


def class_def_from_cls(cls: Type):

    return get_class_def(
        cls.__name__,
        [p.__name__ for p in get_simplified_mro(cls)],
        remove_dunder(cls.__dict__),
    )


def get_class_def(cls_name: str, parent_names: List[str] = None, att_dict: dict = None):
    parent_str = ", ".join(parent_names)

    if att_dict:
        att_strs = [f"   {k} = {v}" for k, v in att_dict.items()]
    else:
        att_strs = ["    pass"]

    return "\n".join(
        [
            f"class {cls_name}({parent_str}):",
            *att_strs,
        ]
    )


def snake_to_camel(snake_str: str):
    return "".join(snake_str.replace("_", " ").title().split())


def simplify_mro(parent_list: List[Type]):
    out = []
    for cls in parent_list:
        if any(map(lambda added_cls: cls in added_cls.mro(), out)):
            continue
        out.append(cls)
    return out


def get_simplified_mro(cls: Type):
    return simplify_mro(cls.mro()[1:])


def remove_dunder(dic: dict):
    return {k: v for k, v in dic.items() if not k.startswith("__")}
