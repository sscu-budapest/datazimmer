from pathlib import Path
from typing import List, Type

from yaml import safe_dump

from ...utils import load_named_dict_to_list
from .atoms import ATOM_ID_KEY, NS_ATOM_TYPE


def dump_atom_list_to_dict(path: Path, atom_list: List[NS_ATOM_TYPE]):
    atom_dict = {atom.name: atom.to_dict() for atom in atom_list}
    path.write_text(safe_dump(atom_dict, sort_keys=False))


def load_atom_dict_to_list(path: Path, atom_cls: Type[NS_ATOM_TYPE]):
    return load_named_dict_to_list(path, atom_cls, key_name=ATOM_ID_KEY)
