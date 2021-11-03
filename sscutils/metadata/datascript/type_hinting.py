from typing import Generic

from ...utils import T, is_type_hint_origin

ERR_TXT = "return type hint not wrapped in Col[...]"


class Col(Generic[T]):
    pass


def get_return_col_type(fun):
    rethint = fun.__annotations__["return"]
    assert is_type_hint_origin(rethint, Col), ERR_TXT
    return rethint.__args__[0]
