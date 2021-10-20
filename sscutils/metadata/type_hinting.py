from typing import Generic, TypeVar

T = TypeVar("T")


class Col(Generic[T]):
    pass


def get_return_col_type(fun):
    rethint = fun.__annotations__["return"]
    assert issubclass(rethint, Col), "return type hint not wrapped in Col[...]"
    return rethint.__args__[0]
