from sscutils.metaprogramming import (
    camel_to_snake,
    class_def_from_cls,
    get_simplified_mro,
    snake_to_camel,
)


def test_simplify_mro():
    class A:
        pass

    class B(A):
        pass

    class X:
        pass

    class Z(B, X):
        pass

    assert get_simplified_mro(Z) == [B, X]


def test_case_conertion():

    tss = "SomethingCamel"
    assert snake_to_camel(camel_to_snake(tss)) == tss

    tss2 = "something_snake"
    assert snake_to_camel(tss2) == "SomethingSnake"


def test_class_def():
    class A:
        x = int

    class B(A):
        y = float

    class K:
        pass

    class Z(B, K):
        h = str

    class_def_from_cls(
        Z
    ) == """class Z(B, K):
    h = str"""
