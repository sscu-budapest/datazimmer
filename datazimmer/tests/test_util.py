from datazimmer.utils import format_code, get_simplified_mro


def test_lint():
    bad_c = "x =  10"
    good_c = "x = 10\n"
    assert good_c == format_code(bad_c)
    assert good_c == format_code(good_c)


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
