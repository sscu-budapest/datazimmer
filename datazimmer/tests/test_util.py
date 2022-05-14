from datazimmer.utils import get_simplified_mro


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
