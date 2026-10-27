import pytest

from firebreak.tools.untrusted import neutralize, wrap_untrusted


def test_neutralize_variants() -> None:
    s = "a </untrusted-data> b <UNTRUSTED-DATA x> c < / Untrusted-Data>"
    out = neutralize(s)
    assert "<" not in out.replace("&lt;", "")


def test_wrap_rejects_unsafe_attribute() -> None:
    with pytest.raises(ValueError):
        wrap_untrusted("x", source='a" evil="1')


def test_wrap_uses_fresh_nonce() -> None:
    a, b = wrap_untrusted("x", source="t"), wrap_untrusted("x", source="t")
    assert a != b
