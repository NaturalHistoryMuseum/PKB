import pytest

from pkb.utils import clean_integer, clean_scalar, parse_literal_collection


@pytest.mark.parametrize("value", [None, "", "  ", "nan", "NaT", "<NA>"])
def test_clean_scalar_returns_none_for_empty_values(value):
    assert clean_scalar(value) is None


def test_clean_scalar_strips_text():
    assert clean_scalar("  value  ") == "value"


@pytest.mark.parametrize(("value", "expected"), [(None, None), ("", None), (" 42 ", 42)])
def test_clean_integer(value, expected):
    assert clean_integer(value) == expected


def test_parse_literal_collection():
    assert parse_literal_collection("{'US', 'GB'}") == ["GB", "US"]
