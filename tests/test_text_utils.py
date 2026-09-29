import pytest

from pkb.utils import clean_scalar, parse_literal_collection


@pytest.mark.parametrize("value", [None, "", "  ", "nan", "NaT", "<NA>"])
def test_clean_scalar_returns_none_for_empty_values(value):
    assert clean_scalar(value) is None


def test_clean_scalar_strips_text():
    assert clean_scalar("  value  ") == "value"


def test_parse_literal_collection():
    assert parse_literal_collection("{'US', 'GB'}") == ["GB", "US"]
