import pytest

from pkb.graph.writer import batched


def test_batched_splits_rows():
    rows = [{"id": i} for i in range(5)]

    result = list(batched(rows, 2))

    assert result == [
        [{"id": 0}, {"id": 1}],
        [{"id": 2}, {"id": 3}],
        [{"id": 4}],
    ]


def test_batched_empty():
    assert list(batched([], 100)) == []


def test_batched_invalid_size():
    with pytest.raises(ValueError):
        list(batched([{"id": 1}], 0))