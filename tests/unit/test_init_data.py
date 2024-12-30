from typing import List

import pytest

from data_sandbox import init_data
from data_sandbox.init_data import DividedCount


@pytest.mark.parametrize(
    "input_number,expected_output",
    [
        (1000, "1,000"),
        (1000000, "1,000,000"),
        (0, "0"),
        (-1000, "-1,000"),
        (999, "999"),
    ],
    ids=[
        "four_digits",
        "seven_digits",
        "zero",
        "negative",
        "three_digits",
    ],
)
def test_format_number(input_number: int, expected_output: str) -> None:
    assert init_data.format_number(input_number, 3, ",") == expected_output


@pytest.mark.parametrize(
    "n,div,expected",
    [
        (1, 1, DividedCount(1, 1, 0)),
        (10, 1, DividedCount(1, 10, 0)),
        (1, 10, DividedCount(1, 1, 0)),
        (100, 10, DividedCount(10, 10, 0)),
        (13, 3, DividedCount(3, 4, 1)),
        (7, 3, DividedCount(3, 2, 1)),
    ],
    ids=["single", "single_div", "single_count", "ten", "reminder", "seven"],
)
def test_divided_count(n: int, div: int, expected: DividedCount) -> None:
    assert DividedCount.new(n=n, div=div) == expected


@pytest.mark.parametrize(
    "n,div,expected",
    [
        (1, 1, 1),
        (10, 1, 10),
        (1, 10, 1),
        (100, 10, 10),
        (13, 3, 5),
        (7, 3, 3),
    ],
    ids=["single", "single_div", "single_count", "ten", "reminder", "seven"],
)
def test_divided_count_len(n: int, div: int, expected: int) -> None:
    assert len(DividedCount.new(n=n, div=div)) == expected


@pytest.mark.parametrize(
    "n,div,expected",
    [
        (1, 1, [0]),
        (7, 3, [0, 0, 0, 1, 1, 1, 2]),
        (10, 1, list(range(10))),
    ],
)
def test_divided_count_iter(n: int, div: int, expected: List[int]) -> None:
    result = list(DividedCount.new(n=n, div=div))
    print(result)
    assert result == expected
