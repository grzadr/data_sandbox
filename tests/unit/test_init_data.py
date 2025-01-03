from typing import List

import pytest

from data_sandbox import init_data
from data_sandbox.init_data import (
    DividedCount,
    gen_batched_name_list,
    gen_batched_num_list,
    gen_labels_with_func,
    safe_iter,
)


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


abc = ["A", "B", "C"]


@pytest.mark.parametrize(
    "n,div,labels,expected",
    [
        (1, 1, abc, ["A"]),
        (7, 3, abc, ["A"] * 3 + ["B"] * 3 + ["C"]),
    ],
)
def test_gen_labels_with_func(
    n: int, div: int, labels: List[str], expected: List[str]
) -> None:
    count = DividedCount.new(n=n, div=div)

    labeler = iter(labels)

    def f() -> str:
        return next(labeler)

    assert list(gen_labels_with_func(count=count, f=f)) == expected


@pytest.mark.parametrize(
    "n,labels,expected",
    [
        (0, abc, []),
        (1, abc, ["A"]),
        (3, abc, abc),
        (4, abc, abc),
    ],
)
def test_safe_iter(n: int, labels: List[str], expected: List[str]) -> None:
    assert list(safe_iter(iter(labels), n)) == expected


@pytest.mark.parametrize(
    "count,groups,batch,expected",
    [
        (1, 1, 3, [[0]]),
        (7, 3, 3, [[0, 0, 0], [1, 1, 1], [2]]),
        (10, 1, 10, [list(range(10))]),
    ],
)
def test_gen_batched_num_list(
    count: int, groups: int, batch: int, expected: List[List[int]]
) -> None:
    result = list(
        gen_batched_num_list(count=count, groups=groups, batch_size=batch)
    )
    assert result == expected


@pytest.mark.parametrize(
    "count,groups,batch,labels,expected",
    [
        (1, 1, 3, abc, [["A"]]),
        (7, 3, 3, abc, [["A"] * 3, ["B"] * 3, ["C"]]),
        (7, 3, 4, abc, [["A"] * 3 + ["B"], ["B"] * 2 + ["C"]]),
    ],
)
def test_gen_batched_name_list(
    count: int,
    groups: int,
    batch: int,
    labels: List[str],
    expected: List[List[str]],
) -> None:
    it = iter(labels)

    def f() -> str:
        return next(it)

    result = list(
        gen_batched_name_list(
            count=count, groups=groups, batch_size=batch, f=f
        )
    )
    assert result == expected
