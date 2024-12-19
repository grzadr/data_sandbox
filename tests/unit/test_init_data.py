from pathlib import Path
import polars as pl
import pytest
from data_sandbox import init_data

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
    ]
)
def test_format_number(input_number: int, expected_output: str) -> None:
    assert init_data.format_number(input_number, 3, ",") == expected_output
