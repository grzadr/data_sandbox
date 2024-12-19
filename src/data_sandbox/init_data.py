"""
Initializes the data directory structure for the data sandbox project.

This script creates and validates the output directory specified by the user,
ensuring proper permissions and structure for data storage. It handles directory
creation with proper error checking and validation.

Example:
    $ python init_data_dir.py /path/to/output
"""

import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import (
    NamedTuple,
    List,
    Tuple,
    Generator,
    TypeAlias,
    Callable,
    Dict,
    Any,
)
from data_sandbox.args import validate_output_dir
from polars import DataFrame
from faker import Faker
from numpy.random import randint
from datetime import datetime


class Arguments(NamedTuple):
    """Container for command line arguments.

    Attributes:
        output_dir: Path to the output directory where data will be stored
    """

    output_dir: Path
    num_rows: int
    seed: int
    worker_multi: int
    time_multi: int


def parse_arguments() -> Arguments:
    """Parses command line arguments.

    Returns:
        Arguments object containing the parsed command line arguments
    """
    parser = ArgumentParser(description="Initializes data")

    parser.add_argument(
        "output_dir",
        type=validate_output_dir,
        help="Directory path where data will be stored",
    )

    parser.add_argument(
        "-n",
        "--num-rows",
        type=int,
        help="Number of records to generate",
        default=1000,
    )

    parser.add_argument(
        "-s",
        "--seed",
        type=int,
        help="Random seed for generating values",
        default=42,
    )

    parser.add_argument(
        "--worker_multiplier",
        type=int,
        help="Random seed for generating values",
        default=50,
    )

    parser.add_argument(
        "--time_multiplier",
        type=int,
        help="Random seed for generating values",
        default=1000,
    )

    args = parser.parse_args()

    return Arguments(
        output_dir=args.output_dir,
        num_rows=args.num_rows,
        seed=args.seed,
        worker_multi=args.worker_multiplier,
        time_multi=args.time_multiplier,
    )


def calc_unique_count(count: int, divisor: int) -> Tuple[int, int]:
    return (count // divisor, divisor) if divisor < count else (1, count)


def gen_num_list(count: int, divisor: int) -> List[str]:
    unique_count, divisor = calc_unique_count(count, divisor)

    return [f"{i + 1}" for i in range(unique_count) for _ in range(divisor)]


def gen_name_list(faker: Faker, count: int, divisor: int) -> List[str]:
    unique_count, divisor = calc_unique_count(count, divisor)
    assert unique_count <= count
    names = [faker.company() for _ in range(unique_count)]
    return [n for n in names for _ in range(divisor)]


def create_cost_centers(faker: Faker, num_records: int) -> DataFrame:
    return DataFrame(
        {
            "CostCenter": gen_num_list(count=num_records, divisor=10),
            "CostCenterName": gen_name_list(
                faker=faker, count=num_records, divisor=100
            ),
            "SubOrganisation": gen_name_list(
                faker=faker, count=num_records, divisor=1000
            ),
            "Organisation": gen_name_list(
                faker=faker, count=num_records, divisor=10000
            ),
            "CompanyName": gen_name_list(
                faker=faker, count=num_records, divisor=1000000
            ),
            "CompanyNumber": gen_num_list(count=num_records, divisor=10),
        },
    )


def gen_binary_list(count: int) -> List[str]:
    return [str(r) for r in randint(0, 2, count)]


def create_employees(faker: Faker, num_records: int) -> DataFrame:
    return DataFrame(
        {
            "EmployeeId": gen_num_list(num_records, 1),
            "EmployeeName": [faker.name() for _ in range(num_records)],
            "CostCenter": gen_num_list(count=num_records, divisor=10),
            "IsEmployed": gen_binary_list(count=num_records),
            "isActive": gen_binary_list(count=num_records),
        }
    )


def format_date(year: int, month: int, day: int) -> str:
    date = datetime(year, month, day)
    return date.strftime("%b%y").upper()


def gen_random_date(low: int, high: int) -> str:
    year = randint(low, high + 1)
    month = randint(1, 13)
    return format_date(year=year, month=month, day=1)


def gen_dates_list(num_rows: int, low: int, high: int) -> List[str]:
    return [gen_random_date(low=low, high=high) for _ in range(num_rows)]


def format_number(n: int, divisor: int, sep: str) -> str:
    minus = "-" if n < 0 else ""

    value = str(abs(n))
    value_l = len(value)

    if value_l <= divisor:
        return f"{minus}{value}"

    first = 0
    start = value_l - ((value_l // divisor) * divisor)

    result = []

    for last in range(start, value_l, divisor):
        result.append(value[first:last])
        first = last

    result.append(value[last:])

    return f"{minus}{sep.join(result)}"


def gen_worktime(low: int, high: int, divisor: int = 3, sep: str = ",") -> str:
    return format_number(randint(low=low, high=high), divisor=divisor, sep=sep)


def gen_worktime_list(num_rows: int, low: int, high: int) -> List[str]:
    return [gen_worktime(low=low, high=high) for _ in range(num_rows)]


def create_working_time(
    faker: Faker, num_records: int, worker_divisor: int
) -> DataFrame:
    return DataFrame(
        {
            "EmployeeId": gen_num_list(
                count=num_records, divisor=worker_divisor
            ),
            "Date": gen_dates_list(num_rows=num_records, low=2000, high=2025),
            "WorkingTime": gen_worktime_list(
                num_rows=num_records, low=1, high=99999
            ),
        }
    )


DataFrameGenerator: TypeAlias = Generator[Tuple[str, DataFrame], None, None]
DataFrameCreator: TypeAlias = Callable[..., DataFrame]
CreatorConfig: TypeAlias = Tuple[DataFrameCreator, Dict[str, Any]]


def generate_dataframes(
    faker: Faker, num_rows: int, worker_multi: int, time_multi: int
) -> DataFrameGenerator:
    configs: Dict[str, CreatorConfig] = {
        "cost_centers": (
            create_cost_centers,
            {"faker": faker, "num_records": num_rows},
        ),
        "employees": (
            create_employees,
            {"faker": faker, "num_records": num_rows * worker_multi},
        ),
        "working_time": (
            create_working_time,
            {
                "faker": faker,
                "num_records": num_rows * time_multi,
                "worker_divisor": worker_multi,
            },
        ),
    }

    for name, (creator_func, params) in configs.items():
        print(f"Creating {name}")
        df = creator_func(**params)
        yield name, df


def save_dataframes(
    output_dir: Path,
    faker: Faker,
    num_rows: int,
    worker_multi: int,
    time_multi: int,
) -> None:
    for name, df in generate_dataframes(
        faker, num_rows, worker_multi, time_multi
    ):
        output_path = output_dir / f"{name}.parquet"
        print(f"Saving {output_path}")
        df.write_parquet(output_path)


def main():
    """Main entry point of the script."""
    args = parse_arguments()
    print(args)

    faker = Faker()
    # faker.add_provider(company)
    Faker.seed(args.seed)

    save_dataframes(
        output_dir=args.output_dir,
        faker=faker,
        num_rows=args.num_rows,
        worker_multi=args.worker_multi,
        time_multi=args.time_multi
    )


if __name__ == "__main__":
    sys.exit(main())
