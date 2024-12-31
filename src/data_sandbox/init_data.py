"""
Initializes the data directory structure for the data sandbox project.

This script creates and validates the output directory specified by the user,
ensuring proper permissions and structure for data storage. It handles directory
creation with proper error checking and validation.

Example:
    $ python init_data_dir.py /path/to/output
"""

import logging
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    NamedTuple,
    Tuple,
    TypeAlias,
    TypeVar,
)

import polars as pl
from faker import Faker
from numpy.random import randint
from polars import DataFrame

from data_sandbox.args import validate_output_dir
from data_sandbox.logging import measure_time, setup_logging

logger = setup_logging(log_level=logging.INFO)


class Arguments(NamedTuple):
    """Container for command line arguments.

    Attributes:
        output_dir: Path to the output directory where data will be stored
    """

    output_dir: Path
    num_rows: int
    seed: int
    batch_size: int
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
        "-b", "--batch", type=int, help="Batch size", default=1000000
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
        batch_size=args.batch,
        worker_multi=args.worker_multiplier,
        time_multi=args.time_multiplier,
    )


class DividedCount(NamedTuple):
    size: int
    groups: int
    reminder: int

    @classmethod
    def new(cls, n: int, div: int) -> "DividedCount":
        if n < 1 or div < 1:
            raise ValueError(f"n: {n} and div: {div} must be positive")

        if n <= div:
            return cls(size=1, groups=n, reminder=0)

        return cls(
            size=div,
            groups=n // div,
            reminder=n % div,
        )

    def __len__(self) -> int:
        return self.groups + (1 if self.reminder else 0)

    def __iter__(self) -> Iterator[int]:
        for i in range(self.groups):
            for _ in range(self.size):
                yield i

        for _ in range(self.reminder):
            yield i + 1


T = TypeVar("T")


def gen_labels_with_func(
    count: DividedCount, f: Callable[[], T]
) -> Iterator[T]:
    counter = iter(count)
    try:
        last_idx = next(counter)
    except StopIteration:
        return
    last_name = f()
    yield last_name

    for idx in counter:
        if idx != last_idx:
            try:
                last_name = f()
            except Exception as e:
                raise RuntimeError("Function raised and error") from e
            last_idx = idx
        yield last_name


def safe_iter(it: Iterator[T], n: int) -> Iterator[T]:
    for _ in range(n):
        try:
            yield next(it)
        except StopIteration:
            return


def gen_batched_num_list(
    count: int, groups: int, batch_size: int
) -> Iterator[List[int]]:
    counter = iter(DividedCount.new(n=count, div=groups))

    for _ in range(0, count, batch_size):
        yield list(safe_iter(counter, batch_size))


def gen_batched_name_list(
    count: int, groups: int, batch_size: int, f: Callable[[], str]
) -> Iterator[List[str]]:
    div = DividedCount.new(n=count, div=groups)

    it = gen_labels_with_func(count=div, f=f)

    for _ in range(0, count, batch_size):
        yield list(safe_iter(it, batch_size))


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


def gen_batched_cost_centers(
    faker: Faker, num_records: int, batch_size: int
) -> Iterator[DataFrame]:
    f = faker.company
    frame = {
        "CostCenter": gen_batched_num_list(
            count=num_records, groups=1, batch_size=batch_size
        ),
        "CostCenterName": gen_batched_name_list(
            count=num_records,
            groups=1,
            batch_size=batch_size,
            f=f,
        ),
        "SubOrganisation": gen_batched_name_list(
            count=num_records,
            groups=1000,
            batch_size=batch_size,
            f=f,
        ),
        "Organisation": gen_batched_name_list(
            count=num_records,
            groups=10000,
            batch_size=batch_size,
            f=f,
        ),
        "CompanyName": gen_batched_name_list(
            count=num_records,
            groups=1000000,
            batch_size=batch_size,
            f=f,
        ),
        "CompanyNumber": gen_batched_num_list(
            count=num_records, groups=1000000, batch_size=batch_size
        ),
    }

    for _ in range(0, num_records, batch_size):
        data = {}
        for col, it in frame.items():
            try:
                values = next(it)
            except StopIteration as s:
                raise ValueError(
                    f"Generator for {col} exhausted prematurely"
                ) from s

            data[col] = values

        yield DataFrame(data)


@measure_time()
def create_cost_centers(faker: Faker, num_records: int) -> DataFrame:

    return DataFrame(
        {
            "CostCenter": gen_num_list(count=num_records, divisor=10),
            "CostCenterName": gen_name_list(
                faker=faker, count=num_records, divisor=100
            ),
            "SubOrganization": gen_name_list(
                faker=faker, count=num_records, divisor=1000
            ),
            "Organization": gen_name_list(
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


@measure_time()
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


@measure_time()
def create_working_time(num_records: int, worker_divisor: int) -> DataFrame:
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


@measure_time()
def create_dataframe_config(
    num_rows: int,
    batch_size: int,
    faker: Faker,
    worker_multi: int,
    time_multi: int,
) -> Dict[str, Tuple]:
    return {
        "cost_centers": (
            gen_batched_cost_centers,
            {
                "faker": faker,
                "num_records": num_rows,
                "batch_size": batch_size,
            },
        ),
        # "employees": (
        #     create_employees,
        #     {"faker": faker, "num_records": num_rows * worker_multi},
        # ),
        # "working_time": (
        #     create_working_time,
        #     {
        #         "num_records": num_rows * time_multi,
        #         "worker_divisor": worker_multi,
        #     },
        # ),
    }


@measure_time()
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
                "num_records": num_rows * time_multi,
                "worker_divisor": worker_multi,
            },
        ),
    }

    for name, (creator_func, params) in configs.items():
        print(f"Creating {name}")
        df = creator_func(**params)
        yield name, df


def recreate_parquet(data: pl.DataFrame, file_path: str | Path) -> None:
    """Creates a new parquet file, overwriting any existing file."""
    data.write_parquet(file_path, compression="zstd")


def append_to_parquet(new_data: pl.DataFrame, file_path: str | Path) -> None:
    """Appends data to existing parquet file using Polars' efficient append operation."""
    existing = pl.scan_parquet(file_path)
    pl.concat([existing, new_data.lazy()]).collect().write_parquet(file_path)


@measure_time()
def save_parquet(
    output_dir: Path,
    name: str,
    it: Iterator[DataFrame],
) -> None:
    initial = next(it)
    output_path = output_dir / f"{name}.parquet"

    recreate_parquet(data=initial, file_path=output_path)

    for data in it:
        append_to_parquet(data, output_path)


@measure_time()
def create_dataframes(
    output_dir: Path,
    faker: Faker,
    num_rows: int,
    batch_size: int,
    worker_multi: int,
    time_multi: int,
) -> None:

    config = create_dataframe_config(
        faker=faker,
        num_rows=num_rows,
        batch_size=batch_size,
        worker_multi=worker_multi,
        time_multi=time_multi,
    )

    for name, (gen, params) in config.items():
        save_parquet(
            output_dir=output_dir,
            name=name,
            it=gen(**params),
        )

    # for name, df in generate_dataframes(
    #     faker, num_rows, worker_multi, time_multi
    # ):
    #     output_path = output_dir / f"{name}.parquet"
    #     print(f"Saving {output_path}")
    #     df.write_parquet(output_path)


def main() -> None:
    """Main entry point of the script."""
    args = parse_arguments()
    print(args)

    faker = Faker()
    Faker.seed(args.seed)

    create_dataframes(
        output_dir=args.output_dir,
        faker=faker,
        num_rows=args.num_rows,
        batch_size=args.batch_size,
        worker_multi=args.worker_multi,
        time_multi=args.time_multi,
    )


if __name__ == "__main__":
    main()
