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
from typing import NamedTuple, List, Tuple
from data_sandbox.args import validate_output_dir
from polars import DataFrame
from faker import Faker


class Arguments(NamedTuple):
    """Container for command line arguments.

    Attributes:
        output_dir: Path to the output directory where data will be stored
    """

    output_dir: Path
    num_rows: int
    seed: int


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

    args = parser.parse_args()

    return Arguments(
        output_dir=args.output_dir, num_rows=args.num_rows, seed=args.seed
    )


def calc_unique_count(count: int, divisor: int) -> Tuple[int, int]:
    return (count // divisor, divisor) if divisor <= count else (1, 1)


def gen_num_list(count: int, divisor: int) -> List[str]:
    unique_count = calc_unique_count(count, divisor)

    return [f"{i + 1}" for i in range(unique_count) for _ in range(divisor)]


def gen_name_list(faker: Faker, count: int, divisor: int) -> List[str]:

    unique_count = calc_unique_count(count, divisor)
    assert(unique_count <= count)
    return [
        faker.company() for _ in range(unique_count) for _ in range(divisor)
    ]


def generate_cost_centers(faker: Faker, num_records: int) -> DataFrame:
    return DataFrame(
        {
            "CostCenter": gen_num_list(count=num_records, divisor=10),
            "CostCenterName": gen_name_list(
                faker=faker, count=num_records, divisor=10
            ),
            "SubOrganisation": gen_name_list(
                faker=faker, count=num_records, divisor=100
            ),
            "Organisation": gen_name_list(
                faker=faker, count=num_records, divisor=1000
            ),
            "CompanyName": gen_name_list(
                faker=faker, count=num_records, divisor=10000
            ),
            "CompanyNumber": gen_num_list(count=num_records, divisor=10),
        },
    )


def main():
    """Main entry point of the script."""
    args = parse_arguments()
    print(args)


    assert(calc_unique_count(100, 10) == (10, 10))
    assert(calc_unique_count(100, 100) == (1))
    assert(calc_unique_count(100, 1000) == 1)

    faker = Faker()
    # faker.add_provider(company)
    Faker.seed(args.seed)

    cost_centers = generate_cost_centers(
        faker=faker, num_records=args.num_rows
    )

    print(cost_centers)


if __name__ == "__main__":
    sys.exit(main())
