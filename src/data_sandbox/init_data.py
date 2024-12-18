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
from typing import NamedTuple
from data_sandbox.args import validate_output_dir
from polars import DataFrame
from faker import Faker
from numpy.random import randint as np_randint


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


def generate_cost_centers(faker: Faker) -> DataFrame:
    return


def main():
    """Main entry point of the script."""
    args = parse_arguments()
    print(args)

    faker = Faker(args.seed)


if __name__ == "__main__":
    sys.exit(main())
