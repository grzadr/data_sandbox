"""
Initializes the data directory structure for the data sandbox project.

This script creates and validates the output directory specified by the user,
ensuring proper permissions and structure for data storage. It handles directory
creation with proper error checking and validation.

Example:
    $ python init_data_dir.py /path/to/output
"""

import sys
from argparse import ArgumentParser, ArgumentTypeError
from pathlib import Path
from typing import NamedTuple


class Arguments(NamedTuple):
    """Container for command line arguments.

    Attributes:
        output_dir: Path to the output directory where data will be stored
    """

    output_dir: Path


def validate_output_dir(path_str: str) -> Path:
    """Validates and creates the output directory.

    Args:
        path_str: String representation of the directory path

    Returns:
        Path object representing the validated output directory

    Raises:
        ArgumentTypeError: If path exists but is not a directory
    """
    output_dir = Path(path_str).resolve()

    if output_dir.exists() and output_dir.is_dir():
        raise ArgumentTypeError(
            f"Output path exists but is not a directory: {output_dir}"
        )

    output_dir.mkdir(mode=644, parents=True, exist_ok=True)
    return output_dir


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

    args = parser.parse_args()

    return Arguments(output_dir=args.output_dir)


def main():
    """Main entry point of the script."""
    args = parse_arguments()
    print(args)


if __name__ == "__main__":
    sys.exit(main())
