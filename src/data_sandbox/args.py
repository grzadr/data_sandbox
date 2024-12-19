from pathlib import Path
from argparse import ArgumentTypeError

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

    if output_dir.exists() and not output_dir.is_dir():
        raise ArgumentTypeError(
            f"Output path exists but is not a directory: {output_dir}"
        )

    output_dir.mkdir(mode=0o744, parents=True, exist_ok=True)
    return output_dir
